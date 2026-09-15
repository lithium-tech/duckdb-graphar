#include "config/config_readable_iface.h"
#include "usage_analytics/usage_analytics.h"

#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/core/record_view.hpp>
#include <boost/log/keywords/file_name.hpp>
#include <boost/log/keywords/target_file_name.hpp>
#include <boost/log/sinks/text_file_backend.hpp>

#include <mutex>
#include <stdio.h>

namespace {

using analytics::usage_analytics::EventContext;
using analytics::usage_analytics::ISink;
using analytics::usage_analytics::Session;
using analytics::usage_analytics::Tracker;

class ProductUsageAnalyticsFileSink final : public ISink {
    static constexpr std::uint64_t kDefaultRotationSizeBytes = 16ULL * 1024ULL * 1024ULL;
    static constexpr std::uint64_t kDefaultRotationIntervalSeconds = 24ULL * 60ULL * 60ULL;

public:
    static constexpr const char* kName = "jsonl_file";

    bool init(const Tracker& tracker, config::ConfigRef config) noexcept override {
        try {
            const auto process_id = std::string(tracker.common_fields().at("process_id").as_string());
            const std::string section = std::string{"pua_sink_"} + kName;
            boost::filesystem::path directory =
#if defined(__linux__) || defined(__APPLE__)
                boost::filesystem::path{P_tmpdir};
#else
                boost::filesystem::temp_directory_path();
#endif
            std::uint64_t rotation_size_bytes = kDefaultRotationSizeBytes;
            std::uint64_t rotation_interval_seconds = kDefaultRotationIntervalSeconds;

            if (config) {
                const auto& cfg = config->get();

                if (const auto path = cfg.GetParameter(section, "path", {}); !path.empty()) {
                    directory = path;
                }

                rotation_size_bytes = config::ReadParameter<std::uint64_t>(cfg, section, "rotation_size_bytes",
                                                                           kDefaultRotationSizeBytes);
                rotation_interval_seconds = config::ReadParameter<std::uint64_t>(
                    cfg, section, "rotation_interval_seconds", kDefaultRotationIntervalSeconds);
            }

            directory /= "grapher-analytics";
            directory /= std::string{"pua_"} + kName;
            boost::filesystem::create_directories(directory);

            backend_ = std::make_unique<boost::log::sinks::text_file_backend>(
                boost::log::keywords::file_name = directory / (process_id + ".lock.jsonl"),
                boost::log::keywords::target_file_name = directory / (process_id + "-%5N.jsonl"));
            backend_->set_rotation_size(rotation_size_bytes);
            backend_->set_time_based_rotation(boost::log::sinks::file::rotation_at_time_interval(
                boost::posix_time::seconds(rotation_interval_seconds)));
            backend_->enable_final_rotation(false);
            backend_->auto_flush(true);
            return true;
        } catch (...) {
            backend_.reset();
            return false;
        }
    }

    void emit(const Session&, const EventContext&, const std::string& serialized_event) noexcept override {
        try {
            const std::lock_guard lock(emit_mutex_);
            if (backend_) {
                backend_->consume(boost::log::record_view{}, serialized_event);
            }
        } catch (...) {
            // Filesystem/logging failures are analytics-specific and must not affect the host.
        }
    }

    void shutdown() noexcept override {
        const std::lock_guard lock(emit_mutex_);
        try {
            if (backend_) {
                backend_->rotate_file();
            }
        } catch (...) {
            // Best-effort finalization only.
        }
        backend_.reset();
    }

private:
    std::unique_ptr<boost::log::sinks::text_file_backend> backend_;
    std::mutex emit_mutex_;
};

ProductUsageAnalyticsFileSink product_usage_analytics_file_sink;
TRACKER_REGISTER_COMPONENT_SINK(product_usage_analytics_file_sink);

}  // namespace
