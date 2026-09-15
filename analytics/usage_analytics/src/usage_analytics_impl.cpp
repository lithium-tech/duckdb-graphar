#include "usage_analytics/usage_analytics.h"

#include <boost/dll/runtime_symbol_info.hpp>
#include <boost/json/serialize.hpp>
#include <boost/system/error_code.hpp>
#include <boost/thread.hpp>
#include <boost/unordered/concurrent_flat_map.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <chrono>

namespace {

namespace fields {

constexpr const char* kModule = "module";
constexpr const char* kProcessId = "process_id";
constexpr const char* kSessionId = "session_id";
constexpr const char* kEventCode = "event_code";
constexpr const char* kEventTs = "event_ts";
constexpr const char* kPayload = "payload";

}  // namespace fields

std::string make_uuid() {
    try {
        return boost::uuids::to_string(boost::uuids::random_generator{}());
    } catch (...) {
        // RFC 9562 Max UUID: explicit sentinel when UUID generation is unavailable.
        return "ffffffff-ffff-ffff-ffff-ffffffffffff";
    }
}

const std::string kDefaultModule = [] {
    boost::system::error_code ec;
    const auto path = boost::dll::program_location(ec);
    return ec ? std::string{"unknown"} : path.filename().string();
}();

std::int64_t event_ts() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
}

}  // namespace

namespace analytics::usage_analytics {

Session::Session() : id(make_uuid()) {}

class Tracker::Impl {
public:
    Impl() {
        common_fields_.emplace(fields::kProcessId, make_uuid());
        common_fields_.emplace(fields::kModule, kDefaultModule);
    }

    void init(const Tracker& tracker, config::ConfigRef config) noexcept {
        if (!shutdown_.load(std::memory_order_acquire)) {
            return;
        }

        sessions_.clear();

        std::erase_if(sinks_, [&](auto& sink) {
            return !sink.component.get().init(tracker, config);
        });
        shutdown_.store(false, std::memory_order_release);
    }

    void shutdown() noexcept {
        if (shutdown_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }

        sessions_.clear();

        for (auto& sink : sinks_) {
            sink.component.get().shutdown();
        }
    }

    void set_module(const std::string& module) noexcept {
        common_fields_[fields::kModule] = module.empty() ? kDefaultModule : module;
    }

    const boost::json::object& common_fields() const noexcept { return common_fields_; }

    void disable(bool disabled) noexcept { disabled_.store(disabled, std::memory_order_relaxed); }
    bool disabled() const noexcept { return disabled_.load(std::memory_order_relaxed); }

    void register_sink(const std::string& name, ISink& sink, std::unique_ptr<ISink> (*construct)()) {
        for (const auto& registered : sinks_) {
            if (registered.name == name) {
                throw std::logic_error(std::string{"Duplicate usage analytics sink: "} + name);
            }
        }

        sinks_.push_back({name, sink, construct});
    }

    void register_sinks_from(const Impl& source) noexcept {
        for (const auto& registered : source.sinks_) {
            auto sink = registered.construct();
            ISink& component = *sink;
            owned_sinks_.push_back(std::move(sink));
            register_sink(registered.name, component, registered.construct);
        }
    }

    void clear_sinks() noexcept {
        sinks_.clear();
        owned_sinks_.clear();
    }

    ISink* find_sink(const std::string& name) noexcept {
        return const_cast<ISink*>(static_cast<const Impl&>(*this).find_sink(name));
    }

    const ISink* find_sink(const std::string& name) const noexcept {
        for (const auto& sink : sinks_) {
            if (sink.name == name) {
                return &sink.component.get();
            }
        }
        return nullptr;
    }

    void emit_payload(EventCode code, boost::json::object&& payload) {
        if (shutdown_.load(std::memory_order_acquire)) {
            return;
        }

        const boost::thread::id thread_id = boost::this_thread::get_id();

        switch (code) {
            case EventCode::Start: {
                Session session;
                sessions_.insert_or_assign(thread_id, session);
                emit_one(session, EventCode::Start, std::move(payload));
                return;
            }

            case EventCode::Event: {
                std::optional<Session> session;
                if (sessions_.try_emplace_and_cvisit(
                        thread_id,
                        [&session](auto& item) noexcept { session = item.second; },
                        [&session](auto& item) noexcept { session = item.second; })) {
                    emit_one(*session, EventCode::Start, {});
                }

                emit_one(*session, EventCode::Event, std::move(payload));
                return;
            }

            case EventCode::End: {
                if (auto session = [&] {
                        std::optional<Session> session;
                        sessions_.erase_if(thread_id, [&session](auto& item) noexcept {
                            session = std::move(item.second);
                            return true;
                        });
                        return session;
                    }()) {
                    emit_one(*session, EventCode::End, std::move(payload));
                    return;
                }

                Session session;
                emit_one(session, EventCode::Start, {});
                emit_one(session, EventCode::End, std::move(payload));
                return;
            }

            [[unlikely]] case EventCode::Undefined:
                break;
        }
    }

    void emit_payload(const Session& session, EventCode code, boost::json::object&& payload) {
        if (shutdown_.load(std::memory_order_acquire)) {
            return;
        }

        switch (code) {
            case EventCode::Start:
            case EventCode::Event:
            case EventCode::End:
                emit_one(session, code, std::move(payload));
                return;

            [[unlikely]] case EventCode::Undefined:
                break;
        }
    }

private:
    struct SinkEntry {
        std::string name;
        std::reference_wrapper<ISink> component;
        std::unique_ptr<ISink> (*construct)();
    };

    using sinks_type = std::vector<SinkEntry>;
    using sessions_type = boost::concurrent_flat_map<boost::thread::id, Session>;

    void emit_one(const Session& session, EventCode code, boost::json::object&& payload) {
        EventContext ctx{
            .module = std::string(common_fields_.at(fields::kModule).as_string()),
            .process_id = std::string(common_fields_.at(fields::kProcessId).as_string()),
            .session_id = session.id,
            .event_code = code,
            .event_ts = event_ts(),
        };

        boost::json::object event_json = common_fields_;
        event_json.reserve(common_fields_.size() + 4);
        event_json.emplace(fields::kSessionId, ctx.session_id);
        event_json.emplace(fields::kEventCode, to_string(ctx.event_code));
        event_json.emplace(fields::kEventTs, ctx.event_ts);
        event_json.emplace(fields::kPayload, std::move(payload));

        const std::string serialized_json = boost::json::serialize(event_json);

        for (auto& sink : sinks_) {
            ISink& component = sink.component.get();
            if (!component.disabled()) {
                component.emit(session, ctx, serialized_json);
            }
        }
    }

    std::atomic_bool shutdown_{true};
    boost::json::object common_fields_;
    std::atomic_bool disabled_{false};
    sessions_type sessions_;
    std::vector<std::unique_ptr<ISink>> owned_sinks_;
    sinks_type sinks_;
};

Tracker::Tracker() : impl_(std::make_unique<Impl>()) {}

Tracker::~Tracker() noexcept { shutdown(); }

void Tracker::init(config::ConfigRef config) noexcept { impl_->init(*this, config); }

void Tracker::shutdown() noexcept { impl_->shutdown(); }

void Tracker::set_module(const std::string& module) noexcept { impl_->set_module(module); }

const boost::json::object& Tracker::common_fields() const noexcept { return impl_->common_fields(); }

void Tracker::disable(bool disabled) noexcept { impl_->disable(disabled); }

bool Tracker::disabled() const noexcept { return impl_->disabled(); }

void Tracker::register_sink(const std::string& name, ISink& sink, std::unique_ptr<ISink> (*construct)()) noexcept {
    impl_->register_sink(name, sink, construct);
}

void Tracker::register_sinks_from(const Tracker& source) noexcept { impl_->register_sinks_from(*source.impl_); }

void Tracker::clear_sinks() noexcept { impl_->clear_sinks(); }

ISink* Tracker::find_sink(const std::string& name) noexcept { return impl_->find_sink(name); }

const ISink* Tracker::find_sink(const std::string& name) const noexcept { return impl_->find_sink(name); }

void Tracker::emit_payload(EventCode code, boost::json::object&& payload) {
    impl_->emit_payload(code, std::move(payload));
}

void Tracker::emit_payload(const Session& session, EventCode code, boost::json::object&& payload) {
    impl_->emit_payload(session, code, std::move(payload));
}

}  // namespace analytics::usage_analytics
