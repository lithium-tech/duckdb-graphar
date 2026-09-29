#include "config/config_readable_iface.h"
#include "usage_analytics/system_info.h"
#include "usage_analytics/usage_analytics.h"

#include <boost/filesystem.hpp>
#include <boost/json/parse.hpp>

#include <catch2/catch_test_macros.hpp>
#include <fstream>
#include <vector>

namespace {

using analytics::usage_analytics::EventCode;
using analytics::usage_analytics::Session;
using analytics::usage_analytics::Tracker;

class TestConfig final : public config::IConfigReadable {
public:
    explicit TestConfig(const boost::filesystem::path& path) {
        config::section_type section;
        section.put("path", path.string());
        config_.put_child("pua_sink_jsonl_file", std::move(section));
    }

    const config::section_type& GetSection(const std::string& section_name) const override {
        if (section_name.empty()) {
            return config_;
        }
        if (const auto section = config_.get_child_optional(section_name)) {
            return section.get();
        }
        static const config::section_type empty_section;
        return empty_section;
    }

    std::string GetParameter(const std::string& section_name, const std::string& parameter_name,
                             const std::string& default_value) const override {
        const std::string path = section_name.empty() ? parameter_name : section_name + "." + parameter_name;
        return config_.get<std::string>(path, default_value);
    }

private:
    config::config_tree_type config_;
};

}  // namespace

TEST_CASE ("product usage analytics self registration and JSONL protocol") {
    const auto directory =
        boost::filesystem::temp_directory_path() / boost::filesystem::unique_path("duckdb-graphar-pua-%%%%-%%%%-%%%%");
    TestConfig config(directory);

    auto& tracker = Tracker::GetInstance();

    // Both production and test sinks are linked directly into this test module and self-register.
    REQUIRE(tracker.find_sink("jsonl_file") != nullptr);
    REQUIRE(tracker.find_sink("test") != nullptr);

    tracker.set_module("duckdb_graphar_test");
    tracker.init(std::cref(config));

    Session explicit_session;
    tracker.emit(explicit_session, EventCode::Start);
    tracker.emit(explicit_session, EventCode::Event, R"({"query":"SELECT 1"})");
    tracker.emit(explicit_session, EventCode::End);

    // The implicit API guarantees Start -> Event* -> End even if Start wasn't emitted explicitly.
    tracker.emit(EventCode::Event, R"({"kind":"implicit"})");
    tracker.emit(EventCode::End);

    static_assert(noexcept(tracker.emit(explicit_session, EventCode::Event, R"({"payload":true})")));

    tracker.shutdown();

    // Minimal lifecycle protection: a valid emit after shutdown is ignored.
    tracker.emit(explicit_session, EventCode::Event, R"({"ignored":true})");

    const auto sink_directory = directory / "grapher-analytics" / "pua_jsonl_file";
    std::vector<boost::json::object> events;
    for (const auto& entry : boost::filesystem::directory_iterator(sink_directory)) {
        if (!boost::filesystem::is_regular_file(entry.path()) || entry.path().extension() != ".jsonl" ||
            entry.path().filename().string().find(".lock.jsonl") != std::string::npos) {
            continue;
        }

        std::ifstream file(entry.path().string());
        for (std::string line; std::getline(file, line);) {
            if (!line.empty()) {
                events.emplace_back(boost::json::parse(line).as_object());
            }
        }
    }

    REQUIRE(events.size() == 6);

    const auto process_id = std::string(tracker.common_fields().at("process_id").as_string());
    for (const auto& event : events) {
        REQUIRE(std::string(event.at("module").as_string()) == "duckdb_graphar_test");
        REQUIRE(std::string(event.at("process_id").as_string()) == process_id);
    }

    REQUIRE(std::string(events[0].at("session_id").as_string()) == explicit_session.id);
    REQUIRE(std::string(events[1].at("session_id").as_string()) == explicit_session.id);
    REQUIRE(std::string(events[2].at("session_id").as_string()) == explicit_session.id);
    REQUIRE(std::string(events[0].at("event_code").as_string()) == "start");
    REQUIRE(std::string(events[1].at("event_code").as_string()) == "event");
    REQUIRE(std::string(events[2].at("event_code").as_string()) == "end");

    const auto implicit_session_id = std::string(events[3].at("session_id").as_string());
    REQUIRE(std::string(events[4].at("session_id").as_string()) == implicit_session_id);
    REQUIRE(std::string(events[5].at("session_id").as_string()) == implicit_session_id);
    REQUIRE(std::string(events[3].at("event_code").as_string()) == "start");
    REQUIRE(std::string(events[4].at("event_code").as_string()) == "event");
    REQUIRE(std::string(events[5].at("event_code").as_string()) == "end");
    REQUIRE(std::string(events[4].at("payload").as_object().at("kind").as_string()) == "implicit");

    boost::system::error_code ec;
    boost::filesystem::remove_all(directory, ec);
}

TEST_CASE ("product usage analytics system info enrichment") {
    boost::json::object payload;
    payload["kind"] = "test";

    analytics::usage_analytics::AddSystemInfo(payload);

    REQUIRE(payload.contains("user"));
    const auto& user = payload.at("user").as_object();
    REQUIRE(user.contains("username"));
    REQUIRE(user.at("username").as_string().size() > 0);
    REQUIRE(user.contains("uid"));

    REQUIRE(payload.contains("hostname"));
    REQUIRE(payload.at("hostname").as_string().size() > 0);

    REQUIRE(payload.contains("os"));
    const auto& os = payload.at("os").as_object();
    REQUIRE(os.contains("sysname"));
    REQUIRE(os.contains("release"));
    REQUIRE(os.contains("version"));
    REQUIRE(os.contains("machine"));

    REQUIRE(payload.contains("cpu"));
    const auto& cpu = payload.at("cpu").as_object();
    REQUIRE(cpu.contains("count"));
    REQUIRE(cpu.at("count").is_uint64());
    REQUIRE(cpu.at("count").as_uint64() >= 1);
    REQUIRE(cpu.contains("available_parallelism"));
    REQUIRE(cpu.at("available_parallelism").is_uint64());
    REQUIRE(cpu.at("available_parallelism").as_uint64() >= 1);

    REQUIRE(payload.contains("memory"));
    const auto& memory = payload.at("memory").as_object();
    REQUIRE(memory.contains("total"));
    REQUIRE(memory.contains("available"));

    REQUIRE(payload.at("kind").as_string() == "test");
}
