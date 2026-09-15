#pragma once

#include <cstdint>
#include <string>

namespace analytics::usage_analytics {

enum class EventCode : std::int8_t {
    Undefined = -1,
    Start = 0,
    Event = 1,
    End = 2,
};

constexpr inline char const* to_string(EventCode code) noexcept {
    switch (code) {
        case EventCode::Start:
            return "start";
        case EventCode::Event:
            return "event";
        case EventCode::End:
            return "end";
        [[unlikely]] case EventCode::Undefined:
            break;
    }
    return "undefined";
}

struct EventContext {
    std::string module;
    std::string process_id;
    std::string session_id;
    EventCode event_code = EventCode::Undefined;
    std::int64_t event_ts = 0;
};

}  // namespace analytics::usage_analytics
