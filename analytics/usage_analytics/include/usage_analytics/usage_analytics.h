#pragma once

#include "usage_analytics/sink_iface.h"

#include <boost/core/noncopyable.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/preprocessor/cat.hpp>

#include <memory>
#include <utility>

namespace analytics::usage_analytics {

struct Session {
    Session();
    explicit Session(std::string id) : id(std::move(id)) {}
    std::string id;
};

inline Session MakeQuerySession(const std::string& process_id, const std::string& query_id) {
    return Session(process_id + ":" + query_id);
}

class Tracker : private boost::noncopyable {
public:
    Tracker();
    ~Tracker() noexcept;

    static Tracker& GetInstance() {
        static Tracker instance;
        return instance;
    }

    void init(config::ConfigRef config = std::nullopt) noexcept;
    void shutdown() noexcept;

    void set_module(const std::string& module) noexcept;
    const boost::json::object& common_fields() const noexcept;

    void disable(bool disabled = true) noexcept;
    bool disabled() const noexcept;

    template <SinkComponent Sink>
    requires std::default_initializable<Sink>
    void register_sink(Sink& sink) noexcept {
        register_sink(Sink::kName, sink, []() -> std::unique_ptr<ISink> { return std::make_unique<Sink>(); });
    }

    void register_sinks_from(const Tracker& source) noexcept;
    void clear_sinks() noexcept;

    ISink* find_sink(const std::string& name) noexcept;
    const ISink* find_sink(const std::string& name) const noexcept;

    template <class Payload = boost::json::object>
    void emit(EventCode code, Payload&& payload = {}) noexcept {
        emit_payload(code, std::forward<Payload>(payload));
    }

    template <class Payload = boost::json::object>
    void emit(const Session& session, EventCode code, Payload&& payload = {}) noexcept {
        emit_payload(code, std::forward<Payload>(payload), session);
    }

private:
    template <class Payload, class... SessionArg>
    requires(sizeof...(SessionArg) <= 1 && (std::is_same_v<std::remove_cvref_t<SessionArg>, Session> && ...))
    void emit_payload(EventCode code, Payload&& payload, SessionArg&&... session) noexcept {
        try {
            if (disabled()) {
                return;
            }

            boost::json::object event_payload = make_payload(std::forward<Payload>(payload));

            if constexpr (sizeof...(SessionArg) == 0) {
                emit_payload(code, std::move(event_payload));
            } else {
                emit_payload(std::forward<SessionArg>(session)..., code, std::move(event_payload));
            }
        } catch (...) {
            // Usage analytics failures must not affect the host application.
        }
    }

    template <class Payload>
    static boost::json::object make_payload(Payload&& payload) {
        if constexpr (std::is_same_v<std::remove_cvref_t<Payload>, boost::json::object>) {
            return boost::json::object(std::forward<Payload>(payload));
        } else {
            boost::json::value value = boost::json::parse(std::forward<Payload>(payload));
            return std::move(value.as_object());
        }
    }

    void register_sink(
        const std::string& name, ISink& sink, std::unique_ptr<ISink> (*construct)()) noexcept;

    void emit_payload(EventCode code, boost::json::object&& payload);
    void emit_payload(const Session& session, EventCode code, boost::json::object&& payload);

    class Impl;
    std::unique_ptr<Impl> impl_;
};

#define TRACKER_INSTANCE() ::analytics::usage_analytics::Tracker::GetInstance()
#define TRACKER_REGISTER_COMPONENT_SINK(sink)                                           \
    [[maybe_unused]] const bool BOOST_PP_CAT(tracker_sink_registration_, __COUNTER__) = \
        (TRACKER_INSTANCE().register_sink(sink), true)

}  // namespace analytics::usage_analytics
