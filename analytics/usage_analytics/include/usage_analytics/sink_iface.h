#pragma once

#include "usage_analytics/event_iface.h"

#include <any>
#include <atomic>
#include <concepts>
#include <functional>
#include <optional>
#include <type_traits>

namespace config {
class IConfigReadable;

using ConfigRef = std::optional<std::reference_wrapper<const IConfigReadable>>;
}  // namespace config

namespace analytics::usage_analytics {

class Tracker;
struct Session;

class ISink {
public:
    virtual ~ISink() = default;

    void disable(bool disabled = true) noexcept { disabled_.store(disabled, std::memory_order_relaxed); }
    bool disabled() const noexcept { return disabled_.load(std::memory_order_relaxed); }

    virtual bool init(const Tracker& tracker, config::ConfigRef config) noexcept = 0;
    virtual void shutdown() noexcept {}
    virtual bool configure(const std::any&) noexcept { return false; }
    virtual void emit(const Session& session, const EventContext& ctx, const std::string& serialized_event) noexcept = 0;

private:
    std::atomic_bool disabled_{false};
};

template <class Sink>
concept SinkComponent = std::derived_from<Sink, ISink> && std::default_initializable<Sink> &&
                        std::same_as<std::remove_cv_t<decltype(Sink::kName)>, const char*>;

}  // namespace analytics::usage_analytics
