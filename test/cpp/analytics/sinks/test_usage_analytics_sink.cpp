#include "usage_analytics/usage_analytics.h"

namespace {

using analytics::usage_analytics::EventContext;
using analytics::usage_analytics::ISink;
using analytics::usage_analytics::Session;
using analytics::usage_analytics::Tracker;

class TestUsageAnalyticsSink final : public ISink {
public:
    static constexpr const char* kName = "test";

    bool init(const Tracker&, config::ConfigRef) noexcept override { return true; }

    void emit(const Session&, const EventContext&, const std::string&) noexcept override {}
};

TestUsageAnalyticsSink test_usage_analytics_sink;
TRACKER_REGISTER_COMPONENT_SINK(test_usage_analytics_sink);

}  // namespace
