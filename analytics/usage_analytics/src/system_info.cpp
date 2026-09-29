#include "usage_analytics/system_info.h"

#include <string>
#include <uvw/util.h>

namespace analytics::usage_analytics {

void AddSystemInfo(boost::json::object& payload) {
    try {
        const auto user = uvw::utilities::os::passwd();
        const auto os = uvw::utilities::os::uname();
        const auto cpus = uvw::utilities::cpu();

        boost::json::object cpu;
        cpu.emplace("count", cpus.size());
        cpu.emplace("available_parallelism", uvw::utilities::available_parallelism());
        if (!cpus.empty()) {
            cpu.emplace("model", cpus.front().model);
            cpu.emplace("speed_mhz", cpus.front().speed);
        }

        boost::json::object user_info;
        user_info.emplace("username", std::string(user.username()));
        user_info.emplace("uid", user.uid());

        boost::json::object os_info;
        os_info.emplace("sysname", std::string(os.sysname()));
        os_info.emplace("release", std::string(os.release()));
        os_info.emplace("version", std::string(os.version()));
        os_info.emplace("machine", std::string(os.machine()));

        boost::json::object memory;
        memory.emplace("total", uvw::utilities::total_memory());
        memory.emplace("available", uvw::utilities::available_memory());

        payload.emplace("user", std::move(user_info));
        payload.emplace("hostname", uvw::utilities::os::hostname());
        payload.emplace("os", std::move(os_info));
        payload.emplace("cpu", std::move(cpu));
        payload.emplace("memory", std::move(memory));
    } catch (...) {
        // Analytics payload enrichment must not affect the host application.
    }
}

}  // namespace analytics::usage_analytics