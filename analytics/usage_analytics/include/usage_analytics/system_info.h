#pragma once

#include <boost/json/object.hpp>

namespace analytics::usage_analytics {

// Adds a block of user/environment information to the given payload:
//   user.{username,uid}, hostname, os.{sysname,release,version,machine},
//   cpu.{count,available_parallelism,model,speed_mhz},
//   memory.{total,available}.
// Gathered via uvw/libuv; failures degrade to empty/default values.
void AddSystemInfo(boost::json::object& payload);

}  // namespace analytics::usage_analytics