#pragma once

#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
class GlobalLogManager {
public:
    static void Initialize(DatabaseInstance& db, LogLevel log_level = LogLevel::LOG_INFO);

    static optional_ptr<LogManager> GetLogManager() { return log_manager; }

private:
    GlobalLogManager() = default;
    ~GlobalLogManager() = default;
    GlobalLogManager(const GlobalLogManager&) = delete;
    GlobalLogManager& operator=(const GlobalLogManager&) = delete;

    static optional_ptr<LogManager> log_manager;
};
};  // namespace duckdb

#define LOG_TRACE(msg) DUCKDB_LOG_TRACE(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), msg)
#define LOG_DEBUG(msg) DUCKDB_LOG_DEBUG(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), msg)
#define LOG_INFO(msg) DUCKDB_LOG_INFO(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), msg)
#define LOG_WARN(msg) DUCKDB_LOG_WARN(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), msg)
#define LOG_ERROR(msg) DUCKDB_LOG_ERROR(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), msg)
#define LOG_FATAL(msg) DUCKDB_LOG_FATAL(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), msg)