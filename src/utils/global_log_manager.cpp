#include "utils/global_log_manager.hpp"

namespace duckdb {

LogManager* GlobalLogManager::log_manager = nullptr;

void GlobalLogManager::Initialize(DatabaseInstance& db, LogLevel log_level) {
    log_manager = &db.GetLogManager();
    log_manager->SetEnableLogging(true);
    log_manager->SetLogStorage(db, "stdout");
    log_manager->SetLogLevel(log_level);
}
}  // namespace duckdb
