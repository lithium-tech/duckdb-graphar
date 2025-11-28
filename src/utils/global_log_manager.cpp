#include "utils/global_log_manager.hpp"

namespace duckdb {

std::optional<std::reference_wrapper<LogManager>> GlobalLogManager::log_manager = std::nullopt;

void GlobalLogManager::Initialize(DatabaseInstance& db, LogLevel log_level) {
    log_manager = db.GetLogManager();
    LogManager& log_manager_ref = log_manager->get();
    log_manager_ref.SetEnableLogging(true);
    log_manager_ref.SetLogStorage(db, "stdout");
    log_manager_ref.SetLogLevel(log_level);
}
}  // namespace duckdb