#include "storage/graphar_catalog.hpp"

#include <filesystem>
#include <iostream>

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "storage/graphar_schema_entry.hpp"
#include "storage/graphar_table_entry.hpp"
#include "storage/graphar_transaction.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

namespace duckdb {
GraphArCatalog::GraphArCatalog(AttachedDatabase& db_p, const std::string& path_,
                               std::shared_ptr<graphar::GraphInfo>& graph_info_, ClientContext& context,
                               std::string& database_name)
    : Catalog(db_p),
      path(path_),
      graph_info(graph_info_),
      client_data(ClientData::Get(context)),
      database_name(database_name) {}

GraphArCatalog::~GraphArCatalog() = default;

void GraphArCatalog::Initialize(bool load_builtin) {
    LOG_TRACE("GraphArCatalog::Initialize");
    CreateSchemaInfo info;
    main_schema = make_uniq<GraphArSchemaEntry>(*this, info);
}

optional_ptr<CatalogEntry> GraphArCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo& info) {
    throw NotImplementedException("GraphArCatalog::CreateSchema");
}

void GraphArCatalog::ScanSchemas(ClientContext& context, std::function<void(SchemaCatalogEntry&)> callback) {
    LOG_TRACE("GraphArCatalog::ScanSchemas");
    callback(*main_schema);
}

optional_ptr<SchemaCatalogEntry> GraphArCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo& schema_lookup,
                                                              OnEntryNotFound if_not_found) {
    throw NotImplementedException("GraphArCatalog::LookupSchema");
}

bool GraphArCatalog::InMemory() { return in_memory; }

string GraphArCatalog::GetDBPath() { return path; }

void GraphArCatalog::DropSchema(ClientContext& context, DropInfo& info) {
    throw NotImplementedException("GraphArCatalog::DropSchema");
}

PhysicalOperator& GraphArCatalog::PlanInsert(ClientContext& context, PhysicalPlanGenerator& planner, LogicalInsert& op,
                                             optional_ptr<PhysicalOperator> plan) {
    throw NotImplementedException("GraphArCatalog::PlanInsert");
}
PhysicalOperator& GraphArCatalog::PlanCreateTableAs(ClientContext& context, PhysicalPlanGenerator& planner,
                                                    LogicalCreateTable& op, PhysicalOperator& plan) {
    throw NotImplementedException("GraphArCatalog::PlanCreateTableAs");
}
PhysicalOperator& GraphArCatalog::PlanDelete(ClientContext& context, PhysicalPlanGenerator& planner, LogicalDelete& op,
                                             PhysicalOperator& plan) {
    throw NotImplementedException("GraphArCatalog::PlanDelete");
}
PhysicalOperator& GraphArCatalog::PlanUpdate(ClientContext& context, PhysicalPlanGenerator& planner, LogicalUpdate& op,
                                             PhysicalOperator& plan) {
    throw NotImplementedException("GraphArCatalog::PlanUpdate");
}
unique_ptr<LogicalOperator> GraphArCatalog::BindCreateIndex(Binder& binder, CreateStatement& stmt,
                                                            TableCatalogEntry& table,
                                                            unique_ptr<LogicalOperator> plan) {
    throw NotImplementedException("GraphArCatalog::BindCreateIndex");
}

DatabaseSize GraphArCatalog::GetDatabaseSize(ClientContext& context) {
    throw NotImplementedException("GraphArCatalog::GetDatabaseSize");
}

}  // namespace duckdb
