#pragma once

#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/common.hpp>
#include <duckdb/common/enums/access_mode.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>

#include <graphar/graph_info.h>

namespace duckdb {
class GraphArSchemaEntry;
class GraphArTableEntry;

class GraphArCatalog : public Catalog {
public:
    explicit GraphArCatalog(AttachedDatabase& db_p, const std::string& path_,
                            std::shared_ptr<graphar::GraphInfo>& graph_info_, ClientContext& context,
                            std::string& database_name);
    ~GraphArCatalog();

public:
    void Initialize(bool load_builtin) override;
    string GetCatalogType() override { return "graphar"; }

    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo& info) override;

    void ScanSchemas(ClientContext& context, std::function<void(SchemaCatalogEntry&)> callback) override;

    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo& schema_lookup,
                                                  OnEntryNotFound if_not_found) override;

    GraphArSchemaEntry& GetMainSchema() { return *main_schema; }

    PhysicalOperator& PlanCreateTableAs(ClientContext& context, PhysicalPlanGenerator& planner, LogicalCreateTable& op,
                                        PhysicalOperator& plan) override;
    PhysicalOperator& PlanInsert(ClientContext& context, PhysicalPlanGenerator& planner, LogicalInsert& op,
                                 optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator& PlanDelete(ClientContext& context, PhysicalPlanGenerator& planner, LogicalDelete& op,
                                 PhysicalOperator& plan) override;
    PhysicalOperator& PlanUpdate(ClientContext& context, PhysicalPlanGenerator& planner, LogicalUpdate& op,
                                 PhysicalOperator& plan) override;

    unique_ptr<LogicalOperator> BindCreateIndex(Binder& binder, CreateStatement& stmt, TableCatalogEntry& table,
                                                unique_ptr<LogicalOperator> plan) override;

    DatabaseSize GetDatabaseSize(ClientContext& context) override;

    bool InMemory() override;
    string GetDBPath() override;

    const std::string& GetPath() const { return path; }

    const std::shared_ptr<graphar::GraphInfo> GetGraphInfo() const { return graph_info; }

private:
    void DropSchema(ClientContext& context, DropInfo& info) override;

private:
    std::string path;
    std::shared_ptr<graphar::GraphInfo> graph_info;
    ClientData& client_data;
    std::string database_name;
    unique_ptr<GraphArSchemaEntry> main_schema;
    bool in_memory = true;
};

}  // namespace duckdb
