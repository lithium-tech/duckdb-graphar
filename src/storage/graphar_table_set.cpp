#include "utils/global_log_manager.hpp"

#include "storage/graphar_catalog.hpp"
#include "storage/graphar_table_set.hpp"

#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/graphar_schema_entry.hpp"
#include "storage/graphar_transaction.hpp"

#include "storage/graphar_table_information.hpp"

#include "utils/func.hpp"

#include <filesystem>

namespace duckdb {

GraphArTableSet::GraphArTableSet(GraphArSchemaEntry& schema) : schema(schema), catalog(schema.ParentCatalog()) {}

void GraphArTableSet::FillEntry(ClientContext& context, GraphArTableInformation& table) {
    DUCKDB_LOG_TRACE(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), "FillEntry");
    if (table.IsFilled()) {
        return;
    }

    auto& graphar_catalog = catalog.Cast<GraphArCatalog>();
}

void GraphArTableSet::Scan(ClientContext& context, const std::function<void(CatalogEntry&)>& callback) {
    DUCKDB_LOG_TRACE(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), "GraphArTableSet::Scan");
    lock_guard<mutex> l(entry_lock);
    LoadEntries(context);
    for (auto& entry : entries) {
        auto& table_info = entry.second;
        FillEntry(context, *table_info);
        callback(table_info->GetEntry());
    }
}

template <typename InfoVector>
void GraphArTableSet::CreateTables(GraphArCatalog& graphar_catalog, const InfoVector& infos,
                                   std::vector<std::string> id_columns) {
    DUCKDB_LOG_TRACE(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), "GraphArTableSet::CreateTables");

    for (auto& info : infos) {
        auto type_info = std::make_unique<CreateTableInfo>();
        auto file_name = GraphArFunctions::GetNameFromInfo(info);
        type_info->table = file_name;
        std::vector<ColumnDefinition> columns;

        for (auto id_column : id_columns) {
            columns.emplace_back(id_column, LogicalTypeId::BIGINT);
        }

        const auto& property_groups = info->GetPropertyGroups();
        for (auto& property_group : property_groups) {
            const auto& properties = property_group->GetProperties();
            for (auto& property : properties) {
                const auto type_name = property.type->ToTypeName();
                const auto property_name = property.name;
                columns.emplace_back(property_name, GraphArFunctions::graphArT2duckT(type_name));
            }
        }

        type_info->columns = ColumnList(std::move(columns));
        CreateSchemaInfo fake_schema_info;
        auto schema = make_uniq<GraphArSchemaEntry>(catalog, fake_schema_info);
        auto table_entry = make_uniq<GraphArTableEntry>(catalog, std::move(schema), *type_info);
        entries[file_name] = make_uniq<GraphArTableInformation>(graphar_catalog, std::move(table_entry), file_name);
    }
}

void GraphArTableSet::LoadEntries(ClientContext& context) {
    DUCKDB_LOG_TRACE(GlobalLogManager::GetLogManager()->GlobalLoggerReference(), "GraphArTableSet::LoadEntries");
    if (!entries.empty()) {
        return;
    }

    auto& graphar_catalog = catalog.Cast<GraphArCatalog>();

    CreateTables(graphar_catalog, graphar_catalog.GetGraphInfo()->GetVertexInfos(), {GID_COLUMN_INTERNAL});
    CreateTables(graphar_catalog, graphar_catalog.GetGraphInfo()->GetEdgeInfos(), {SRC_GID_COLUMN, DST_GID_COLUMN});
}

unique_ptr<GraphArTableInformation> GraphArTableSet::GetTableInfo(ClientContext& context, GraphArSchemaEntry& schema,
                                                                  const string& table_name) {
    throw NotImplementedException("GraphArTableSet::GetTableInformation");
}

}  // namespace duckdb
