#pragma once

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>

#include <graphar/graph_info.h>

namespace duckdb {

struct GraphArTableInformation;

class GraphArTableEntry : public TableCatalogEntry {
public:
    GraphArTableEntry(Catalog& catalog, unique_ptr<SchemaCatalogEntry> schema, CreateTableInfo& info);

public:
    unique_ptr<BaseStatistics> GetStatistics(ClientContext& context, column_t column_id) override;

    TableFunction GetScanFunction(ClientContext& context, unique_ptr<FunctionData>& bind_data) override;
    TableFunction GetScanFunction(ClientContext& context, unique_ptr<FunctionData>& bind_data,
                                  const EntryLookupInfo& lookup) override;

    TableStorageInfo GetStorageInfo(ClientContext& context) override;

    void BindUpdateConstraints(Binder& binder, LogicalGet& get, LogicalProjection& proj, LogicalUpdate& update,
                               ClientContext& context) override;

    void SetTableInfo(shared_ptr<GraphArTableInformation> table_info_) { table_info = table_info_; }

private:
    unique_ptr<SchemaCatalogEntry> schema;
    weak_ptr<GraphArTableInformation> table_info;
};

}  // namespace duckdb
