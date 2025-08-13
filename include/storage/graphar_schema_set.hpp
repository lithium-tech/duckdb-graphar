
#pragma once

// #include "storage/graphar_schema_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
// struct CreateSchemaInfo;

class GraphArSchemaSet {
public:
    explicit GraphArSchemaSet(Catalog& catalog);

public:
    void LoadEntries(ClientContext& context);
    optional_ptr<CatalogEntry> GetEntry(ClientContext& context, const string& name);
    void Scan(ClientContext& context, const std::function<void(CatalogEntry&)>& callback);

protected:
    optional_ptr<CatalogEntry> CreateEntryInternal(ClientContext& context, unique_ptr<CatalogEntry> entry);

private:
    Catalog& catalog;
    case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
    mutex entry_lock;
};

}  // namespace duckdb
