
#pragma once

#include "storage/graphar_table_entry.hpp"
#include "storage/graphar_table_information.hpp"

#include <duckdb/catalog/catalog_entry.hpp>

namespace duckdb {
class GraphArTransaction;

class GraphArCatalogSet {
public:
    explicit GraphArCatalogSet(GraphArSchemaEntry& schema);

public:
    virtual optional_ptr<CatalogEntry> GetEntry(ClientContext& context, const EntryLookupInfo& lookup) = 0;
    virtual void Scan(ClientContext& context, const std::function<void(CatalogEntry&)>& callback) = 0;

protected:
    GraphArSchemaEntry& schema;
    Catalog& catalog;
    mutex entry_lock;
};

}  // namespace duckdb
