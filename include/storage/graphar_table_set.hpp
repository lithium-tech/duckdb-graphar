
#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "storage/graphar_table_entry.hpp"
#include "storage/graphar_table_information.hpp"

namespace duckdb {
struct CreateTableInfo;
class GraphArResult;
class GraphArSchemaEntry;
class GraphArTransaction;

class GraphArTableSet {
   public:
    explicit GraphArTableSet(GraphArSchemaEntry& schema);

   public:
    static unique_ptr<GraphArTableInformation> GetTableInfo(ClientContext& context, GraphArSchemaEntry& schema,
                                                            const string& table_name);
    optional_ptr<CatalogEntry> GetEntry(ClientContext& context, const EntryLookupInfo& lookup);
    void Scan(ClientContext& context, const std::function<void(CatalogEntry&)>& callback);
    void CreateNewEntry(ClientContext& context, GraphArCatalog& catalog, GraphArSchemaEntry& schema,
                        CreateTableInfo& info);
    void LoadEntries(ClientContext& context);
    void FillEntry(ClientContext& context, GraphArTableInformation& table);

    template <typename InfoVector>
    void CreateTables(GraphArCatalog& graphar_catalog, const InfoVector& infos, std::vector<std::string> id_columns);

   private:
    GraphArSchemaEntry& schema;
    Catalog& catalog;
    case_insensitive_map_t<unique_ptr<GraphArTableInformation>> entries;
    mutex entry_lock;
};

}  // namespace duckdb
