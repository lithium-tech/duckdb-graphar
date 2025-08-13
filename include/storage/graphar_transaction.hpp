#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "storage/graphar_schema_set.hpp"

namespace duckdb {
class GraphArCatalog;
class GraphArTableEntry;

class GraphArTransaction : public Transaction {
   public:
    GraphArTransaction(GraphArCatalog& graphar_catalog, TransactionManager& manager, ClientContext& context);
    ~GraphArTransaction() override;

   public:
    void Start();
    void Commit();
    void Rollback();

    optional_ptr<CatalogEntry> GetCatalogEntry(const string& table_name);
    void DropEntry(CatalogType type, const string& table_name, bool cascade);
    void ClearTableEntry(const string& table_name);

    static GraphArTransaction& Get(ClientContext& context, Catalog& catalog);

   private:
    DatabaseInstance& db;
    GraphArCatalog& graphar_catalog;
    case_insensitive_map_t<unique_ptr<CatalogEntry>> catalog_entries;
};

}  // namespace duckdb
