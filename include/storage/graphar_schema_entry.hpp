#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "storage/graphar_table_set.hpp"

namespace duckdb {
class GraphArTransaction;
class GraphArTableEntry;

class GraphArSchemaEntry : public SchemaCatalogEntry {
   public:
    GraphArSchemaEntry(Catalog& catalog, CreateSchemaInfo& info);

   public:
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo& info) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo& info) override;
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo& info,
                                           TableCatalogEntry& table) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo& info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo& info) override;
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
                                                   CreateTableFunctionInfo& info) override;
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
                                                  CreateCopyFunctionInfo& info) override;
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
                                                    CreatePragmaFunctionInfo& info) override;
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo& info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo& info) override;
    void Alter(CatalogTransaction transaction, AlterInfo& info) override;
    void Scan(ClientContext& context, CatalogType type, const std::function<void(CatalogEntry&)>& callback) override;
    void Scan(CatalogType type, const std::function<void(CatalogEntry&)>& callback) override;
    void DropEntry(ClientContext& context, DropInfo& info) override;
    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo& lookup_info) override;

   private:
    void AlterTable(GraphArTransaction& transaction, RenameTableInfo& info);
    void AlterTable(GraphArTransaction& transaction, RenameColumnInfo& info);
    void AlterTable(GraphArTransaction& transaction, AddColumnInfo& info);
    void AlterTable(GraphArTransaction& transaction, RemoveColumnInfo& info);

    void TryDropEntry(ClientContext& context, CatalogType catalog_type, const string& name);

   private:
    GraphArTableSet& GetCatalogSet(CatalogType type);
    GraphArTableSet tables;
};

}  // namespace duckdb
