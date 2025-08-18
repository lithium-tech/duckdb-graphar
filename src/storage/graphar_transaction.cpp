#include "storage/graphar_transaction.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_schema_entry.hpp"
#include "storage/graphar_table_entry.hpp"
#include "utils/global_log_manager.hpp"

#include <iostream>

namespace duckdb {

GraphArTransaction::GraphArTransaction(GraphArCatalog& graphar_catalog, TransactionManager& manager,
                                       ClientContext& context)
    : Transaction(manager, context), graphar_catalog(graphar_catalog), db(*context.db) {}

GraphArTransaction::~GraphArTransaction() = default;

void GraphArTransaction::Start() { DUCKDB_GRAPHAR_LOG_TRACE("GraphArTransaction::Start"); }
void GraphArTransaction::Commit() {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArTransaction::Commit");
    if (!IsReadOnly()) {
        throw NotImplementedException("GraphArTransaction::Commit not implemented for write transactions");
    }
}
void GraphArTransaction::Rollback() { throw NotImplementedException("GraphArTransaction::Rollback"); }

GraphArTransaction& GraphArTransaction::Get(ClientContext& context, Catalog& catalog) {
    return Transaction::Get(context, catalog).Cast<GraphArTransaction>();
}

optional_ptr<CatalogEntry> GraphArTransaction::GetCatalogEntry(const string& entry_name) {
    throw NotImplementedException("GraphArTransaction::GetCatalogEntry");
}

void GraphArTransaction::DropEntry(CatalogType type, const string& table_name, bool cascade) {
    throw NotImplementedException("GraphArTransaction::DropEntry");
}

void GraphArTransaction::ClearTableEntry(const string& table_name) {
    throw NotImplementedException("GraphArTransaction::ClearTableEntry");
}

}  // namespace duckdb
