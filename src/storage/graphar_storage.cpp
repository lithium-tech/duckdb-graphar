#include "storage/graphar_storage.hpp"

#include "storage/graphar_catalog.hpp"
#include "storage/graphar_transaction_manager.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/transaction/transaction_manager.hpp>

#include <duckdb.hpp>

namespace duckdb {

static unique_ptr<Catalog> GraphArAttach(StorageExtensionInfo* storage_info, ClientContext& context,
                                         AttachedDatabase& db, const string& name, AttachInfo& info,
                                         AccessMode access_mode) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArAttach");
    auto maybe_graph_info = graphar::GraphInfo::Load(info.path);
    if (maybe_graph_info.has_error()) {
        throw IOException("Failed to load graph info from path: %s", info.path);
    }
    auto graph_info = maybe_graph_info.value();
    return make_uniq<GraphArCatalog>(db, info.path, graph_info, context, db.name);
}

static unique_ptr<TransactionManager> GraphArCreateTransactionManager(StorageExtensionInfo* storage_info,
                                                                      AttachedDatabase& db, Catalog& catalog) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArCreateTransactionManager");
    auto& graphar_catalog = catalog.Cast<GraphArCatalog>();
    return make_uniq<GraphArTransactionManager>(db, graphar_catalog);
}

GraphArStorageExtension::GraphArStorageExtension() {
    attach = GraphArAttach;
    create_transaction_manager = GraphArCreateTransactionManager;
}

}  // namespace duckdb
