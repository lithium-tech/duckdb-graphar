#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_transaction.hpp"

namespace duckdb {

class GraphArTransactionManager : public TransactionManager {
   public:
    GraphArTransactionManager(AttachedDatabase& db_p, GraphArCatalog& graphar_catalog);

    Transaction& StartTransaction(ClientContext& context) override;
    ErrorData CommitTransaction(ClientContext& context, Transaction& transaction) override;
    void RollbackTransaction(Transaction& transaction) override;

    void Checkpoint(ClientContext& context, bool force = false) override;

   private:
    GraphArCatalog& graphar_catalog;
    mutex transaction_lock;
    reference_map_t<Transaction, unique_ptr<GraphArTransaction>> transactions;
};

}  // namespace duckdb
