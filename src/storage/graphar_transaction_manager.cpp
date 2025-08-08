#include "storage/graphar_transaction_manager.hpp"

#include "duckdb/main/attached_database.hpp"

namespace duckdb {

GraphArTransactionManager::GraphArTransactionManager(AttachedDatabase &db_p, GraphArCatalog &graphar_catalog)
    : TransactionManager(db_p), graphar_catalog(graphar_catalog) {
}

Transaction &GraphArTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<GraphArTransaction>(graphar_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData GraphArTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &graphar_transaction = transaction.Cast<GraphArTransaction>();
	graphar_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void GraphArTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &graphar_transaction = transaction.Cast<GraphArTransaction>();
	graphar_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void GraphArTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = GraphArTransaction::Get(context, db.GetCatalog());
}

} // namespace duckdb
