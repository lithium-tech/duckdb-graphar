#include "utils/global_log_manager.hpp"

#include "storage/graphar_schema_entry.hpp"
#include "storage/graphar_table_entry.hpp"
#include "storage/graphar_transaction.hpp"
#include "storage/graphar_catalog.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

#include <iostream>

namespace duckdb {

GraphArSchemaEntry::GraphArSchemaEntry(Catalog& catalog, CreateSchemaInfo& info)
    : SchemaCatalogEntry(catalog, info), tables(*this) {
}

GraphArTransaction &GetGraphArTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<GraphArTransaction>();
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::CreateTable");
}

void GraphArSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::DropEntry");
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::CreateFunction");
}

void GraphArUnqualifyColumnRef(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto name = std::move(colref.column_names.back());
		colref.column_names = {std::move(name)};
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr, GraphArUnqualifyColumnRef);
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                       TableCatalogEntry &table) {
	throw NotImplementedException("GraphArSchemaEntry::CreateIndex");
}

string GetUCCreateView(CreateViewInfo &info) {
	throw NotImplementedException("Get Create View");
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::CreateView");
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::CreateType");
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::CreateSequence");
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                               CreateTableFunctionInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::CreateTableFunction");
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                              CreateCopyFunctionInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::CreateCopyFunction");
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::CreatePragmaFunction");
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::CreateCollation");
}

void GraphArSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("GraphArSchemaEntry::Alter");
}

static bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

void GraphArSchemaEntry::Scan(ClientContext &context, CatalogType type,
                          const std::function<void(CatalogEntry &)> &callback) {
	DUCKDB_GRAPHAR_LOG_TRACE("GraphArSchemaEntry::Scan");
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	GetCatalogSet(type).Scan(context, callback);
}

void GraphArSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

optional_ptr<CatalogEntry> GraphArSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                       const EntryLookupInfo &lookup_info) {
	DUCKDB_GRAPHAR_LOG_TRACE("GraphArSchemaEntry::LookupEntry");
	auto type = lookup_info.GetCatalogType();
	if (!CatalogTypeIsSupported(type)) {
		return nullptr;
	}
	auto &context = transaction.GetContext();
	return GetCatalogSet(type).GetEntry(context, lookup_info);
}

GraphArTableSet &GraphArSchemaEntry:: GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	default:
		throw InternalException("Type not supported for GetCatalogSet");
	}
}

} // namespace duckdb
