#include "storage/graphar_catalog.hpp"
#include "storage/graphar_transaction.hpp"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>

namespace duckdb {

GraphArSchemaSet::GraphArSchemaSet(Catalog& catalog) : catalog(catalog) {}

optional_ptr<CatalogEntry> GraphArSchemaSet::GetEntry(ClientContext& context, const string& name) {
    LoadEntries(context);
    lock_guard<mutex> l(entry_lock);
    auto entry = entries.find(name);
    if (entry == entries.end()) {
        return nullptr;
    }
    return entry->second.get();
}

void GraphArSchemaSet::Scan(ClientContext& context, const std::function<void(CatalogEntry&)>& callback) {
    lock_guard<mutex> l(entry_lock);
    LoadEntries(context);
    for (auto& entry : entries) {
        callback(*entry.second);
    }
}

static string GetSchemaName(const vector<string>& items) { return StringUtil::Join(items, "."); }

void GraphArSchemaSet::LoadEntries(ClientContext& context) {
    throw NotImplementedException("GraphArSchemaSet::LoadEntries");
}

optional_ptr<CatalogEntry> GraphArSchemaSet::CreateEntryInternal(ClientContext& context,
                                                                 unique_ptr<CatalogEntry> entry) {
    auto result = entry.get();
    if (result->name.empty()) {
        throw InternalException("GraphArSchemaSet::CreateEntry called with empty name");
    }
    entries.insert(make_pair(result->name, std::move(entry)));
    return result;
}

}  // namespace duckdb
