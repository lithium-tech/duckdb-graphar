#include "storage/graphar_schema_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_transaction.hpp"

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
    // throw NotImplementedException("GraphArSchemaSet::LoadEntries");
    if (!entries.empty()) {
        return;
    }

    auto& ic_catalog = catalog.Cast<GraphArCatalog>();
    auto schemas = GraphArAPI::GetSchemas(context, ic_catalog, {});
    for (const auto& schema : schemas) {
        CreateSchemaInfo info;
        info.schema = GetSchemaName(schema.items);
        info.internal = false;
        auto schema_entry = make_uniq<GraphArSchemaEntry>(catalog, info);
        schema_entry->namespace_items = std::move(schema.items);
        CreateEntryInternal(context, std::move(schema_entry));
    }
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
