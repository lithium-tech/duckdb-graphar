#include "storage/graphar_catalog_set.hpp"

#include "storage/graphar_schema_entry.hpp"

namespace duckdb {

GraphArCatalogSet::GraphArCatalogSet(GraphArSchemaEntry& schema) : schema(schema), catalog(schema.ParentCatalog()) {}

}  // namespace duckdb
