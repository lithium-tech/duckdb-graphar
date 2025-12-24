#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include "duckdb_graphar_extension.hpp"
#include "storage/graphar_schema_entry.hpp"
#include "storage/graphar_table_entry.hpp"

#include "test_helpers.hpp"

using namespace duckdb;

class GraphArTestSchema : public GraphArSchemaEntry {
public:
	GraphArTestSchema(Catalog &catalog, CreateSchemaInfo &info) : GraphArSchemaEntry(catalog, info) {}
};

class TestCatalogEntry : public TableCatalogEntry {
public:
	TestCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info) : TableCatalogEntry(catalog, schema, info) {}
};

TEST_CASE("Test GraphArSchemaEntry creation", "[graphar]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	// Создаем схему
	CreateSchemaInfo schema_info;
	schema_info.schema_name = "test_schema";
	GraphArTestSchema schema(Catalog::GetCatalog(*con.context, ""), schema_info);

	REQUIRE(schema.name == "test_schema");
	REQUIRE(schema.parent == &Catalog::GetCatalog(*con.context, ""));
}

TEST_CASE("Test GraphArSchemaEntry CreateTable throws", "[graphar]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	CreateSchemaInfo schema_info;
	schema_info.schema_name = "test_schema";
	GraphArTestSchema schema(Catalog::GetCatalog(*con.context, ""), schema_info);

	CreateTableInfo table_info;
	table_info.table = make_uniq<BoundCreateTableInfo>();
	table_info.table->name = "test_table";

	REQUIRE_THROWS_AS(schema.CreateTable(*con.context, *table_info.table), NotImplementedException);
}

TEST_CASE("Test GraphArSchemaEntry DropEntry throws", "[graphar]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	CreateSchemaInfo schema_info;
	schema_info.schema_name = "test_schema";
	GraphArTestSchema schema(Catalog::GetCatalog(*con.context, ""), schema_info);

	DropInfo drop_info;
	drop_info.type = CatalogType::SCHEMA_ENTRY;
	drop_info.name = "test_schema";

	REQUIRE_THROWS_AS(schema.DropEntry(*con.context, drop_info), NotImplementedException);
}

TEST_CASE("Test GraphArSchemaEntry CreateIndex throws", "[graphar]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	CreateSchemaInfo schema_info;
	schema_info.schema_name = "test_schema";
	GraphArTestSchema schema(Catalog::GetCatalog(*con.context, ""), schema_info);

	CreateIndexInfo index_info;
	index_info.constraint_type = IndexConstraintType::NONE;
	index_info.table = "test_table";
	index_info.name = "test_index";

	REQUIRE_THROWS_AS(schema.CreateIndex(*con.context, index_info, *schema.tables.GetEntry(*con.context, EntryLookupInfo("test_table"))), NotImplementedException);
}

TEST_CASE("Test GraphArSchemaEntry CreateView throws", "[graphar]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	CreateSchemaInfo schema_info;
	schema_info.schema_name = "test_schema";
	GraphArTestSchema schema(Catalog::GetCatalog(*con.context, ""), schema_info);

	CreateViewInfo view_info;
	view_info.view_name = "test_view";

	REQUIRE_THROWS_AS(schema.CreateView(*con.context, view_info), NotImplementedException);
}

TEST_CASE("Test GraphArSchemaEntry Scan with supported types", "[graphar]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	CreateSchemaInfo schema_info;
	schema_info.schema_name = "test_schema";
    auto &catalog = Catalog::GetCatalog(*con.context, "");
	GraphArTestSchema schema(catalog, schema_info);

	bool scanned = false;
	schema.Scan(*con.context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		// Должно быть пусто
		REQUIRE(false);
		scanned = true;
	});

	REQUIRE(!scanned);

	// Добавляем таблицу вручную для проверки
	auto table_info = make_uniq<CreateTableInfo>();
	table_info->table = make_uniq<BoundCreateTableInfo>();
	table_info->table->name = "manual_table";
	auto table_entry = make_uniq<TestCatalogEntry>(schema, *table_info);
	schema.tables.AddEntry(*con.context, std::move(table_entry));

	schema.Scan(*con.context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		REQUIRE(entry.name == "manual_table");
		scanned = true;
	});

	REQUIRE(scanned);
}

TEST_CASE("Test GraphArSchemaEntry LookupEntry", "[graphar]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	Connection con(db);

	CreateSchemaInfo schema_info;
	schema_info.schema_name = "test_schema";
	GraphArTestSchema schema(Catalog::GetCatalog(*con.context, ""), schema_info);

	// Создаем и добавляем таблицу
	auto table_info = make_uniq<CreateTableInfo>();
	table_info->table = make_uniq<BoundCreateTableInfo>();
	table_info->table->name = "lookup_table";
	auto table_entry = make_uniq<TestCatalogEntry>(schema, *table_info);
	schema.tables.AddEntry(*con.context, std::move(table_entry));

	EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, "lookup_table");
	auto entry = schema.LookupEntry(*con.context, lookup_info);
	REQUIRE(entry != nullptr);
	REQUIRE(entry->name == "lookup_table");

	EntryLookupInfo bad_lookup_info(CatalogType::TABLE_ENTRY, "nonexistent_table");
	auto bad_entry = schema.LookupEntry(*con.context, bad_lookup_info);
	REQUIRE(bad_entry == nullptr);
}