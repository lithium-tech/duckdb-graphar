#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include "duckdb_graphar_extension.hpp"
#include "storage/graphar_storage.hpp"

#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/transaction/transaction_manager.hpp>

using namespace duckdb;

// Фиктивные реализации для тестирования
namespace graphar {
class GraphInfo {
public:
    static std::optional<GraphInfo> Load(const std::string& path) {
        if (!path.empty()) {
            return GraphInfo();
        }
        return std::nullopt;
    }
};
} // namespace graphar

class TestGraphArCatalog : public GraphArCatalog {
public:
    TestGraphArCatalog(AttachedDatabase& db, const string& path, const graphar::GraphInfo& graph_info,
                       ClientContext& context, const string& db_name)
        : GraphArCatalog(db, path, graph_info, context, db_name) {}
};

class TestGraphArTransactionManager : public GraphArTransactionManager {
public:
    TestGraphArTransactionManager(AttachedDatabase& db, GraphArCatalog& catalog)
        : GraphArTransactionManager(db, catalog) {}
};

// Фиктивный контекст для тестов
class TestClientContext : public ClientContext {
public:
    TestClientContext() : ClientContext(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr) {}
};

// Тест для функции GraphArAttach
TEST_CASE("GraphArAttach function test") {
    // Подготовка тестовых данных
    StorageExtensionInfo storage_info;
    TestClientContext context;
    AttachedDatabase db(nullptr);
    std::string name = "test_db";
    AttachInfo info;
    info.path = "/test/path";
    AccessMode access_mode = AccessMode::READ_ONLY;

    SECTION("Successful attachment") {
        auto catalog = GraphArAttach(&storage_info, context, db, name, info, access_mode);
        REQUIRE(catalog != nullptr);
        REQUIRE(dynamic_cast<TestGraphArCatalog*>(catalog.get()) != nullptr);
    }

    SECTION("Attachment with empty path") {
        info.path = "";
        REQUIRE_THROWS_AS(GraphArAttach(&storage_info, context, db, name, info, access_mode), std::exception);
    }
}

// Тест для функции GraphArCreateTransactionManager
TEST_CASE("GraphArCreateTransactionManager function test") {
    // Подготовка тестовых данных
    StorageExtensionInfo storage_info;
    AttachedDatabase db(nullptr);
    
    // Создаем тестовый каталог
    TestClientContext context;
    std::string name = "test_db";
    AttachInfo info;
    info.path = "/test/path";
    AccessMode access_mode = AccessMode::READ_ONLY;
    auto catalog = GraphArAttach(&storage_info, context, db, name, info, access_mode);

    SECTION("Transaction manager creation") {
        auto transaction_manager = GraphArCreateTransactionManager(&storage_info, db, *catalog);
        REQUIRE(transaction_manager != nullptr);
        REQUIRE(dynamic_cast<TestGraphArTransactionManager*>(transaction_manager.get()) != nullptr);
    }
}

// Тест для конструктора GraphArStorageExtension
TEST_CASE("GraphArStorageExtension constructor test") {
    GraphArStorageExtension extension;

    SECTION("Attach function pointer set correctly") {
        REQUIRE(extension.attach == GraphArAttach);
    }

    SECTION("Create transaction manager function pointer set correctly") {
        REQUIRE(extension.create_transaction_manager == GraphArCreateTransactionManager);
    }
}