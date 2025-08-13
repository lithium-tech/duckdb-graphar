#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class GraphArStorageExtension : public StorageExtension {
public:
    GraphArStorageExtension();
};

}  // namespace duckdb
