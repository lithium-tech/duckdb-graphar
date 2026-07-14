#pragma once

#include <duckdb/main/connection.hpp>

#include <graphar/api/info.h>

// #include "functions/table/hop_base.hpp"
#include "utils/global_log_manager.hpp"

namespace duckdb {
class HopBaseGlobalTableFunctionState;
}

namespace graphar {

class VidsChunkReader {
public:
    VidsChunkReader() = default;

    Status next_chunk();

    Result<IdType> GetChunk();

    static Result<std::shared_ptr<VidsChunkReader>> Make() {
        auto reader = std::make_shared<VidsChunkReader>();
        return reader;
    }

    static Result<std::shared_ptr<VidsChunkReader>> Make(
        std::shared_ptr<duckdb::HopBaseGlobalTableFunctionState>& gstate_ptr) {
        auto reader = std::make_shared<VidsChunkReader>();
        if (!reader->Init(gstate_ptr.get()).ok()) {
            return Status::Invalid("Failed to initialize VidsChunkReader");
        }
        return reader;
    }

    Status Init(duckdb::HopBaseGlobalTableFunctionState* init_gstate_ptr);

private:
    duckdb::HopBaseGlobalTableFunctionState* gstate_ptr;
};

}  // namespace graphar