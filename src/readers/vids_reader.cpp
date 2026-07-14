#include "readers/vids_reader.hpp"

#include "functions/table/hop_base.hpp"

namespace graphar {
using namespace duckdb;

Status VidsChunkReader::next_chunk() {
    DUCKDB_GRAPHAR_LOG_TRACE("VidsChunkReader::next_chunk");
    if (gstate_ptr == nullptr) {
        return Status::Invalid("No global state pointer");
    }

    if (!gstate_ptr->vertexes.empty()) {
        return Status::OK();
    }
    return Status::IndexError("No more chunks to read!!");
}

Result<IdType> VidsChunkReader::GetChunk() {
    DUCKDB_GRAPHAR_LOG_TRACE("VidsChunkReader::GetChunk");
    if (gstate_ptr == nullptr) {
        throw InternalException("Gst is nullptr: ");
    }
    auto vid = gstate_ptr->vertexes.front();
    gstate_ptr->vertexes.pop();
    return vid;
}

Status VidsChunkReader::Init(duckdb::HopBaseGlobalTableFunctionState* init_gstate_ptr) {
    DUCKDB_GRAPHAR_LOG_TRACE("VidsChunkReader::Init");
    if (!init_gstate_ptr) {
        return Status::Invalid("HopBaseGlobalTableFunctionState is nullptr");
    }
    gstate_ptr = init_gstate_ptr;
    return Status::OK();
}

}  // namespace graphar