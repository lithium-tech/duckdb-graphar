#pragma once

#include "readers/base_reader.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <graphar/arrow/chunk_reader.h>
#include <graphar/result.h>

#include <duckdb.hpp>
#include <iostream>

namespace duckdb {

template <typename BaseArrowChunkReader>
requires(std::is_same_v<BaseArrowChunkReader, graphar::TSVertexPropertyArrowChunkReader> ||
         std::is_same_v<BaseArrowChunkReader, graphar::TSAdjListArrowChunkReader> ||
         std::is_same_v<BaseArrowChunkReader, graphar::TSAdjListPropertyArrowChunkReader>)
class DuckArrowChunkReader {
public:
    DuckArrowChunkReader(std::vector<std::shared_ptr<BaseArrowChunkReader>> init_bases, ClientContext& init_context)
        : base(std::move(init_bases)), context(init_context) {}

    static graphar::Result<std::shared_ptr<DuckArrowChunkReader>> Make(
        ClientContext& context, std::vector<std::shared_ptr<BaseArrowChunkReader>> base_ptrs) {
        if (base_ptrs.empty()) {
            return graphar::Status::Invalid("base_ptrs can't be empty!");
        }
        return std::make_shared<DuckArrowChunkReader>(std::move(base_ptrs), context);
    }

    template <typename... Args>
    static graphar::Result<std::shared_ptr<DuckArrowChunkReader>> Make(ClientContext& context, Args&&... args) {
        GAR_ASSIGN_OR_RAISE(auto base_ptr, BaseArrowChunkReader::Make(std::forward<Args>(args)...));
        std::vector<std::shared_ptr<BaseArrowChunkReader>> base_ptrs;
        base_ptrs.push_back(std::move(base_ptr));
        return std::make_shared<DuckArrowChunkReader>(std::move(base_ptrs), context);
    }

    bool CheckIfNewFileNeeded() {
        if (cur_chunk && read_rows < cur_chunk->size()) {
            return false;
        }
        read_rows = 0;
        return true;
    }

    void AcquirePathUnderLock() {
        auto gc_result = base[current_base_idx]->GetChunk();
        while (gc_result.no_more_chunks) {
            current_base_idx++;
            if (current_base_idx >= base.size()) {
                next_arrow_table = nullptr;
                path_acquired = true;
                return;
            }
            gc_result = base[current_base_idx]->GetChunk();
        }
        
        auto maybe_arrow_table = gc_result.chunk;
        if (maybe_arrow_table.has_error()) {
            DUCKDB_GRAPHAR_LOG_DEBUG("Error while getting chunk from base reader: " +
                                     maybe_arrow_table.error().message());
            throw std::runtime_error(maybe_arrow_table.error().message());
        }
        next_arrow_table = maybe_arrow_table.value();
        next_chunk_idx = gc_result.chunk_idx;
        path_acquired = true;
    }

    idx_t GetRowsNum() {
        if (path_acquired) {
            if (next_arrow_table) {
                if (!cur_chunk) {
                    cur_chunk = make_uniq<DataChunk>();
                }
                ConvertArrowTableToDataChunk(*next_arrow_table, *cur_chunk, proj_columns, context);
                cur_result_idx = next_chunk_idx;
                read_rows = 0;
                cur_read_idx = 0;
            } else {
                cur_chunk = nullptr;
            }
            path_acquired = false;
        }
        
        if (cur_chunk && read_rows < cur_chunk->size()) {
            return cur_chunk->size() - read_rows;
        }
        return 0;
    }

    graphar::Result<graphar::GetChunkFinalResult> GetChunk(idx_t num_rows) {
        if (GetRowsNum() == 0) {
            throw graphar::Status::IndexError("No more chunks to read!");
        }
        if (num_rows > cur_chunk->size() - read_rows) {
            throw graphar::Status::IndexError("Can't read this many rows");
        }
        auto res = make_uniq<DataChunk>();
        res->Initialize(context, cur_chunk->GetTypes());
        res->Reference(*cur_chunk);
        res->Slice(read_rows, num_rows);
        read_rows += num_rows;
        cur_read_idx++;
        return std::make_pair(std::move(res), GetChunkIdx(cur_result_idx, cur_read_idx));
    }

    void FilterByRange(std::pair<int64_t, int64_t> vid_range, const std::string& filter_column) {
        throw NotImplementedException("Arrow-based readers do not suppport filtering!");
    }

    void SelectColumns(std::vector<column_t> proj_columns_) { proj_columns = std::move(proj_columns_); }

private:
    std::vector<column_t> proj_columns;
    ClientContext& context;
    std::vector<std::shared_ptr<BaseArrowChunkReader>> base;
    size_t current_base_idx = 0;
    idx_t read_rows = 0;
    unique_ptr<DataChunk> cur_chunk = nullptr;
    duckdb::idx_t cur_read_idx = 0;
    duckdb::idx_t cur_result_idx = 0;

    // Added for thread-safe lock separation
    std::shared_ptr<arrow::Table> next_arrow_table = nullptr;
    duckdb::idx_t next_chunk_idx = 0;
    bool path_acquired = false;
};

}  // namespace duckdb

namespace graphar {

using DuckVertexPropertyArrowChunkReader = duckdb::DuckArrowChunkReader<TSVertexPropertyArrowChunkReader>;
using DuckAdjListArrowChunkReader = duckdb::DuckArrowChunkReader<TSAdjListArrowChunkReader>;
using DuckAdjListPropertyArrowChunkReader = duckdb::DuckArrowChunkReader<TSAdjListPropertyArrowChunkReader>;

}  // namespace graphar
