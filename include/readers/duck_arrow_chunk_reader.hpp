#pragma once

#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <graphar/arrow/chunk_reader.h>
#include <graphar/result.h>

#include <duckdb.hpp>
#include <iostream>

namespace graphar {

template <typename BaseArrowChunkReader>
requires(std::is_same_v<BaseArrowChunkReader, VertexPropertyArrowChunkReader> ||
         std::is_same_v<BaseArrowChunkReader, AdjListArrowChunkReader> ||
         std::is_same_v<BaseArrowChunkReader, AdjListPropertyArrowChunkReader>)
class DuckArrowChunkReader {
public:
    DuckArrowChunkReader(std::shared_ptr<BaseArrowChunkReader> base_, duckdb::ClientContext& context_)
        : base(std::move(base_)), context(context_) {}

    template <typename... Args>
    static Result<std::shared_ptr<DuckArrowChunkReader>> Make(duckdb::ClientContext& context, Args&&... args) {
        GAR_ASSIGN_OR_RAISE(auto base_ptr, BaseArrowChunkReader::Make(std::forward<Args>(args)...));
        return std::make_shared<DuckArrowChunkReader>(std::move(base_ptr), context);
    }

    duckdb::idx_t EnsureNotRead() {
        if (!cur_chunk) {
            GAR_ASSIGN_OR_RAISE_ERROR(auto arrow_table, base->GetChunk());
            cur_chunk = duckdb::make_uniq<duckdb::DataChunk>();
            duckdb::ConvertArrowTableToDataChunk(*arrow_table, *cur_chunk, proj_columns, context);
        }
        if (read_rows == cur_chunk->size()) {
            if (!base->next_chunk().ok()) {
                return 0;
            }
            read_rows = 0;
            GAR_ASSIGN_OR_RAISE_ERROR(auto arrow_table, base->GetChunk());
            duckdb::ConvertArrowTableToDataChunk(*arrow_table, *cur_chunk, proj_columns, context);
        }
        return cur_chunk->size() - read_rows;
    }

    graphar::Result<duckdb::unique_ptr<duckdb::DataChunk>> GetChunk(duckdb::idx_t num_rows) {
        if (EnsureNotRead() == 0) {
            throw graphar::Status::IndexError("No more chunks to read!");
        }
        if (num_rows > cur_chunk->size() - read_rows) {
            throw graphar::Status::IndexError("Can't read this many rows");
        }
        auto res = duckdb::make_uniq<duckdb::DataChunk>();
        res->Initialize(context, cur_chunk->GetTypes());
        res->Reference(*cur_chunk);
        res->Slice(read_rows, num_rows);
        read_rows += num_rows;
        return std::move(res);
    }

    void FilterByRange(std::pair<int64_t, int64_t> vid_range, const std::string& filter_column) {
        throw duckdb::NotImplementedException("Arrow-based readers do not suppport filtering!");
    }

    void SelectColumns(std::vector<duckdb::column_t>& proj_columns_) { proj_columns = std::move(proj_columns_); }

private:
    std::vector<duckdb::column_t> proj_columns;
    duckdb::ClientContext& context;
    std::shared_ptr<BaseArrowChunkReader> base;
    duckdb::idx_t read_rows = 0;
    duckdb::unique_ptr<duckdb::DataChunk> cur_chunk = nullptr;
};

using DuckVertexPropertyArrowChunkReader = DuckArrowChunkReader<VertexPropertyArrowChunkReader>;
using DuckAdjListArrowChunkReader = DuckArrowChunkReader<AdjListArrowChunkReader>;
using DuckAdjListPropertyArrowChunkReader = DuckArrowChunkReader<AdjListPropertyArrowChunkReader>;

}  // namespace graphar