#pragma once

#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <graphar/arrow/chunk_reader.h>
#include <graphar/result.h>

#include <duckdb.hpp>
#include <iostream>

namespace duckdb {

template <typename BaseArrowChunkReader>
requires(std::is_same_v<BaseArrowChunkReader, graphar::VertexPropertyArrowChunkReader> ||
         std::is_same_v<BaseArrowChunkReader, graphar::AdjListArrowChunkReader> ||
         std::is_same_v<BaseArrowChunkReader, graphar::AdjListPropertyArrowChunkReader>)
class DuckArrowChunkReader {
public:
    DuckArrowChunkReader(std::shared_ptr<BaseArrowChunkReader> init_base, ClientContext& init_context)
        : base(std::move(init_base)), context(init_context) {}

    template <typename... Args>
    static graphar::Result<std::shared_ptr<DuckArrowChunkReader>> Make(ClientContext& context, Args&&... args) {
        GAR_ASSIGN_OR_RAISE(auto base_ptr, BaseArrowChunkReader::Make(std::forward<Args>(args)...));
        return std::make_shared<DuckArrowChunkReader>(std::move(base_ptr), context);
    }

    idx_t ReserveRowsToRead() {
        if (!cur_chunk) {
            GAR_ASSIGN_OR_RAISE_ERROR(auto arrow_table, base->GetChunk());
            cur_chunk = make_uniq<DataChunk>();
            ConvertArrowTableToDataChunk(*arrow_table, *cur_chunk, proj_columns, context);
        }
        if (read_rows == cur_chunk->size()) {
            if (!base->next_chunk().ok()) {
                return 0;
            }
            read_rows = 0;
            GAR_ASSIGN_OR_RAISE_ERROR(auto arrow_table, base->GetChunk());
            ConvertArrowTableToDataChunk(*arrow_table, *cur_chunk, proj_columns, context);
        }
        return cur_chunk->size() - read_rows;
    }

    graphar::Result<unique_ptr<DataChunk>> GetChunk(idx_t num_rows) {
        if (ReserveRowsToRead() == 0) {
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
        return std::move(res);
    }

    void FilterByRange(std::pair<int64_t, int64_t> vid_range, const std::string& filter_column) {
        throw NotImplementedException("Arrow-based readers do not suppport filtering!");
    }

    void SelectColumns(std::vector<column_t>& proj_columns_) { proj_columns = std::move(proj_columns_); }

private:
    std::vector<column_t> proj_columns;
    ClientContext& context;
    std::shared_ptr<BaseArrowChunkReader> base;
    idx_t read_rows = 0;
    unique_ptr<DataChunk> cur_chunk = nullptr;
};

}  // namespace duckdb

namespace graphar {

using DuckVertexPropertyArrowChunkReader = duckdb::DuckArrowChunkReader<VertexPropertyArrowChunkReader>;
using DuckAdjListArrowChunkReader = duckdb::DuckArrowChunkReader<AdjListArrowChunkReader>;
using DuckAdjListPropertyArrowChunkReader = duckdb::DuckArrowChunkReader<AdjListPropertyArrowChunkReader>;

}  // namespace graphar