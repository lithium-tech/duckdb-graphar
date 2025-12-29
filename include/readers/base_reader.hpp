#pragma once

#include "utils/func.hpp"

#include <graphar/arrow/chunk_reader.h>
#include <graphar/chunk_info_reader.h>
#include <graphar/graph_info.h>
#include <graphar/result.h>
#include <graphar/types.h>

#include <duckdb.hpp>
#include <mutex>

namespace graphar {

struct FilterInfo {
    duckdb::idx_t offset_rows = -1;
    duckdb::idx_t last_chunk_rows = -1;
    duckdb::idx_t total_chunks = -1;
};

template <typename ChunkType>
struct GetChunkResult {
    ChunkType chunk;
    std::pair<int64_t, int64_t> rows_range = {-1, -1};
    bool no_more_chunks = false;
};

template <typename T>
concept IsVertexReader =
    std::is_same_v<T, VertexPropertyArrowChunkReader> || std::is_same_v<T, VertexPropertyChunkInfoReader>;

template <typename T>
concept IsEdgeReader =
    std::is_same_v<T, AdjListArrowChunkReader> || std::is_same_v<T, AdjListPropertyArrowChunkReader> ||
    std::is_same_v<T, AdjListChunkInfoReader> || std::is_same_v<T, AdjListPropertyChunkInfoReader>;

template <typename StoredReader>
class ThreadSafeReader {
public:
    using ChunkType = decltype(std::declval<StoredReader>().GetChunk());

    explicit ThreadSafeReader(std::shared_ptr<StoredReader> reader) : reader(std::move(reader)) {}

    template <typename... Args>
    static graphar::Result<std::shared_ptr<ThreadSafeReader>> Make(Args&&... args) {
        GAR_ASSIGN_OR_RAISE(auto stored_reader, StoredReader::Make(std::forward<Args>(args)...));
        return std::make_shared<ThreadSafeReader>(std::move(stored_reader));
    }

    GetChunkResult<ChunkType> GetChunk() {
        std::lock_guard<std::mutex> lock(mtx);
        GetChunkResult<ChunkType> cur_result;
        if (filter_info && chunk_count == filter_info->total_chunks) {
            cur_result.no_more_chunks = true;
            return cur_result;
        }
        if (chunk_count > 0) {
            const auto next_chunk_result = reader->next_chunk();
            if (!next_chunk_result.ok() && next_chunk_result.IsIndexError()) {
                cur_result.no_more_chunks = true;
                return cur_result;
            } else if (!next_chunk_result.ok()) {
                GAR_RAISE_ERROR_NOT_OK(next_chunk_result);
            }
        }
        cur_result.chunk = reader->GetChunk();

        if (filter_info) {
            if (chunk_count == 0) {
                cur_result.rows_range.first = filter_info->offset_rows;
            }
            if (chunk_count == filter_info->total_chunks - 1) {
                cur_result.rows_range.second = filter_info->last_chunk_rows;
            }
        }

        chunk_count++;
        return cur_result;
    }

    void FilterByRangeVertex(const std::pair<int64_t, int64_t>& vid_range, const std::string& filter_column,
                             std::shared_ptr<graphar::VertexInfo> vertex_info)
    requires IsVertexReader<StoredReader>
    {
        if (filter_column != duckdb::GID_COLUMN_INTERNAL) {
            throw duckdb::NotImplementedException("Only graphar id filter is supported");
        }
        filter_info = std::make_unique<FilterInfo>();
        if (vid_range.first >= vid_range.second) {
            filter_info->total_chunks = 0;
            return;
        }
        GAR_RAISE_ERROR_NOT_OK(reader->seek(vid_range.first));
        const auto chunk_size = vertex_info->GetChunkSize();
        filter_info->offset_rows = vid_range.first % chunk_size;
        filter_info->last_chunk_rows = (vid_range.second - 1) % chunk_size + 1;
        filter_info->total_chunks = (vid_range.second - 1) / chunk_size - vid_range.first / chunk_size + 1;
    }

    void FilterByRangeEdge(const std::pair<int64_t, int64_t>& vid_range, const std::string& filter_column,
                           std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& prefix)
    requires IsEdgeReader<StoredReader>
    {
        if (vid_range.first + 1 != vid_range.second) {
            throw duckdb::NotImplementedException("FilterByRangeEdge not implemented for vid range");
        }
        graphar::AdjListType adj_list_type;
        if (filter_column == duckdb::SRC_GID_COLUMN) {
            adj_list_type = graphar::AdjListType::ordered_by_source;
        } else if (filter_column == duckdb::DST_GID_COLUMN) {
            adj_list_type = graphar::AdjListType::ordered_by_dest;
        } else {
            throw duckdb::NotImplementedException("Only src and dst filters are supported");
        }
        if (adj_list_type == graphar::AdjListType::ordered_by_source) {
            reader->seek_src(vid_range.first);
        } else {
            reader->seek_dst(vid_range.first);
        }
        const auto chunk_size = edge_info->GetChunkSize();
        GAR_ASSIGN_OR_RAISE_ERROR(auto offset_pair, graphar::util::GetAdjListOffsetOfVertex(
                                                        edge_info, prefix, adj_list_type, vid_range.first));
        filter_info = std::make_unique<FilterInfo>();
        if (offset_pair.first >= offset_pair.second) {
            filter_info->total_chunks = 0;
            return;
        }
        filter_info->offset_rows = offset_pair.first % chunk_size;
        filter_info->last_chunk_rows = (offset_pair.second - 1) % chunk_size + 1;
        filter_info->total_chunks = (offset_pair.second - 1) / chunk_size - offset_pair.first / chunk_size + 1;
    }

private:
    std::shared_ptr<StoredReader> reader;
    std::mutex mtx;
    duckdb::idx_t chunk_count = 0;

    std::unique_ptr<FilterInfo> filter_info;
};

using TSVertexPropertyArrowChunkReader = ThreadSafeReader<VertexPropertyArrowChunkReader>;
using TSAdjListArrowChunkReader = ThreadSafeReader<AdjListArrowChunkReader>;
using TSAdjListPropertyArrowChunkReader = ThreadSafeReader<AdjListPropertyArrowChunkReader>;
using TSVertexPropertyChunkInfoReader = ThreadSafeReader<VertexPropertyChunkInfoReader>;
using TSAdjListChunkInfoReader = ThreadSafeReader<AdjListChunkInfoReader>;
using TSAdjListPropertyChunkInfoReader = ThreadSafeReader<AdjListPropertyChunkInfoReader>;

}  // namespace graphar