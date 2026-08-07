#pragma once

#include "readers/offset_reader.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>

#include <graphar/api/info.h>
#include <graphar/reader_util.h>

namespace duckdb {

class LowEdgeReaderByVertex {
public:
    LowEdgeReaderByVertex(const std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& prefix,
                          graphar::AdjListType adj_list_type)
        : edge_info(edge_info),
          original_prefix(prefix),
          adj_list_type(adj_list_type),
          file_type(edge_info->GetAdjacentList(adj_list_type)->GetFileType()),
          vid(-1),
          vertex_chunk_index(-1),
          cached_vertex_chunk_index(-1),
          cached_edge_chunk_num(0) {
        DUCKDB_GRAPHAR_LOG_TRACE("LowEdgeReaderByVertex::LowEdgeReaderByVertex");
        offset_reader = std::make_unique<OffsetReader>(edge_info, prefix, adj_list_type);
        duckdb_prefix = prefix;
        if (!duckdb_prefix.empty() && duckdb_prefix.back() != '/') {
            duckdb_prefix += '/';
        }
    }

    void SetVertex(graphar::IdType _vid) {
        DUCKDB_GRAPHAR_LOG_TRACE("LowEdgeReaderByVertex::SetVertex " + std::to_string(_vid));
        vid = _vid;
        offset = offset_reader->GetOffset(vid);
        vertex_chunk_index = vid / offset_reader->vertex_chunk_size;
        DUCKDB_GRAPHAR_LOG_DEBUG("LowEdgeReaderByVertex::SetVertex - vid=" + std::to_string(vid) + " offset=[" +
                                 std::to_string(offset.first) + "," + std::to_string(offset.second) + ")" +
                                 " vertex_chunk_index=" + std::to_string(vertex_chunk_index));
        result = nullptr;
    }

    unique_ptr<DataChunk> read() {
        DUCKDB_GRAPHAR_LOG_TRACE("LowEdgeReaderByVertex::read");
        if (!started()) {
            start();
        }
        if (result == nullptr) {
            DUCKDB_GRAPHAR_LOG_DEBUG("LowEdgeReaderByVertex::read - returning empty chunk (no files)");
            return nullptr;
        }
        return std::move(result->Fetch());
    }

    bool started() { return (result != nullptr); }

    void start() {
        DUCKDB_GRAPHAR_LOG_TRACE("LowEdgeReaderByVertex::start");
        if (!conn) {
            throw InternalException("LowEdgeReaderByVertex::start: conn is nullptr");
        }

        if (vertex_chunk_index != cached_vertex_chunk_index) {
            auto maybe_edge_chunk_num =
                graphar::util::GetEdgeChunkNum(original_prefix, edge_info, adj_list_type, vertex_chunk_index);

            if (!maybe_edge_chunk_num.has_value() || maybe_edge_chunk_num.value() <= 0) {
                DUCKDB_GRAPHAR_LOG_DEBUG("No edge chunks found for vertex chunk " + std::to_string(vertex_chunk_index));
                cached_edge_chunk_num = 0;
                cached_vertex_chunk_index = vertex_chunk_index;
                result = nullptr;
                return;
            }

            cached_edge_chunk_num = maybe_edge_chunk_num.value();
            cached_vertex_chunk_index = vertex_chunk_index;
        }

        graphar::IdType edge_chunk_num = cached_edge_chunk_num;
        if (edge_chunk_num <= 0) {
            DUCKDB_GRAPHAR_LOG_DEBUG("No edge chunks to read for vertex " + std::to_string(vid));
            result = nullptr;
            return;
        }

        DUCKDB_GRAPHAR_LOG_DEBUG("Edge chunk num for vertex chunk " + std::to_string(vertex_chunk_index) + ": " +
                                 std::to_string(edge_chunk_num));

        auto chunk_range = GetChunkRange();
        auto begin_chunk = chunk_range.first;
        auto end_chunk = chunk_range.second;

        if (begin_chunk >= edge_chunk_num || begin_chunk >= end_chunk) {
            DUCKDB_GRAPHAR_LOG_DEBUG("No chunks to read for vertex " + std::to_string(vid));
            result = nullptr;
            return;
        }

        if (end_chunk > edge_chunk_num) {
            end_chunk = edge_chunk_num;
        }

        vector<Value> paths_val;
        paths_val.reserve(end_chunk - begin_chunk);

        for (int chunk_index = begin_chunk; chunk_index < end_chunk; ++chunk_index) {
            auto path = edge_info->GetAdjListFilePath(vertex_chunk_index, chunk_index, adj_list_type).value();
            std::string full_path = duckdb_prefix + path;
            paths_val.emplace_back(full_path);
        }

        if (paths_val.empty()) {
            DUCKDB_GRAPHAR_LOG_DEBUG("No chunk files to read for vertex " + std::to_string(vid));
            result = nullptr;
            return;
        }

        std::string query = GetQuery();
        auto offset_in_chunk = offset.first % edge_info->GetChunkSize();
        auto count = offset.second - offset.first;
        Value path_list_val = Value::LIST(paths_val);
        DUCKDB_GRAPHAR_LOG_DEBUG("Executing query with " + std::to_string(paths_val.size()) + " files");
        result = std::move(conn->Query(query, path_list_val, offset_in_chunk, count));

        if (result && result->HasError()) {
            DUCKDB_GRAPHAR_LOG_DEBUG("Query error: " + result->GetError());
            result = nullptr;
        }
    }

    std::vector<std::string> GetChunkPaths() {
        auto range = GetChunkRange();
        auto begin_chunk = range.first, end_chunk = range.second;
        std::vector<std::string> chunks;
        if (begin_chunk >= end_chunk) {
            return chunks;
        }
        chunks.reserve(end_chunk - begin_chunk);
        for (int chunk_index = begin_chunk; chunk_index < end_chunk; ++chunk_index) {
            auto path = edge_info->GetAdjListFilePath(vertex_chunk_index, chunk_index, adj_list_type).value();
            chunks.push_back(path);
        }
        return chunks;
    }

    std::pair<graphar::IdType, graphar::IdType> GetChunkRange() {
        auto end_chunk = offset.second / edge_info->GetChunkSize();
        if (offset.second % edge_info->GetChunkSize() != 0) {
            ++end_chunk;
        }

        return std::make_pair(offset.first / edge_info->GetChunkSize(), end_chunk);
    };

    const long long size() { return offset.second - offset.first; }

    const string GetQuery() {
        switch (file_type) {
            case graphar::FileType::PARQUET:
                return "SELECT #1, #2 FROM read_parquet($1, file_row_number=true) "
                       "WHERE file_row_number BETWEEN $2 AND ($2 + $3 - 1);";
            case graphar::FileType::CSV:
                return "SELECT #1, #2 FROM read_csv($1, skip=($2 + 1)) LIMIT $3;";
            default:
                throw NotImplementedException("LowEdgeReaderByVertex:: Unsupported file type of adj file");
        }
    }

    const graphar::IdType GetVertex() { return vid; }

public:
    pair<graphar::IdType, graphar::IdType> offset;
    std::unique_ptr<Connection> conn;

private:
    const std::shared_ptr<graphar::EdgeInfo> edge_info;
    const std::string original_prefix;
    graphar::AdjListType adj_list_type;
    graphar::FileType file_type;
    graphar::IdType vertex_chunk_index;
    std::unique_ptr<QueryResult> result = nullptr;
    std::unique_ptr<OffsetReader> offset_reader;
    graphar::IdType vid;
    std::string duckdb_prefix;
    graphar::IdType cached_vertex_chunk_index;
    graphar::IdType cached_edge_chunk_num;
};
}  // namespace duckdb
