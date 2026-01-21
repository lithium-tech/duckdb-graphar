#pragma once

#include "readers/offset_reader.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>

#include <graphar/api/info.h>

namespace duckdb {

class LowEdgeReaderByVertex {
public:
    LowEdgeReaderByVertex(const std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& prefix,
                          graphar::AdjListType adj_list_type, std::shared_ptr<OffsetReader> offset_reader)
        : edge_info(edge_info),
          prefix(prefix),
          adj_list_type(adj_list_type),
          file_type(edge_info->GetAdjacentList(adj_list_type)->GetFileType()),
          offset_reader(offset_reader) {}

    void SetVertex(graphar::IdType vid) {
        DUCKDB_GRAPHAR_LOG_TRACE("Reader::SetVertex");
        offset = offset_reader->GetOffset(vid);
        vertex_chunk_index = vid / offset_reader->vertex_chunk_size;
        result = nullptr;
    }

    unique_ptr<DataChunk> read() {
        DUCKDB_GRAPHAR_LOG_TRACE("Reader::read");
        if (!started()) {
            throw NotImplementedException("Reader for Vertex not started");
        }
        return std::move(result->Fetch());
    }

    bool started() { return (result != nullptr); }

    void start(std::unique_ptr<Connection> conn) {
        DUCKDB_GRAPHAR_LOG_TRACE("Reader::start");
        auto paths = GetChunkPaths();
        vector<Value> paths_val;
        paths_val.reserve(paths.size());
        for (auto& el : paths) {
            paths_val.emplace_back(prefix + el);
        }
        std::string query = GetQuery();
        auto offset_in_chunk = offset.first % edge_info->GetChunkSize();
        auto count = offset.second - offset.first;
        Value path_list_val = Value::LIST(paths_val);
        DUCKDB_GRAPHAR_LOG_DEBUG("Reader::params: " + path_list_val.ToString() + " " + std::to_string(offset_in_chunk) +
                                 " " + std::to_string(count));
        result = std::move(conn->Query(query, path_list_val, offset_in_chunk, count));
    }

    std::vector<std::string> GetChunkPaths() {
        auto range = GetChunkRange();
        auto begin_chunk = range.first, end_chunk = range.second;
        std::vector<std::string> chunks(end_chunk - begin_chunk);
        for (int chunk_index = begin_chunk; chunk_index < end_chunk; ++chunk_index) {
            chunks[chunk_index - begin_chunk] =
                edge_info->GetAdjListFilePath(vertex_chunk_index, chunk_index, adj_list_type).value();
        }

        return std::move(chunks);
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
            case graphar::PARQUET:
                return "SELECT #1, #2 FROM read_parquet($1, file_row_number=true) "
                       "WHERE file_row_number BETWEEN $2 AND ($2 + $3 - 1);";
            case graphar::CSV:
                return "SELECT #1, #2 FROM read_csv($1, skip=($2 + 1)) LIMIT $3;";
            default:
                throw NotImplementedException("LowEdgeReaderByVertex:: Unsupported file type of adj file");
        }
    }

private:
    const std::shared_ptr<graphar::EdgeInfo> edge_info;
    const std::string prefix;
    graphar::AdjListType adj_list_type;
    graphar::FileType file_type;
    pair<graphar::IdType, graphar::IdType> offset;
    graphar::IdType vertex_chunk_index;
    std::unique_ptr<QueryResult> result = nullptr;
    std::shared_ptr<OffsetReader> offset_reader;
};
}  // namespace duckdb