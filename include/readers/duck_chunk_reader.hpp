#pragma once

#include "readers/base_reader.hpp"
#include "utils/func.hpp"

#include <graphar/chunk_info_reader.h>
#include <graphar/fwd.h>
#include <graphar/graph_info.h>
#include <graphar/reader_util.h>
#include <graphar/result.h>
#include <graphar/types.h>

#include <duckdb.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

class QueryStringConstructor {
public:
    enum class QueryType { FIRST, MIDDLE, LAST, SINGLE };
    std::string GetMainQueryString(const std::vector<column_t>& proj_columns,
                                   std::pair<int64_t, int64_t> range = {-1, -1});

    void SetFileType(graphar::FileType new_file_type) { file_type = new_file_type; }

private:
    graphar::FileType file_type = graphar::FileType::PARQUET;
};

class DuckParquetFileReader {
public:
    explicit DuckParquetFileReader(std::shared_ptr<duckdb::Connection> conn_) : conn(conn_) {}
    unique_ptr<QueryResult> ReadFileToTable(const std::string& path, const std::vector<duckdb::column_t>& proj_columns,
                                            std::pair<int64_t, int64_t> range = {-1, -1}) {
        DUCKDB_GRAPHAR_LOG_TRACE("DuckParquetFileReader::ReadFileToTable");
        auto query_string = query_string_constructor.GetMainQueryString(proj_columns, range);
        DUCKDB_GRAPHAR_LOG_DEBUG("::RFTT GetMainQueryString " + query_string + " | " + path);
        conn->Interrupt();
        auto query_result = conn->Query(query_string, Value(path));
        if (query_result->HasError()) {
            DUCKDB_GRAPHAR_LOG_DEBUG("::RFTT error");
            throw std::runtime_error(query_result->GetError());
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("::RFTT success");
        return query_result;
    }

private:
    std::shared_ptr<duckdb::Connection> conn;
    QueryStringConstructor query_string_constructor;
};

template <typename BaseArrowChunkReader>
class BaseDuckChunkReader {
public:
    BaseDuckChunkReader(ClientContext& init_context, std::vector<std::shared_ptr<BaseArrowChunkReader>> init_bases,
                        std::shared_ptr<DuckParquetFileReader> init_file_reader)
        : context(init_context), bases(std::move(init_bases)), file_reader(std::move(init_file_reader)) {}

    bool CheckIfNewFileNeeded() {
        if (cur_chunk && read_rows < cur_chunk->size()) {
            return false;
        }
        read_rows = 0;
        if (cur_result) {
            cur_chunk = cur_result->Fetch();
            if (cur_chunk && cur_chunk->size() > 0) {
                return false;
            }
            cur_result = nullptr;
        }
        return true;
    }

    void AcquirePathUnderLock() {
        next_path = "";
        path_acquired = true;
        if (base_idx >= bases.size()) {
            return;
        }
        auto gc_result = bases[base_idx]->GetChunk();
        while (gc_result.no_more_chunks) {
            base_idx++;
            if (base_idx >= bases.size()) {
                return;
            }
            gc_result = bases[base_idx]->GetChunk();
        }
        auto maybe_path = gc_result.chunk;
        if (maybe_path.has_error()) {
            throw std::runtime_error(maybe_path.error().message());
        }
        next_path = maybe_path.value();
        next_rows_range = gc_result.rows_range;
        next_chunk_idx = gc_result.chunk_idx;
        path_acquired = true;
    }

    idx_t GetRowsNum() {
        DUCKDB_GRAPHAR_LOG_TRACE("BaseDuckChunkReader::GetRowsNum");
        DUCKDB_GRAPHAR_LOG_DEBUG("::GRN path_acquired: " + std::to_string(path_acquired));
        if (path_acquired) {
            DUCKDB_GRAPHAR_LOG_DEBUG("::GRN next_path: " + next_path);
            if (!next_path.empty()) {
                cur_result = file_reader->ReadFileToTable(next_path, proj_columns, next_rows_range);
                cur_chunk = cur_result->Fetch();
                DUCKDB_GRAPHAR_LOG_DEBUG("::GRN new cur_chunk: " + std::to_string(cur_chunk != nullptr));
                cur_result_idx = next_chunk_idx;
            } else {
                cur_chunk = nullptr;
            }
            path_acquired = false;
            read_rows = 0;
            cur_read_idx = 0;
        }

        DUCKDB_GRAPHAR_LOG_DEBUG("::GRN cur_chunk: " + std::to_string(cur_chunk != nullptr));
        if (cur_chunk) {
            DUCKDB_GRAPHAR_LOG_DEBUG("::GRN "" cc " + std::to_string(cur_chunk->size()) + " rr " + std::to_string(read_rows));
        }
        if (cur_chunk && read_rows < cur_chunk->size()) {
            return cur_chunk->size() - read_rows;
        }
        return 0;
    }

    graphar::Result<graphar::GetChunkFinalResult> GetChunk(duckdb::idx_t num_rows) {
        if (GetRowsNum() == 0) {
            throw graphar::Status::IndexError("No more chunks to read!");
        }
        if (num_rows > cur_chunk->size() - read_rows) {
            throw graphar::Status::IndexError("Could read at most " + std::to_string(cur_chunk->size() - read_rows) +
                                              " rows, but " + std::to_string(num_rows) + " were requested");
        }
        auto res = duckdb::make_uniq<duckdb::DataChunk>();
        res->Initialize(context, cur_chunk->GetTypes());
        res->Reference(*cur_chunk);
        res->Slice(read_rows, num_rows);
        read_rows += num_rows;
        cur_read_idx++;
        return std::make_pair(std::move(res), GetChunkIdx(cur_result_idx, cur_read_idx));
    }

    void SelectColumns(std::vector<duckdb::column_t> proj_columns_) {
        if (cur_result) {
            throw std::runtime_error("Can't select columns after reading started!");
        }
        proj_columns = std::move(proj_columns_);
    }

    void Reset() {
        base_idx = 0;
    }

protected:
    std::vector<std::shared_ptr<BaseArrowChunkReader>> bases;
    duckdb::idx_t base_idx = 0;
    std::vector<duckdb::column_t> proj_columns;
    duckdb::idx_t read_rows = 0;
    duckdb::unique_ptr<duckdb::DataChunk> cur_chunk = nullptr;
    duckdb::idx_t cur_read_idx = 0;
    duckdb::unique_ptr<duckdb::QueryResult> cur_result = nullptr;
    duckdb::idx_t cur_result_idx = 0;
    std::shared_ptr<DuckParquetFileReader> file_reader;
    ClientContext& context;
    std::string next_path = "";
    std::pair<int64_t, int64_t> next_rows_range = {-1, -1};
    duckdb::idx_t next_chunk_idx = 0;
    bool path_acquired = false;
};

class DuckVertexChunkReader : public BaseDuckChunkReader<graphar::TSVertexPropertyChunkInfoReader> {
public:
    template <typename... Args>
    explicit DuckVertexChunkReader(const std::shared_ptr<graphar::VertexInfo>& init_vertex_info, Args&&... args)
        : BaseDuckChunkReader<graphar::TSVertexPropertyChunkInfoReader>(std::forward<Args>(args)...),
          vertex_info(init_vertex_info) {}

    static graphar::Result<std::shared_ptr<DuckVertexChunkReader>> Make(
        ClientContext& context, std::shared_ptr<DuckParquetFileReader> file_reader,
        const std::shared_ptr<graphar::VertexInfo>& vertex_info,
        const std::shared_ptr<graphar::PropertyGroup>& property_group, const std::string& prefix,
        const std::vector<std::shared_ptr<graphar::TSVertexPropertyChunkInfoReader>>& init_baseptrs = {}) {
        std::vector<std::shared_ptr<graphar::TSVertexPropertyChunkInfoReader>> bases;
        bases = init_baseptrs;
        return std::make_shared<DuckVertexChunkReader>(vertex_info, context, std::move(bases), file_reader);
    }

private:
    std::shared_ptr<graphar::VertexInfo> vertex_info;
};

template <typename BaseArrowChunkReader>
class DuckEdgeChunkReader : public BaseDuckChunkReader<BaseArrowChunkReader> {
public:
    DuckEdgeChunkReader(const std::vector<std::shared_ptr<BaseArrowChunkReader>>& init_bases,
                        std::shared_ptr<DuckParquetFileReader> init_file_reader, ClientContext& init_context,
                        const std::shared_ptr<graphar::EdgeInfo>& edge_info_, graphar::AdjListType adj_list_type_,
                        const std::string& prefix_)
        : BaseDuckChunkReader<BaseArrowChunkReader>(init_context, std::move(init_bases), std::move(init_file_reader)),
          edge_info(edge_info_),
          adj_list_type(adj_list_type_),
          prefix(prefix_) {}

    static graphar::Result<std::shared_ptr<DuckEdgeChunkReader>> Make(
        ClientContext& context, std::shared_ptr<DuckParquetFileReader> file_reader,
        const std::shared_ptr<graphar::EdgeInfo>& edge_info, graphar::AdjListType adj_list_type,
        const std::string& prefix, const std::vector<std::shared_ptr<BaseArrowChunkReader>>& init_baseptrs = {}) {
        std::vector<std::shared_ptr<BaseArrowChunkReader>> bases;
        bases = init_baseptrs;
        return std::make_shared<DuckEdgeChunkReader>(bases, file_reader, context, edge_info, adj_list_type, prefix);
    }

    static graphar::Result<std::shared_ptr<DuckEdgeChunkReader>> Make(
        ClientContext& context, std::shared_ptr<DuckParquetFileReader> file_reader,
        const std::shared_ptr<graphar::EdgeInfo>& edge_info,
        const std::shared_ptr<graphar::PropertyGroup>& property_group, graphar::AdjListType adj_list_type,
        const std::string& prefix, const std::vector<std::shared_ptr<BaseArrowChunkReader>>& init_baseptrs = {}) {
        std::vector<std::shared_ptr<BaseArrowChunkReader>> bases;
        if (init_baseptrs.empty()) {
            GAR_ASSIGN_OR_RAISE(auto init_baseptr,
                                BaseArrowChunkReader::Make(edge_info, property_group, adj_list_type, prefix))
            bases.push_back(std::move(init_baseptr));
        } else {
            bases = init_baseptrs;
        }
        return std::make_shared<DuckEdgeChunkReader>(bases, file_reader, context, edge_info, adj_list_type, prefix);
    }

    void SelectColumns(std::vector<duckdb::column_t> proj_columns_) {
        if (this->cur_result) {
            throw std::runtime_error("Can't select columns after reading started!");
        }
        this->proj_columns = std::move(proj_columns_);
    }

private:
    graphar::AdjListType adj_list_type;
    std::shared_ptr<graphar::EdgeInfo> edge_info;
    const std::string& prefix;
};

}  // namespace duckdb

namespace graphar {

using DuckAdjListChunkReader = duckdb::DuckEdgeChunkReader<TSAdjListChunkInfoReader>;
using DuckAdjListPropertyChunkReader = duckdb::DuckEdgeChunkReader<TSAdjListPropertyChunkInfoReader>;
using DuckVertexPropertyChunkReader = duckdb::DuckVertexChunkReader;

}  // namespace graphar