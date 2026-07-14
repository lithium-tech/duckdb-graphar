#pragma once

#include "readers/base_reader.hpp"
#include "readers/vids_reader.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <graphar/result.h>

#include <duckdb.hpp>

namespace duckdb {

class QueryReadEdgesStringConstructor {
public:
    enum class QueryFilterType { SRC, DST };
    static std::string GetQueryReadString(const std::string& graph_info_path, std::shared_ptr<graphar::EdgeInfo> info);
    std::string const GetQueryFilterString(const std::vector<graphar::IdType>& vids);

    static std::string GetQueryColumnsString(const std::vector<column_t>& proj_columns);
    void GenerateQueryTableString(const std::string& edge_table_name, const std::string& graph_info_path,
                                  std::shared_ptr<graphar::EdgeInfo> info);
    std::string GetQueryString(const std::vector<column_t>& proj_columns, const std::vector<graphar::IdType>& vids);

    bool const inline isReady() { return !query_table.empty(); }

    void SetQueryFilterType(QueryFilterType new_query_filter_type) { query_filter_type = new_query_filter_type; }
    void SetQueryFilter(std::string& new_query_filter) { query_filter = new_query_filter; }

public:
    QueryFilterType query_filter_type = QueryFilterType::SRC;
    std::string query_filter;

    std::string query_table;
};

class DuckEdgeReader {
public:
    explicit DuckEdgeReader(std::shared_ptr<Connection> conn_) : conn(conn_) {}

    static graphar::Result<std::shared_ptr<DuckEdgeReader>> Make(std::shared_ptr<Connection> conn_,
                                                                 const std::string& edge_table_name,
                                                                 const std::string& graph_info_path,
                                                                 std::shared_ptr<graphar::EdgeInfo> info) {
        auto edge_reader = std::make_shared<DuckEdgeReader>(conn_);
        edge_reader->PrepareQuery(edge_table_name, graph_info_path, info);

        return edge_reader;
    }

    std::string GetTable() { return query_string_constructor.query_table; }

    void PrepareQuery(const std::string& edge_table_name, const std::string& graph_info_path,
                      std::shared_ptr<graphar::EdgeInfo> info) {
        query_string_constructor.GenerateQueryTableString(edge_table_name, graph_info_path, info);
    }

    unique_ptr<QueryResult> ReadEdgesToTable(const std::vector<column_t>& proj_columns,
                                             const std::vector<graphar::IdType>& vids) {
        auto query_string = query_string_constructor.GetQueryString(proj_columns, vids);
        auto query_result = conn->Query(query_string);
        if (query_result->HasError()) {
            throw std::runtime_error(query_result->GetError());
        }
        return query_result;
    }

private:
    std::shared_ptr<duckdb::Connection> conn;
    QueryReadEdgesStringConstructor query_string_constructor;
};

class DuckReadEdgesChunkReader {
public:
    DuckReadEdgesChunkReader(const std::vector<std::shared_ptr<graphar::TSVidsChunkReader>>& init_bases,
                             std::shared_ptr<DuckEdgeReader> init_edge_reader, ClientContext& init_context)
        : context(init_context), bases(std::move(init_bases)), edge_reader(std::move(init_edge_reader)) {}

    static graphar::Result<std::shared_ptr<DuckReadEdgesChunkReader>> Make(
        ClientContext& context, std::shared_ptr<DuckEdgeReader> edge_reader,
        const std::vector<std::shared_ptr<graphar::TSVidsChunkReader>>& init_baseptrs = {}) {
        std::vector<std::shared_ptr<graphar::TSVidsChunkReader>> bases;
        bases = init_baseptrs;
        return std::make_shared<DuckReadEdgesChunkReader>(std::move(bases), edge_reader, context);
    }

    static graphar::Result<std::shared_ptr<DuckReadEdgesChunkReader>> Make(
        ClientContext& context, std::shared_ptr<duckdb::HopBaseGlobalTableFunctionState> gstate_ptr,
        std::shared_ptr<DuckEdgeReader> edge_reader, const std::shared_ptr<graphar::PropertyGroup>& property_group,
        const std::vector<std::shared_ptr<graphar::TSVidsChunkReader>>& init_baseptrs = {}) {
        std::vector<std::shared_ptr<graphar::TSVidsChunkReader>> bases;
        if (init_baseptrs.empty()) {
            GAR_ASSIGN_OR_RAISE(auto init_baseptr, graphar::TSVidsChunkReader::Make(gstate_ptr))
            bases.push_back(std::move(init_baseptr));
        } else {
            bases = init_baseptrs;
        }
        return std::make_shared<DuckReadEdgesChunkReader>(std::move(bases), edge_reader, context);
    }

    void SelectColumns(std::vector<duckdb::column_t> proj_columns_) {
        if (this->cur_result) {
            throw std::runtime_error("Can't select columns after reading started!");
        }
        this->proj_columns = std::move(proj_columns_);
    }

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

    void AcquireVidUnderLock() {
        DUCKDB_GRAPHAR_LOG_TRACE("DuckReadEdgesChunkReader::AcquireVidUnderLock " + std::to_string(next_chunk_idx));
        next_vid = -1;
        vid_acquired = true;
        if (base_idx >= bases.size()) {
            return;
        }
        DUCKDB_GRAPHAR_LOG_TRACE("DuckReadEdgesChunkReader::AcquireVidUnderLock bases size=" +
                                 std::to_string(bases.size()) + " idx=" + std::to_string(base_idx));
        auto gc_result = bases[base_idx]->GetChunk();
        while (gc_result.no_more_chunks) {
            base_idx++;
            if (base_idx >= bases.size()) {
                return;
            }
            gc_result = bases[base_idx]->GetChunk();
        }
        auto maybe_vid = gc_result.chunk;
        if (maybe_vid.has_error()) {
            throw InternalException(maybe_vid.status().message());
        }
        next_vid = maybe_vid.value();
        next_chunk_idx = gc_result.chunk_idx;
        vid_acquired = true;
    }

    void AcquirePathUnderLock() {
        DUCKDB_GRAPHAR_LOG_TRACE("DuckReadEdgesChunkReader::AcquirePathUnderLock");
        AcquireVidUnderLock();  // for ReaderPtr interface
    }

    idx_t GetRowsNum() {
        DUCKDB_GRAPHAR_LOG_TRACE("DuckReadEdgesChunkReader::GetRowsNum");
        DUCKDB_GRAPHAR_LOG_DEBUG("::GRN vid_acquired: " + std::to_string(vid_acquired));
        if (vid_acquired) {
            DUCKDB_GRAPHAR_LOG_DEBUG("::GRN next_vid: " + std::to_string(next_vid));
            if (next_vid != -1) {
                cur_result = edge_reader->ReadEdgesToTable(proj_columns, {next_vid});
                cur_chunk = cur_result->Fetch();
                DUCKDB_GRAPHAR_LOG_DEBUG("::GRN new cur_chunk: " + std::to_string(cur_chunk != nullptr));
                cur_result_idx = next_chunk_idx;
            } else {
                cur_chunk = nullptr;
            }
            vid_acquired = false;
            read_rows = 0;
            cur_read_idx = 0;
        }

        DUCKDB_GRAPHAR_LOG_DEBUG("::GRN cur_chunk: " + std::to_string(cur_chunk != nullptr));
        if (cur_chunk) {
            DUCKDB_GRAPHAR_LOG_DEBUG(
                "::GRN "
                " cc " +
                std::to_string(cur_chunk->size()) + " rr " + std::to_string(read_rows));
        }
        if (cur_chunk && read_rows < cur_chunk->size()) {
            return cur_chunk->size() - read_rows;
        }
        return 0;
    }

    void CopyVidFrom(DuckReadEdgesChunkReader& other) {
        DUCKDB_GRAPHAR_LOG_TRACE("DuckReadEdgesChunkReader::CopyVid");
        next_vid = other.next_vid;
        next_chunk_idx = other.next_chunk_idx;
        vid_acquired = other.vid_acquired;
    }

    graphar::Result<graphar::GetChunkFinalResult> GetChunk(duckdb::idx_t num_rows, duckdb::idx_t read_begin = -1) {
        DUCKDB_GRAPHAR_LOG_TRACE("DuckReadEdgesChunkReader::GetChunk");
        if (GetRowsNum() == 0) {
            throw graphar::Status::IndexError("No more chunks to read!");
        }
        auto read_start = read_begin == -1 ? read_rows : read_begin;

        if (num_rows > cur_chunk->size() - read_start) {
            throw graphar::Status::IndexError("Could read at most " + std::to_string(cur_chunk->size() - read_start) +
                                              " rows, but " + std::to_string(num_rows) + " were requested");
        }
        auto res = duckdb::make_uniq<duckdb::DataChunk>();
        res->Initialize(context, cur_chunk->GetTypes());
        res->Reference(*cur_chunk);
        res->Slice(read_start, num_rows);
        if (read_begin == -1) {
            read_rows += num_rows;
            cur_read_idx++;
        }
        return std::make_pair(std::move(res), GetChunkIdx(cur_result_idx, cur_read_idx));
    }

private:
    std::vector<std::shared_ptr<graphar::TSVidsChunkReader>> bases;
    duckdb::idx_t base_idx = 0;
    std::vector<column_t> proj_columns;
    duckdb::idx_t read_rows = 0;
    duckdb::unique_ptr<duckdb::DataChunk> cur_chunk = nullptr;
    duckdb::idx_t cur_read_idx = 0;
    duckdb::unique_ptr<duckdb::QueryResult> cur_result = nullptr;
    duckdb::idx_t cur_result_idx = 0;
    std::shared_ptr<DuckEdgeReader> edge_reader;
    ClientContext& context;
    graphar::IdType next_vid = -1;
    duckdb::idx_t next_chunk_idx = 0;
    bool vid_acquired = false;
};

template <typename T>
concept isDuckReadEdgesChunkReader = std::is_same_v<T, DuckReadEdgesChunkReader>;

}  // namespace duckdb

namespace graphar {

using DuckReadEdgesChunkReader = duckdb::DuckReadEdgesChunkReader;

}  // namespace graphar