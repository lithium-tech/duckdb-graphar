#pragma once

#include <graphar/chunk_info_reader.h>
#include <graphar/fwd.h>
#include <graphar/graph_info.h>
#include <graphar/reader_util.h>
#include <graphar/result.h>
#include <graphar/types.h>

#include <duckdb.hpp>
#include <memory>
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
    DuckParquetFileReader(std::shared_ptr<duckdb::Connection> conn_) : conn(conn_) {}
    unique_ptr<QueryResult> ReadFileToTable(const std::string& path, const std::vector<duckdb::column_t>& proj_columns,
                                            std::pair<int64_t, int64_t> range = {-1, -1}) {
        auto query_string = query_string_constructor.GetMainQueryString(proj_columns, range);
        auto query_result = conn->Query(query_string, Value(path));
        if (query_result->HasError()) {
            throw std::runtime_error(query_result->GetError());
        }
        return std::move(query_result);
    }

private:
    std::shared_ptr<duckdb::Connection> conn;
    QueryStringConstructor query_string_constructor;
};

template <typename BaseArrowChunkReader>
class BaseDuckChunkReader {
public:
    BaseDuckChunkReader(ClientContext& init_context, std::shared_ptr<BaseArrowChunkReader> init_base,
                        std::shared_ptr<DuckParquetFileReader> init_file_reader)
        : context(init_context), base(std::move(init_base)), file_reader(std::move(init_file_reader)) {}

    idx_t ReserveRowsToRead() {
        if (rows_to_read == 0) {
            return 0;
        }
        if (cur_chunk && read_rows < cur_chunk->size()) {
            return cur_chunk->size() - read_rows;
        }
        read_rows = 0;
        if (cur_result && (cur_chunk = cur_result->Fetch())) {
            return cur_chunk->size();
        }
        if (cur_result) {
            if (!base->next_chunk().ok()) {
                return 0;
            }
        }
        std::pair<int64_t, int64_t> range = {-1, -1};
        if (offset_rows != -1) {
            // was filtered
            if (single_chunk) {
                range = {offset_rows, offset_rows + rows_to_read - 1};
            } else if (!cur_result) {
                range.first = offset_rows;
            } else if (rows_to_read < chunk_size) {
                range.second = rows_to_read - 1;
            }
        }
        GAR_ASSIGN_OR_RAISE_ERROR(auto path, base->GetChunk());
        cur_result = file_reader->ReadFileToTable(path, proj_columns, range);
        cur_chunk = cur_result->Fetch();
        return cur_chunk->size();
    }

    graphar::Result<duckdb::unique_ptr<duckdb::DataChunk>> GetChunk(duckdb::idx_t num_rows) {
        if (ReserveRowsToRead() == 0) {
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
        if (rows_to_read != -1) {
            rows_to_read -= num_rows;
        }
        return std::move(res);
    }

    void SelectColumns(std::vector<duckdb::column_t>& proj_columns_) {
        if (cur_result) {
            throw std::runtime_error("Can't select columns after reading started!");
        }
        proj_columns = std::move(proj_columns_);
    }

protected:
    std::shared_ptr<BaseArrowChunkReader> base;
    std::vector<duckdb::column_t> proj_columns;
    duckdb::idx_t read_rows = 0;
    duckdb::unique_ptr<duckdb::DataChunk> cur_chunk = nullptr;
    duckdb::unique_ptr<duckdb::QueryResult> cur_result = nullptr;

    duckdb::idx_t offset_rows = -1;
    duckdb::idx_t rows_to_read = -1;
    bool single_chunk = false;

    idx_t chunk_size = 0;

    std::shared_ptr<DuckParquetFileReader> file_reader;
    ClientContext& context;
};

class DuckVertexChunkReader : public BaseDuckChunkReader<graphar::VertexPropertyChunkInfoReader> {
public:
    template <typename... Args>
    explicit DuckVertexChunkReader(const std::shared_ptr<graphar::VertexInfo>& init_vertex_info, Args&&... args)
        : BaseDuckChunkReader<graphar::VertexPropertyChunkInfoReader>(std::forward<Args>(args)...),
          vertex_info(init_vertex_info) {
        this->chunk_size = vertex_info->GetChunkSize();
    }

    template <typename... Args>
    static graphar::Result<std::shared_ptr<DuckVertexChunkReader>> Make(
        ClientContext& context, std::shared_ptr<DuckParquetFileReader> file_reader,
        const std::shared_ptr<graphar::VertexInfo>& vertex_info,
        const std::shared_ptr<graphar::PropertyGroup>& property_group, const std::string& prefix) {
        GAR_ASSIGN_OR_RAISE(auto init_baseptr,
                            graphar::VertexPropertyChunkInfoReader::Make(vertex_info, property_group, prefix));
        return std::make_shared<DuckVertexChunkReader>(vertex_info, context, std::move(init_baseptr), file_reader);
    }

    void FilterByRange(const std::pair<int64_t, int64_t>& vid_range, const std::string& filter_column) {
        if (cur_result) {
            throw std::runtime_error("Can't filter the data after the start of reading");
        }
        GAR_RAISE_ERROR_NOT_OK(base->seek(vid_range.first));
        offset_rows = vid_range.first % this->chunk_size;
        rows_to_read = vid_range.second - vid_range.first + 1;
        single_chunk = (vid_range.first / this->chunk_size == vid_range.second / this->chunk_size);
    }

private:
    std::shared_ptr<graphar::VertexInfo> vertex_info;
};

template <typename BaseArrowChunkReader>
class DuckEdgeChunkReader : public BaseDuckChunkReader<BaseArrowChunkReader> {
public:
    DuckEdgeChunkReader(std::shared_ptr<BaseArrowChunkReader> init_base,
                        std::shared_ptr<DuckParquetFileReader> init_file_reader, ClientContext& init_context,
                        const std::shared_ptr<graphar::EdgeInfo>& edge_info_, graphar::AdjListType adj_list_type_,
                        const std::string& prefix_)
        : BaseDuckChunkReader<BaseArrowChunkReader>(init_context, std::move(init_base), std::move(init_file_reader)),
          edge_info(edge_info_),
          adj_list_type(adj_list_type_),
          prefix(prefix_) {
        this->chunk_size = edge_info_->GetChunkSize();
    }

    static graphar::Result<std::shared_ptr<DuckEdgeChunkReader>> Make(
        ClientContext& context, std::shared_ptr<DuckParquetFileReader> file_reader,
        const std::shared_ptr<graphar::EdgeInfo>& edge_info, graphar::AdjListType adj_list_type,
        const std::string& prefix) {
        GAR_ASSIGN_OR_RAISE(auto init_baseptr, BaseArrowChunkReader::Make(edge_info, adj_list_type, prefix));
        return std::make_shared<DuckEdgeChunkReader>(std::move(init_baseptr), file_reader, context, edge_info,
                                                     adj_list_type, prefix);
    }

    static graphar::Result<std::shared_ptr<DuckEdgeChunkReader>> Make(
        ClientContext& context, std::shared_ptr<DuckParquetFileReader> file_reader,
        const std::shared_ptr<graphar::EdgeInfo>& edge_info,
        const std::shared_ptr<graphar::PropertyGroup>& property_group, graphar::AdjListType adj_list_type,
        const std::string& prefix) {
        GAR_ASSIGN_OR_RAISE(auto init_baseptr,
                            BaseArrowChunkReader::Make(edge_info, property_group, adj_list_type, prefix));
        return std::make_shared<DuckEdgeChunkReader>(std::move(init_baseptr), file_reader, context, edge_info,
                                                     adj_list_type, prefix);
    }

    void FilterByRange(const std::pair<int64_t, int64_t>& vid_range, const std::string& filter_column) {
        if (this->cur_result) {
            throw std::runtime_error("Can't filter after reading started!");
        }
        if (vid_range.first != vid_range.second) {
            throw NotImplementedException("FilterByRange not implemented for vid range");
        }
        if (adj_list_type == graphar::AdjListType::ordered_by_source) {
            this->base->seek_src(vid_range.first);
        } else {
            this->base->seek_dst(vid_range.first);
        }
        GAR_ASSIGN_OR_RAISE_ERROR(auto offset_pair, graphar::util::GetAdjListOffsetOfVertex(
                                                        edge_info, prefix, adj_list_type, vid_range.first));
        this->offset_rows = offset_pair.first % this->chunk_size;
        this->rows_to_read = offset_pair.second - offset_pair.first;
        this->single_chunk = (offset_pair.first / this->chunk_size == offset_pair.second / this->chunk_size);
    }

    void SelectColumns(std::vector<duckdb::column_t>& proj_columns_) {
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

using DuckAdjListChunkReader = duckdb::DuckEdgeChunkReader<AdjListChunkInfoReader>;
using DuckAdjListPropertyChunkReader = duckdb::DuckEdgeChunkReader<AdjListPropertyChunkInfoReader>;
using DuckVertexPropertyChunkReader = duckdb::DuckVertexChunkReader;

}  // namespace graphar