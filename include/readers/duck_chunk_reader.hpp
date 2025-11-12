#pragma once

#include <duckdb.hpp>

#include <graphar/fwd.h>
#include <graphar/types.h>
#include <graphar/chunk_info_reader.h>
#include <graphar/result.h>
#include <graphar/reader_util.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {
// struct DuckFilterOptions {
//     std::pair<int64_t, int64_t> filter_range;
//     std::vector<duckdb::column_t> proj_columns;
// };

class QueryStringConstructor {
public:
    enum class QueryType { FIRST, MIDDLE, LAST, SINGLE };
    std::string GetMainQueryString(const std::vector<column_t>& proj_columns, std::pair<int64_t, int64_t> range = {-1, -1});

    std::string GetGrapharOffsetQueryString();

    void SetFileType(graphar::FileType file_type_) { file_type = file_type_; }

private:
    static constexpr std::string_view SQL_SELECT_CLAUSE = "SELECT";
    static constexpr std::string_view SQL_FROM_CLAUSE = "FROM";
    static constexpr std::string_view SQL_WHERE_CLAUSE = "WHERE";
    static constexpr std::string_view SQL_BETWEEN_CLAUSE = "BETWEEN";
    static constexpr std::string_view SQL_LIMIT_CLAUSE = "LIMIT";
    static constexpr std::string_view SQL_OFFSET_CLAUSE = "OFFSET";
    static constexpr std::string_view READ_PARQUET_FUNCTION = "read_parquet";
    static constexpr std::string_view FILE_ROW_NUMBER_CLAUSE = "file_row_number";

private:
    graphar::FileType file_type = graphar::FileType::PARQUET;
};

class DuckParquetFileReader {
public:
    DuckParquetFileReader(std::shared_ptr<duckdb::Connection> conn_) : conn(conn_) {
    }
    unique_ptr<QueryResult> ReadFileToTable(const std::string& path, const std::vector<duckdb::column_t>& proj_columns, std::pair<int64_t, int64_t> range = {-1, -1}) {
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

template<typename BaseArrowChunkReader>
class BaseDuckChunkReader {
public:
    BaseDuckChunkReader(std::shared_ptr<BaseArrowChunkReader> base_, std::shared_ptr<DuckParquetFileReader> file_reader_, ClientContext& context_, idx_t chunk_size_ = -1) :
    base(std::move(base_)), file_reader(std::move(file_reader_)), context(context_), chunk_size(chunk_size_) {
    }

    idx_t EnsureNotRead() {
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

    idx_t chunk_size = -1;

    std::shared_ptr<DuckParquetFileReader> file_reader;
    ClientContext& context;
};

class DuckVertexChunkReader : public BaseDuckChunkReader<graphar::VertexPropertyChunkInfoReader> {
public:
    template<typename... Args>
    explicit DuckVertexChunkReader(Args&&... args)
        : BaseDuckChunkReader<graphar::VertexPropertyChunkInfoReader>(std::forward<Args>(args)...) {
    }

    template<typename... Args>
    static graphar::Result<std::shared_ptr<DuckVertexChunkReader>> Make(ClientContext& context, std::shared_ptr<DuckParquetFileReader> file_reader, idx_t chunk_size,
      Args&&... args) {
        GAR_ASSIGN_OR_RAISE(auto base_ptr, graphar::VertexPropertyChunkInfoReader::Make(std::forward<Args>(args)...));
        return std::make_shared<DuckVertexChunkReader>(std::move(base_ptr), file_reader, context, chunk_size);
    }

    void FilterByRange(std::pair<int64_t, int64_t> vid_range, const std::string& filter_column) {
        if (cur_result) {
            throw std::runtime_error("Can't filter after reading started!");
        }
        if (chunk_size == -1) {
            throw std::runtime_error("Can't filter before setting chunk size!");
        }
        GAR_RAISE_ERROR_NOT_OK(base->seek(vid_range.first));
        offset_rows = vid_range.first % chunk_size;
        rows_to_read = vid_range.second - vid_range.first + 1;
        single_chunk = (vid_range.first / chunk_size == vid_range.second / chunk_size);
    }
};

template<typename BaseArrowChunkReader>
class DuckEdgeChunkReader : public BaseDuckChunkReader<BaseArrowChunkReader> {
public:
    // using BaseDuckChunkReader<BaseArrowChunkReader>::cur_result;
    // using BaseDuckChunkReader<BaseArrowChunkReader>::chunk_size;
    // using BaseDuckChunkReader<BaseArrowChunkReader>::base;

    DuckEdgeChunkReader(std::shared_ptr<BaseArrowChunkReader> base_, std::shared_ptr<DuckParquetFileReader> file_reader_, ClientContext& context_, idx_t chunk_size_,
    const std::shared_ptr<graphar::EdgeInfo>& edge_info_, graphar::AdjListType adj_list_type_, const std::string& prefix_) :
    BaseDuckChunkReader<BaseArrowChunkReader>(std::move(base_), std::move(file_reader_), context_, chunk_size_),
    edge_info(edge_info_), adj_list_type(adj_list_type_), prefix(prefix_) {
    }

    static graphar::Result<std::shared_ptr<DuckEdgeChunkReader>> Make(ClientContext& context, std::shared_ptr<DuckParquetFileReader> file_reader, idx_t chunk_size,
      const std::shared_ptr<graphar::EdgeInfo>& edge_info, graphar::AdjListType adj_list_type,
      const std::string& prefix) {
        GAR_ASSIGN_OR_RAISE(auto base_ptr, BaseArrowChunkReader::Make(edge_info, adj_list_type, prefix));
        return std::make_shared<DuckEdgeChunkReader>(std::move(base_ptr), file_reader, context, chunk_size, edge_info, adj_list_type, prefix);
    }

    static graphar::Result<std::shared_ptr<DuckEdgeChunkReader>> Make(ClientContext& context, std::shared_ptr<DuckParquetFileReader> file_reader, idx_t chunk_size,
      const std::shared_ptr<graphar::EdgeInfo>& edge_info, const std::shared_ptr<graphar::PropertyGroup>& property_group, graphar::AdjListType adj_list_type,
      const std::string& prefix) {
        GAR_ASSIGN_OR_RAISE(auto base_ptr, BaseArrowChunkReader::Make(edge_info, property_group, adj_list_type, prefix));
        return std::make_shared<DuckEdgeChunkReader>(std::move(base_ptr), file_reader, context, chunk_size, edge_info, adj_list_type, prefix);
    }

    void FilterByRange(std::pair<int64_t, int64_t> vid_range, const std::string& filter_column) {
        if (this->cur_result) {
            throw std::runtime_error("Can't filter after reading started!");
        }
        if (this->chunk_size == -1) {
            throw std::runtime_error("Can't filter before setting chunk size!");
        }
        if (vid_range.first != vid_range.second) {
            throw NotImplementedException("FilterByRange not implemented for vid range");
        }
        if (adj_list_type == graphar::AdjListType::ordered_by_source) {
            this->base->seek_src(vid_range.first);
        } else {
            this->base->seek_dst(vid_range.first);
        }
        GAR_ASSIGN_OR_RAISE_ERROR(auto offset_pair, graphar::util::GetAdjListOffsetOfVertex(edge_info, prefix, adj_list_type, vid_range.first));
        this->offset_rows = offset_pair.first % this->chunk_size;
        this->rows_to_read = offset_pair.second - offset_pair.first;
        this->single_chunk = true;
        // seek_src / seek_dst
        // calculate total number of rows
        // save offset of first vertex
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

} // namespace duckdb

namespace graphar {

using DuckAdjListChunkReader = duckdb::DuckEdgeChunkReader<AdjListChunkInfoReader>;
using DuckAdjListPropertyChunkReader = duckdb::DuckEdgeChunkReader<AdjListPropertyChunkInfoReader>;
using DuckVertexPropertyChunkReader = duckdb::DuckVertexChunkReader;

} // namespace graphar