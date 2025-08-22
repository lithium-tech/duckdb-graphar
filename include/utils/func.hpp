#pragma once

#include <arrow/api.h>
#include <arrow/scalar.h>

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table/arrow/arrow_type_info.hpp>
#include <duckdb/function/table/arrow/enum/arrow_type_info_type.hpp>

#include <graphar/api/arrow_reader.h>
#include <graphar/reader_util.h>
#include <graphar/types.h>

#include <iostream>
#include <math.h>

namespace duckdb {

const std::string GID_COLUMN = "grapharId";
const std::string GID_COLUMN_INTERNAL = "_graphArVertexIndex";
const std::string SRC_GID_COLUMN = "_graphArSrcIndex";
const std::string DST_GID_COLUMN = "_graphArDstIndex";

struct GraphArFunctions {
    static LogicalTypeId graphArT2duckT(const std::string& name);

    static std::shared_ptr<arrow::DataType> graphArT2arrowT(const std::string& name);

    static unique_ptr<ArrowTypeInfo> graphArT2ArrowTypeInfo(const std::string& name);

    template <typename Info>
    static std::string GetNameFromInfo(const std::shared_ptr<Info>& info);

    template <typename GraphArIter>
    static void setByIter(DataChunk& output, GraphArIter& iter, const int prop_i, const int row_i,
                          const std::string& prop_name, const std::string& prop_type) {
        if (prop_type == "bool") {
            output.SetValue(prop_i, row_i, iter.template property<bool>(prop_name).value());
        } else if (prop_type == "int32") {
            output.SetValue(prop_i, row_i, iter.template property<int32_t>(prop_name).value());
        } else if (prop_type == "int64") {
            output.SetValue(prop_i, row_i, iter.template property<int64_t>(prop_name).value());
        } else if (prop_type == "float") {
            output.SetValue(prop_i, row_i, iter.template property<float_t>(prop_name).value());
        } else if (prop_type == "double") {
            output.SetValue(prop_i, row_i, iter.template property<double_t>(prop_name).value());
        } else if (prop_type == "string") {
            output.SetValue(prop_i, row_i, iter.template property<std::string>(prop_name).value());
        } else {
            throw NotImplementedException("Unsupported type");
        }
    };

    static graphar::Result<std::shared_ptr<arrow::Schema>> NamesAndTypesToArrowSchema(const vector<std::string>& names,
                                                                                      const vector<std::string>& types);

    static std::shared_ptr<arrow::Table> EmptyTableFromNamesAndTypes(const vector<std::string>& names,
                                                                     const vector<std::string>& types);

    static std::shared_ptr<graphar::Expression> GetFilter(const std::string& filter_type,
                                                          const std::string& filter_value,
                                                          const std::string& filter_column);
};

inline std::pair<int64_t, int64_t> GetChunkAndOffset(graphar::IdType chunk_size, graphar::IdType offset) {
    int64_t chunk_num = offset / chunk_size;
    int64_t offset_in_chunk = offset % chunk_size;
    return std::make_pair(chunk_num, offset_in_chunk);
}

static void release_children_only(struct ArrowArray* array) {
    if (array == nullptr) return;

    if (array->children != nullptr) {
        free(array->children);
        array->children = nullptr;
    }

    array->release = nullptr;
}

template <typename Array>
static int64_t GetInt64Value(std::shared_ptr<Array> array, int64_t index) {
    return std::static_pointer_cast<arrow::Int64Scalar>(array->GetScalar(index).ValueOrDie())->value;
}

class MyAdjReaderOrdSrc {
public:
    MyAdjReaderOrdSrc(const std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& prefix)
        : edge_info_(edge_info),
          prefix_(prefix),
          reader_(graphar::AdjListArrowChunkReader::Make(edge_info, graphar::AdjListType::ordered_by_source, prefix)
                      .value()) {}
    void find_src(graphar::IdType src) {
        reader_->seek_src(src);
        std::pair<graphar::IdType, graphar::IdType> range =
            (graphar::util::GetAdjListOffsetOfVertex(edge_info_, prefix_, graphar::AdjListType::ordered_by_source, src)
                 .value());
        auto chunk_size = edge_info_->GetChunkSize();
        size_ = range.second - range.first;
        start_ = GetChunkAndOffset(chunk_size, range.first);
        end_ = GetChunkAndOffset(chunk_size, range.second);
        if (start_.first == end_.first) {
            if (start_.second == end_.second) {
                chunk_count_ = 0;
            } else {
                chunk_count_ = 1;
            }
        } else {
            chunk_count_ = end_.first - start_.first;
            if (end_.second != 0) {
                chunk_count_++;
            }
        }
        is_empty_ = true;
    }

    bool has_next_table() { return chunk_i_ + 1 < chunk_count_; }

    bool finish_table() const { return chunk_i_ >= chunk_count_; }

    std::shared_ptr<arrow::Table> get_table() {
        auto table = reader_->GetChunk().value();

        if (chunk_i_ == chunk_count_ - 1 && end_.second != 0) {
            if (start_.first == end_.first) {
                return table->Slice(0, end_.second - start_.second);
            }
            return table->Slice(0, end_.second);
        }
        return table;
    }

    void next_table() {
        reader_->next_chunk();
        chunk_i_++;
    }

    std::shared_ptr<arrow::Table> get(int64_t count) {
        if (is_empty_) {
            table_ = get_table();
            offset_ = 0;
            is_empty_ = false;
        }

        auto result = table_->Slice(offset_, count);
        offset_ += count;
        if (offset_ >= table_->num_rows()) {
            if (has_next_table()) {
                next_table();
                offset_ = 0;
            }
        }
        return result;
    }

    bool finish() {
        if (is_empty_) {
            if (finish_table()) {
                return true;
            }
            table_ = get_table();
            offset_ = 0;
            is_empty_ = false;
        }
        if (offset_ >= table_->num_rows()) {
            return true;
        }
        return false;
    }

    int64_t size() { return size_; }

private:
    const std::shared_ptr<graphar::EdgeInfo> edge_info_;
    const std::string prefix_;
    int64_t size_ = 0;
    std::shared_ptr<graphar::AdjListArrowChunkReader> reader_;
    std::pair<int64_t, int64_t> start_;
    std::pair<int64_t, int64_t> end_;
    int64_t chunk_count_ = 0, chunk_i_ = 0;
    int64_t offset_ = 0;
    std::shared_ptr<arrow::Table> table_;
    bool is_empty_ = false;
};

inline void PrintArrowTable(const std::shared_ptr<arrow::Table>& table, int64_t limit = 0) {
    int64_t num_rows = table->num_rows();
    int num_columns = table->num_columns();

    for (int col = 0; col < num_columns; ++col) {
        std::cout << table->field(col)->name();
        if (col < num_columns - 1) std::cout << "\t";
    }
    std::cout << std::endl;
    if (limit > 0) {
        num_rows = std::min(num_rows, limit);
    }

    for (int64_t row = 0; row < num_rows; ++row) {
        for (int col = 0; col < num_columns; ++col) {
            const std::shared_ptr<arrow::ChunkedArray>& chunked_array = table->column(col);
            int64_t local_row = row;
            for (const auto& chunk : chunked_array->chunks()) {
                if (local_row < chunk->length()) {
                    auto scalar_result = chunk->GetScalar(local_row);
                    if (scalar_result.ok()) {
                        std::shared_ptr<arrow::Scalar> scalar = scalar_result.ValueOrDie();
                        std::cout << scalar->ToString();
                    } else {
                        std::cout << "[error]";
                    }
                    break;
                } else {
                    local_row -= chunk->length();
                }
            }
            if (col < num_columns - 1) std::cout << "\t";
        }
        std::cout << std::endl;
    }
}

std::string GetYamlContent(const std::string& path);
std::string GetDirectory(const std::string& path);
std::int64_t GetCount(const std::string& path);
std::int64_t GetVertexCount(const std::shared_ptr<graphar::EdgeInfo>& edge_info, const std::string& directory);
}  // namespace duckdb
