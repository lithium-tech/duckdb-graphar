#pragma once

#include "readers/offset_reader.hpp"
#include "utils/global_log_manager.hpp"

#include <arrow/api.h>
#include <arrow/scalar.h>

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/table/arrow.hpp>
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
const std::vector<graphar::AdjListType> all_adj_list_types = {
    graphar::AdjListType::ordered_by_source, graphar::AdjListType::ordered_by_dest,
    graphar::AdjListType::unordered_by_source, graphar::AdjListType::unordered_by_dest};

struct GraphArFunctions {
    static LogicalTypeId graphArT2duckT(const std::string& name);

    static std::shared_ptr<arrow::DataType> graphArT2arrowT(const std::string& name);

    static unique_ptr<ArrowTypeInfo> graphArT2ArrowTypeInfo(const std::string& name);

    static Value ArrowScalar2DuckValue(const std::shared_ptr<arrow::Scalar>& scalar);

    template <typename Info>
    static std::string GetNameFromInfo(const Info& info);

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

template <typename Array>
static int64_t GetInt64Value(std::shared_ptr<Array> array, int64_t index) {
    return std::static_pointer_cast<arrow::Int64Scalar>(array->GetScalar(index).ValueOrDie())->value;
}

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

void ConvertArrowTableToDataChunk(const arrow::Table& table, DataChunk& output, const std::vector<column_t>& column_ids,
                                  ClientContext& context);
}  // namespace duckdb
