#pragma once

#include <cassert>

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "functions/table/read_base.hpp"
#include "graphar/api/high_level_reader.h"
#include "graphar/arrow/chunk_reader.h"
#include "graphar/graph_info.h"

namespace duckdb {
class ReadEdges : public ReadBase<ReadEdges> {
   public:
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);

    static std::shared_ptr<Reader> GetReader(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                                             idx_t ind, const std::string& filter_value,
                                             const std::string& filter_column, const std::string& filter_type);

    static TableFunction GetFunction();

    static void SetFilter(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data, std::string& filter_value,
                          std::string& filter_column, std::string& filter_type);
};
}  // namespace duckdb