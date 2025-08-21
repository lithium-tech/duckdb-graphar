#pragma once

#include "functions/table/read_base.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow/arrow_duck_schema.hpp>
#include <duckdb/function/table_function.hpp>

#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/graph_info.h>

#include <cassert>

namespace duckdb {
class ReadVertices : public ReadBase<ReadVertices> {
public:
    static void SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info, const graphar::VertexInfo& vertex_info,
                            unique_ptr<ReadBindData>& bind_data);
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);

    static std::shared_ptr<Reader> GetReader(const ReadBaseGlobalTableFunctionState& gstate,
                                             const ReadBindData& bind_data, idx_t ind, const std::string& filter_value,
                                             const std::string& filter_column, const std::string& filter_type);

    static TableFunction GetFunction();

    static TableFunction GetScanFunction();

    static void SetFilter(ReadBaseGlobalTableFunctionState& gstate, const ReadBindData& bind_data,
                          const std::string& filter_value, const std::string& filter_column,
                          const std::string& filter_type);
};
}  // namespace duckdb
