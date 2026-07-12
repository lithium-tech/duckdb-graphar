// #pragma once

// #include "functions/table/read_base.hpp"
// #include "functions/table/read_hop_filtered.hpp"

// #include <duckdb/common/named_parameter_map.hpp>
// #include <duckdb/function/table/arrow/arrow_duck_schema.hpp>
// #include <duckdb/function/table_function.hpp>
// #include <duckdb/main/extension/extension_loader.hpp>

// #include <graphar/api/high_level_reader.h>
// #include <graphar/arrow/chunk_reader.h>
// #include <graphar/graph_info.h>

// #include <cassert>
// #include <cxxabi.h>

// namespace duckdb {

// class ReadTwoHopBindData : public ReadHopFilteredBindData {
// public:
//     std::string query_filter;
// };

// class ReadTwoHopGlobalTableFunctionState : public ReadBaseGlobalTableFunctionState {
// public:
//     void GenerateQuery(const ReadTwoHopBindData &bind_data) {
//         if (query_filter.empty()) {
//             // query_string = "SELECT _graphArSrcIndex, _graphArDstIndex FROM read_hop('" + graph_info_path + "', src='" +
//             //                     edge_info->GetSrcType() + "', type='" + edge_info->GetEdgeType() + "', dst='" +
//             //                     edge_info->GetDstType() + "') WHERE _graphArSrcIndex = $1";
//             query_string = "SELECT _graphArSrcIndex, _graphArDstIndex FROM read_hop_filtered('" + graph_info_path + "', src='" +
//                                 edge_info->GetSrcType() + "', type='" + edge_info->GetEdgeType() + "', dst='" +
//                                 edge_info->GetDstType() + "', vids=" + vids_str + ")";
//         } else {
//             query_string = "SELECT _graphArSrcIndex, _graphArDstIndex FROM read_hop_filtered('" + graph_info_path + "', src='" +
//                                 edge_info->GetSrcType() + "', type='" + edge_info->GetEdgeType() + "', dst='" +
//                                 edge_info->GetDstType() + "', vids=" + vids_str + ") WHERE " + query_filter;
//         }
//         DUCKDB_GRAPHAR_LOG_DEBUG(query_string);
//     }

// private:
//     std::vector<graphar::IdType> vertexes;
//     std::unordered_set<graphar::IdType> _vertexes;

//     std::mutex mtx;
//     std::string query_string;
//     std::string query_filter;

//     friend class ReadTwoHop;
// };

// class ReadTwoHopLocalTableFunctionState : public ReadBaseLocalTableFunctionState {
// };

// class ReadTwoHop : public ReadBase<ReadTwoHop> {
// public:
//     static void SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info,
//                             std::shared_ptr<graphar::EdgeInfo> edge_info,
//                             unique_ptr<ReadTwoHopBindData>& bind_data);
//     static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
//                                          vector<LogicalType>& return_types, vector<string>& names);

//     static BaseReaderPtr GetBaseReader(ClientContext& context, ReadTwoHopGlobalTableFunctionState& gstate);
//     static void SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
//                           const std::pair<int64_t, int64_t>& vid_range, const std::string& filter_column);
//     static ReaderPtr GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
//                                ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column);

//     static unique_ptr<BaseStatistics> GetStatistics(ClientContext& context, const FunctionData* bind_data,
//                                                     column_t column_index);

//     static void PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
//                                       vector<unique_ptr<Expression>>& filters);

//     static TableFunction GetFunction();
//     static TableFunction GetScanFunction();
//     static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
//     static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);
//     static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
//                                                          GlobalTableFunctionState* gstate_ptr);
// };
// }  // namespace duckdb
