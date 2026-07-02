#pragma once

#include "functions/table/read_base.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow/arrow_duck_schema.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/graph_info.h>

#include <cassert>
#include <cxxabi.h>

namespace duckdb {

class ReadHopBindData : public ReadBindData {
private:
    std::vector<int64_t> vids;

    friend class ReadHop;
};

class ReadHopGlobalTableFunctionState : public ReadBaseGlobalTableFunctionState {
public:
    static std::string demangle(const char* name) {
        int status = -4;
        std::unique_ptr<char, void (*)(void*)> res{abi::__cxa_demangle(name, NULL, NULL, &status), std::free};
        return (status == 0) ? res.get() : name;
    }
    size_t MoveBaseReaders(size_t state_ind) {
        if (cur_ind == state_ind) {
            cur_ind++;

            if (cur_ind >= vertexes.size()) {
                return cur_ind;
            }

            if (cur_ind == next_hop_ind) {
                storage_state = false;
            }

            const auto prefix = graph_info->GetPrefix();
            auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&type_info);

            DUCKDB_GRAPHAR_LOG_DEBUG("Before move readers");
            for (size_t i = 0; i < base_readers.size(); ++i) {
                if (global_projected_inds[i].empty()) {
                    continue;
                }

                auto& base_reader = base_readers[i];
                FilterByRangeEdge(base_reader, {vertexes[cur_ind], vertexes[cur_ind] + 1}, SRC_GID_COLUMN, edge_info, prefix);
                std::visit(
                    [&](const auto& ptr) {
                        DUCKDB_GRAPHAR_LOG_DEBUG("Base reader " + demangle(typeid(ptr).name()) + " for vertex " +
                                                 std::to_string(vertexes[cur_ind]));
                    },
                    base_reader);

                PrintFilterInfo(base_reader);
            }
        }
        return cur_ind;
    }

    column_t dstColumn = -1;

private:
    std::vector<graphar::IdType> vertexes;
    std::unordered_set<graphar::IdType> _vertexes;
    size_t cur_ind;
    size_t next_hop_ind;

    std::mutex mtx;
    bool storage_state = true;

    friend class ReadHop;
};

class ReadHopLocalTableFunctionState : public ReadBaseLocalTableFunctionState {
private:
    bool storage_state = true;
    size_t cur_ind;
    friend class ReadHop;
};

class ReadHop : public ReadBase<ReadHop> {
public:
    static void SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info,
                            std::shared_ptr<graphar::EdgeInfo> edge_info, unique_ptr<ReadHopBindData>& bind_data);
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);

    static BaseReaderPtr GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                       const std::string& filter_column);
    static void SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                          const std::pair<int64_t, int64_t>& vid_range, const std::string& filter_column);
    static ReaderPtr GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                               ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column);

    static unique_ptr<BaseStatistics> GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                    column_t column_index);

    static void PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                      vector<unique_ptr<Expression>>& filters);

    static TableFunction GetFunction();
    static TableFunction GetScanFunction();
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);
    static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                         GlobalTableFunctionState* gstate_ptr);

private:
    static std::string demangle(const char* name) {
        int status = -4;
        std::unique_ptr<char, void (*)(void*)> res{abi::__cxa_demangle(name, NULL, NULL, &status), std::free};
        return (status == 0) ? res.get() : name;
    }
};
}  // namespace duckdb
