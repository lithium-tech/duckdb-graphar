#pragma once

#include "readers/adj_reader.hpp"
#include "readers/low_edge_reader.hpp"
#include "utils/func.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include <graphar/api/high_level_reader.h>

namespace duckdb {

//-------------------------------------------------------------------
// BindData
//-------------------------------------------------------------------
class TwoHopBindData : public TableFunctionData {
public:
    TwoHopBindData(std::shared_ptr<graphar::EdgeInfo> edge_info, std::string prefix, graphar::IdType src_id)
        : edge_info(edge_info), prefix(prefix), src_id(src_id) {};

    const std::shared_ptr<graphar::EdgeInfo>& GetEdgeInfo() const { return edge_info; }
    const std::string& GetPrefix() const { return prefix; }
    graphar::IdType GetSrcId() const { return src_id; }
    graphar::IdType GetVid() const { return GetSrcId(); }

private:
    std::shared_ptr<graphar::EdgeInfo> edge_info;
    std::string prefix;
    graphar::IdType src_id;
};

//-------------------------------------------------------------------
// GlobalState
//-------------------------------------------------------------------
struct TwoHopGlobalState {
public:
    TwoHopGlobalState(ClientContext& context, const TwoHopBindData& bind_data) { hop_i = hop_ids.begin(); };

    void init_iter() { hop_i = hop_ids.begin(); }

public:
    std::set<std::int64_t> hop_ids;
    bool one_hop = true;
    std::unique_ptr<LowEdgeReaderByVertex> reader;
    std::shared_ptr<OffsetReader> offset_reader;
    std::set<std::int64_t>::const_iterator hop_i;
};

struct TwoHopGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    TwoHopGlobalTableFunctionState(ClientContext& context, const TwoHopBindData& bind_data)
        : state(context, bind_data) {};

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);

    TwoHopGlobalState& GetState() { return state; }

private:
    TwoHopGlobalState state;
};

struct OneMoreHopGlobalState {
public:
    OneMoreHopGlobalState(ClientContext& context, const TwoHopBindData& bind_data)
        : src_reader(MyAdjReaderOrdSrc(bind_data.GetEdgeInfo(), bind_data.GetPrefix())) {
        src_reader.find_src(bind_data.GetSrcId());
        hop_ids.reserve(src_reader.size());
        hop_i = hop_ids.begin();
    };

public:
    std::unordered_set<std::int64_t> hop_ids;
    bool one_hop = true;
    MyAdjReaderOrdSrc src_reader;
    std::unordered_set<std::int64_t>::const_iterator hop_i;
};

struct OneMoreHopGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    OneMoreHopGlobalTableFunctionState(ClientContext& context, const TwoHopBindData& bind_data)
        : state(context, bind_data) {};

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);

public:
    OneMoreHopGlobalState state;
};

//-------------------------------------------------------------------
// Function
//-------------------------------------------------------------------
struct TwoHop {
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static void Register(ExtensionLoader& loader);
    static TableFunction GetFunction();
};

struct OneMoreHop {
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static void Register(ExtensionLoader& loader);
    static TableFunction GetFunction();
};
}  // namespace duckdb
