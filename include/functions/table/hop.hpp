#pragma once

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/function/table_function.hpp"
#include "graphar/api/high_level_reader.h"
#include "graphar/graph_info.h"
#include "utils/func.hpp"

namespace duckdb {

class TwoHopBindData final : public TableFunctionData {
public:
    TwoHopBindData(std::shared_ptr<graphar::EdgeInfo> edge_info, std::string prefix, graphar::IdType src_id)
        : edge_info(edge_info), prefix(prefix), src_id(src_id){};

    const std::shared_ptr<graphar::EdgeInfo>& GetEdgeInfo() const { return edge_info; }
    const std::string& GetPrefix() const { return prefix; }
    graphar::IdType GetSrcId() const { return src_id; }

private:
    std::shared_ptr<graphar::EdgeInfo> edge_info;
    std::string prefix;
    graphar::IdType src_id;
};

struct TwoHopGlobalState {
public:
    TwoHopGlobalState(ClientContext& context, TwoHopBindData bind_data)
        : src_reader(MyAdjReaderOrdSrc(bind_data.GetEdgeInfo(), bind_data.GetPrefix())) {
        src_reader.find_src(bind_data.GetSrcId());
        hop_ids.reserve(src_reader.size());
    };

    const std::vector<std::int64_t>& GetHopIds() const { return hop_ids; }
    bool IsOneHop() const { return one_hop; }
    void SetOneHop(bool one_hop_) { one_hop = one_hop_; }
    MyAdjReaderOrdSrc& GetSrcReader() { return src_reader; }
    size_t GetHopI() const { return hop_i; }
    size_t IncrementHopI() { return hop_i++; }
    void AddHopId(std::int64_t id) { hop_ids.push_back(id); }

private:
    std::vector<std::int64_t> hop_ids;
    bool one_hop = true;
    MyAdjReaderOrdSrc src_reader;
    size_t hop_i = 0;
};

struct TwoHopGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    TwoHopGlobalTableFunctionState(ClientContext& context, TwoHopBindData& bind_data) : state(context, bind_data) {};

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);

    TwoHopGlobalState& GetState() { return state; }

private:
    TwoHopGlobalState state;
};

struct TwoHop {
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static void Execute(ClientContext& context, TableFunctionInput& data, DataChunk& output);
    static void Register(DatabaseInstance& db);
    static TableFunction GetFunction();
};

struct OneMoreHopGlobalState {
public:
    OneMoreHopGlobalState(ClientContext& context, TwoHopBindData bind_data)
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
<<<<<<< HEAD:src/include/functions/table/hop.hpp
   public:
    OneMoreHopGlobalTableFunctionState(ClientContext& context, TwoHopBindData& bind_data) : state(context, bind_data){};
=======
public:
    OneMoreHopGlobalTableFunctionState(ClientContext& context, TwoHopBindData& bind_data)
        : state(context, bind_data) {};
>>>>>>> upstream/main:include/functions/table/hop.hpp

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);

public:
    OneMoreHopGlobalState state;
};

struct OneMoreHop {
    // static unique_ptr<FunctionData> Bind(ClientContext &context,
    // TableFunctionBindInput &input, vector<LogicalType> &return_types,
    // vector<string> &names);
    static void Execute(ClientContext& context, TableFunctionInput& data, DataChunk& output);
    static void Register(DatabaseInstance& db);
    static TableFunction GetFunction();
};

}  // namespace duckdb
