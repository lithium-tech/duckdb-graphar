#include "functions/table/hop_thread.hpp"

#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>

#include <graphar/api/high_level_reader.h>

#include <iostream>

namespace duckdb {
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> TwoHopThreads::Bind(ClientContext& context, TableFunctionBindInput& input,
                                             vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHopThreads::Bind");

    const auto edge_info_path = StringValue::Get(input.inputs[0]);
    const int64_t vid = IntegerValue::Get(input.named_parameters.at("vid"));

    return_types.push_back(LogicalType::BIGINT);
    names.push_back(SRC_GID_COLUMN);
    return_types.push_back(LogicalType::BIGINT);
    names.push_back(DST_GID_COLUMN);

    return std::move(std::make_unique<TwoHopThreadsBindData>(edge_info_path, vid));
}
//-------------------------------------------------------------------
// State Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> TwoHopThreadsGlobalTableFunctionState::Init(ClientContext& context,
                                                                                 TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHopThreads::GlobalInit");
    auto bind_data = input.bind_data->Cast<TwoHopThreadsBindData>();
    std::unique_ptr<TwoHopThreadsGlobalTableFunctionState> global_state =
        std::make_unique<TwoHopThreadsGlobalTableFunctionState>(context, bind_data);
    auto& state = global_state->GetState();
    std::string yaml_content = GetYamlContent(bind_data.edge_info_path);
    auto edge_info = graphar::EdgeInfo::Load(yaml_content).value();
    if (!edge_info) {
        throw BinderException("No found edge this type");
    }
    const std::string prefix = GetDirectory(bind_data.edge_info_path);
    std::shared_ptr<OffsetReader> offset_reader =
        std::make_shared<OffsetReader>(edge_info, prefix, graphar::AdjListType::ordered_by_source);
    LowEdgeReaderByVertex one_hop_reader =
        LowEdgeReaderByVertex(edge_info, prefix, graphar::AdjListType::ordered_by_source, offset_reader);
    one_hop_reader.SetVertex(bind_data.vid);

    std::unique_ptr<Connection> conn = std::make_unique<Connection>(*context.db);
    one_hop_reader.start(std::move(conn));

    for (std::unique_ptr<DataChunk> data = std::move(one_hop_reader.read()); data != nullptr;
         data = std::move(one_hop_reader.read())) {
        for (int i = 0; i < data->size(); ++i) {
            std::unique_ptr<LowEdgeReaderByVertex> reader = std::make_unique<LowEdgeReaderByVertex>(
                edge_info, prefix, graphar::AdjListType::ordered_by_source, offset_reader);
            reader->SetVertex(data->GetValue(1, i).GetValue<int64_t>());
            state.vertex_readers.push(std::move(reader));
        }
    }

    return std::move(global_state);
}

unique_ptr<LocalTableFunctionState> TwoHopThreadsLocalTableFunctionState::Init(ExecutionContext& context,
                                                                               TableFunctionInitInput& input,
                                                                               GlobalTableFunctionState* global_state) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHopThreads::LocalStateInit");
    std::unique_ptr<TwoHopThreadsLocalTableFunctionState> local_state =
        std::make_unique<TwoHopThreadsLocalTableFunctionState>();
    return std::move(local_state);
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
void TwoHopThreads::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHopThreads::Execute")
    TwoHopThreadsGlobalState& gstate = input.global_state->Cast<TwoHopThreadsGlobalTableFunctionState>().GetState();

    std::unique_ptr<LowEdgeReaderByVertex> reader;
    while (!gstate.vertex_readers.empty()) {
        {
            std::lock_guard<std::mutex> lock(gstate.vertex_readers_mutex);
            reader = std::move(gstate.vertex_readers.front());
            gstate.vertex_readers.pop();
        }
        if (!reader->started()) {
            std::unique_ptr<Connection> conn = std::make_unique<Connection>(*context.db);
            reader->start(std::move(conn));
        }
        std::unique_ptr<DataChunk> data = std::move(reader->read());

        if (data != nullptr) {
            output.Reference(*data);
            {
                std::lock_guard<std::mutex> lock(gstate.vertex_readers_mutex);
                gstate.vertex_readers.push(std::move(reader));
            }
            return;
        } else {
            DUCKDB_GRAPHAR_LOG_DEBUG("Vertex finished")
        }
    }
    output.SetCardinality(0);
}
//-------------------------------------------------------------------
// Function
//-------------------------------------------------------------------
TableFunction TwoHopThreads::GetFunction() {
    TableFunction read_edges("two_hop_threads", {LogicalType::VARCHAR}, Execute, Bind,
                             TwoHopThreadsGlobalTableFunctionState::Init, TwoHopThreadsLocalTableFunctionState::Init);
    read_edges.named_parameters["vid"] = LogicalType::INTEGER;

    return read_edges;
}

void TwoHopThreads::Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunction()); }
}  // namespace duckdb