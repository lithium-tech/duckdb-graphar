#include "functions/table/shortest_path.hpp"

#include "storage/graphar_table_entry.hpp"
#include "storage/graphar_table_information.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"
#include "utils/type_info.hpp"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <duckdb/planner/binder.hpp>

#include <queue>
#include <unordered_map>

namespace duckdb {

unique_ptr<FunctionData> ShortestPath::Bind(ClientContext& context, TableFunctionBindInput& input,
                                            vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("ShortestPath::Bind");

    auto bind_data = make_uniq<ShortestPathBindData>();
    bind_data->start_id = input.inputs[0].GetValue<graphar::IdType>();
    bind_data->end_id = input.inputs[1].GetValue<graphar::IdType>();

    // Check if named parameters exist (new signature) or not (old signature)
    bool use_yaml_path = !input.named_parameters.empty();

    if (use_yaml_path) {
        // New signature: file_path + named params (src, type, dst)
        const auto file_path = StringValue::Get(input.inputs[2]);
        const auto src_type = StringValue::Get(input.named_parameters.at("src"));
        const auto e_type = StringValue::Get(input.named_parameters.at("type"));
        const auto dst_type = StringValue::Get(input.named_parameters.at("dst"));

        DUCKDB_GRAPHAR_LOG_DEBUG("ShortestPath parameters: start=" + std::to_string(bind_data->start_id) +
                                 ", end=" + std::to_string(bind_data->end_id) + ", file=" + file_path +
                                 ", edge=" + src_type + "_" + e_type + "_" + dst_type);

        // Load graph info from YAML path directly (no catalog lookup)
        auto maybe_graph_info = graphar::GraphInfo::Load(file_path);
        if (maybe_graph_info.has_error()) {
            throw IOException("Failed to load graph info from path: %s", file_path);
        }
        bind_data->graph_info = maybe_graph_info.value();

        // Get edge info by type names
        auto edge_info = bind_data->graph_info->GetEdgeInfo(src_type, e_type, dst_type);
        if (!edge_info) {
            throw BinderException("Edges of this type are not found: " + src_type + "_" + e_type + "_" + dst_type);
        }
        bind_data->edge_info = edge_info;

        auto src_vtype = edge_info->GetSrcType();
        auto dst_vtype = edge_info->GetDstType();
        if (src_vtype != dst_vtype) {
            throw InvalidInputException(
                "Shortest path requires same vertex type for source and destination. "
                "Got src=" +
                src_vtype + ", dst=" + dst_vtype);
        }
        if (!edge_info->IsDirected()) {
            throw InvalidInputException(
                "Shortest path algorithm only supports directed graphs. "
                "Edge type '" +
                edge_info->GetEdgeType() + "' is undirected.");
        }
        bind_data->vertex_info = bind_data->graph_info->GetVertexInfo(src_vtype);
        if (!bind_data->vertex_info) {
            throw InvalidInputException("Failed to get vertex info for type: " + src_vtype);
        }
    } else {
        // Old signature: table_name - use catalog lookup
        auto table_name = input.inputs[2].GetValue<string>();

        DUCKDB_GRAPHAR_LOG_DEBUG("ShortestPath parameters: start=" + std::to_string(bind_data->start_id) +
                                 ", end=" + std::to_string(bind_data->end_id) + ", table=" + table_name);

        auto qname = QualifiedName::Parse(table_name);
        auto catalog_name = qname.Catalog();
        auto schema_name = qname.Schema();
        Binder::BindSchemaOrCatalog(context, catalog_name, schema_name);

        auto& entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, catalog_name, schema_name, qname.Name());

        auto& table_entry = entry.template Cast<GraphArTableEntry>();
        auto table_info = table_entry.GetTableInfo();
        if (table_info == nullptr) {
            throw InvalidInputException("Table info for '" + table_name + "' is expired.");
        }
        auto type_info = table_info->GetTypeInfo();

        if (!std::holds_alternative<std::shared_ptr<graphar::EdgeInfo>>(type_info)) {
            throw InvalidInputException("Table '" + table_name + "' is not an edge table.");
        }

        bind_data->edge_info = std::get<std::shared_ptr<graphar::EdgeInfo>>(type_info);
        bind_data->graph_info = table_info->GetCatalog().GetGraphInfo();

        auto src_type = bind_data->edge_info->GetSrcType();
        auto dst_type = bind_data->edge_info->GetDstType();
        if (src_type != dst_type) {
            throw InvalidInputException(
                "Shortest path requires same vertex type for source and destination. "
                "Got src=" +
                src_type + ", dst=" + dst_type);
        }
        if (!bind_data->edge_info->IsDirected()) {
            throw InvalidInputException(
                "Shortest path algorithm only supports directed graphs. "
                "Edge type '" +
                bind_data->edge_info->GetEdgeType() + "' is undirected.");
        }
        bind_data->vertex_info = bind_data->graph_info->GetVertexInfo(src_type);
        if (bind_data->vertex_info == nullptr) {
            throw InvalidInputException("Failed to get vertex info for type: " + src_type);
        }
    }

    return_types = {LogicalType::BIGINT, LogicalType::BIGINT};
    names = {"step_number", "_graphArVertexIndex"};

    return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> ShortestPath::InitGlobal(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("ShortestPath::InitGlobal");

    auto global_state = make_uniq<ShortestPathGlobalState>();
    const auto& bind_data = input.bind_data->Cast<ShortestPathBindData>();

    global_state->start_id = bind_data.start_id;
    global_state->end_id = bind_data.end_id;
    global_state->path_found = false;
    global_state->current_step = 0;

    // Create forward edges collection (ordered by source for efficient forward traversal)
    auto forward_edges_result = graphar::EdgesCollection::Make(
        bind_data.graph_info, bind_data.edge_info->GetSrcType(), bind_data.edge_info->GetEdgeType(),
        bind_data.edge_info->GetDstType(), graphar::AdjListType::ordered_by_source);

    if (forward_edges_result.has_error()) {
        throw InvalidInputException(
            "Failed to create forward edges collection for: " + bind_data.edge_info->GetSrcType() + "--" +
            bind_data.edge_info->GetEdgeType() + "->" + bind_data.edge_info->GetDstType());
    }

    // Create backward edges collection (ordered by destination for efficient backward traversal)
    auto backward_edges_result = graphar::EdgesCollection::Make(
        bind_data.graph_info, bind_data.edge_info->GetSrcType(), bind_data.edge_info->GetEdgeType(),
        bind_data.edge_info->GetDstType(), graphar::AdjListType::ordered_by_dest);

    if (backward_edges_result.has_error()) {
        throw InvalidInputException(
            "Failed to create backward edges collection for: " + bind_data.edge_info->GetSrcType() + "--" +
            bind_data.edge_info->GetEdgeType() + "->" + bind_data.edge_info->GetDstType());
    }

    global_state->forward_edges = forward_edges_result.value();
    global_state->backward_edges = backward_edges_result.value();

    TypeInfoPtr vertex_type_info = bind_data.vertex_info;
    auto vertex_count = GetCountClass::GetCount(vertex_type_info, bind_data.graph_info->GetPrefix());

    if (bind_data.start_id < 0 || bind_data.end_id < 0 || bind_data.start_id >= vertex_count ||
        bind_data.end_id >= vertex_count) {
        global_state->path_found = false;
        return std::move(global_state);
    }

    if (bind_data.start_id == bind_data.end_id) {
        global_state->path_found = true;
        global_state->path = {bind_data.start_id};
    } else {
        // Bidirectional BFS: search from both start and end
        std::vector<bool> visited_forward(vertex_count, false);
        std::vector<bool> visited_backward(vertex_count, false);
        std::vector<graphar::IdType> parent_forward(vertex_count, std::numeric_limits<graphar::IdType>::max());
        std::vector<graphar::IdType> parent_backward(vertex_count, std::numeric_limits<graphar::IdType>::max());

        std::queue<graphar::IdType> q_forward;
        std::queue<graphar::IdType> q_backward;

        q_forward.push(bind_data.start_id);
        q_backward.push(bind_data.end_id);
        visited_forward[bind_data.start_id] = true;
        visited_backward[bind_data.end_id] = true;
        parent_forward[bind_data.start_id] = bind_data.start_id;
        parent_backward[bind_data.end_id] = bind_data.end_id;

        graphar::IdType meeting_vertex = std::numeric_limits<graphar::IdType>::max();
        bool found = false;

        // Alternate between forward and backward search
        while (!q_forward.empty() && !q_backward.empty() && !found) {
            // Expand forward frontier (level by level)
            auto forward_level_size = q_forward.size();
            for (idx_t i = 0; i < forward_level_size && !found; i++) {
                auto curr = q_forward.front();
                q_forward.pop();

                // Use find_src to get iterator for edges from curr
                auto forward_iter = global_state->forward_edges->find_src(curr, global_state->forward_edges->begin());

                if (forward_iter != global_state->forward_edges->end()) {
                    do {
                        auto dst = forward_iter.destination();
                        if (dst >= 0 && dst < vertex_count && !visited_forward[dst]) {
                            visited_forward[dst] = true;
                            parent_forward[dst] = curr;

                            // Check if frontiers meet
                            if (visited_backward[dst]) {
                                meeting_vertex = dst;
                                found = true;
                                break;
                            }
                            q_forward.push(dst);
                        }
                    } while (forward_iter.next_src());
                }
            }

            if (found) break;

            // Expand backward frontier (level by level)
            auto backward_level_size = q_backward.size();
            for (idx_t i = 0; i < backward_level_size && !found; i++) {
                auto curr = q_backward.front();
                q_backward.pop();

                // Use find_dst to get iterator for edges to curr
                auto backward_iter =
                    global_state->backward_edges->find_dst(curr, global_state->backward_edges->begin());

                if (backward_iter != global_state->backward_edges->end()) {
                    do {
                        auto src = backward_iter.source();
                        if (src >= 0 && src < vertex_count && !visited_backward[src]) {
                            visited_backward[src] = true;
                            parent_backward[src] = curr;

                            // Check if frontiers meet
                            if (visited_forward[src]) {
                                meeting_vertex = src;
                                found = true;
                                break;
                            }
                            q_backward.push(src);
                        }
                    } while (backward_iter.next_dst());
                }
            }
        }

        if (found) {
            global_state->path_found = true;

            // Reconstruct path: start -> meeting_vertex (forward) + meeting_vertex -> end (backward)
            std::vector<graphar::IdType> forward_path;
            auto curr = meeting_vertex;
            while (curr != bind_data.start_id) {
                forward_path.push_back(curr);
                curr = parent_forward[curr];
            }
            forward_path.push_back(bind_data.start_id);
            std::reverse(forward_path.begin(), forward_path.end());

            std::vector<graphar::IdType> backward_path;
            curr = meeting_vertex;
            while (curr != bind_data.end_id) {
                curr = parent_backward[curr];
                backward_path.push_back(curr);
            }

            // Combine paths
            forward_path.insert(forward_path.end(), backward_path.begin(), backward_path.end());
            global_state->path = forward_path;
        }
    }

    return std::move(global_state);
}

void ShortestPath::Function(ClientContext& context, TableFunctionInput& data_p, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("ShortestPath::Function");

    auto& bind_data = data_p.bind_data->Cast<ShortestPathBindData>();
    auto& global_state = data_p.global_state->Cast<ShortestPathGlobalState>();

    if (!global_state.path_found || global_state.current_step >= global_state.path.size()) {
        output.SetCardinality(0);
        return;
    }

    output.SetCardinality(1);
    auto& step_vector = output.data[0];
    auto& vertex_vector = output.data[1];

    step_vector.SetVectorType(VectorType::FLAT_VECTOR);
    vertex_vector.SetVectorType(VectorType::FLAT_VECTOR);

    auto step_data = FlatVector::GetDataMutable<int64_t>(step_vector);
    auto vertex_data = FlatVector::GetDataMutable<int64_t>(vertex_vector);

    step_data[0] = static_cast<int64_t>(global_state.current_step);
    vertex_data[0] = static_cast<int64_t>(global_state.path[global_state.current_step]);

    global_state.current_step++;
}

TableFunction ShortestPath::GetFunction() {
    // Supports two signatures:
    // 1. shortest_path(start_id, end_id, edge_table_name) - uses catalog lookup
    // 2. shortest_path(start_id, end_id, graph_path, src=..., type=..., dst=...) - uses YAML path
    TableFunction func(Identifier("shortest_path"), {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
                       Function, Bind, InitGlobal);
    func.named_parameters["src"] = LogicalType::VARCHAR;
    func.named_parameters["type"] = LogicalType::VARCHAR;
    func.named_parameters["dst"] = LogicalType::VARCHAR;
    return func;
}

void ShortestPath::Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunction()); }

}  // namespace duckdb
