#include "functions/scalar/bfs.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "graphar/graph_info.h"
#include "graphar/high-level/graph_reader.h"
#include "graphar/types.h"
#include "utils/benchmark.hpp"
#include "utils/global_log_manager.hpp"

namespace duckdb {

template <typename T>
void clear(std::queue<T>& q) {
    std::queue<T> empty;
    std::swap(q, empty);
}

void Bfs::WayLength(DataChunk& args, ExpressionState& state, Vector& result) {
    auto& context = state.GetContext();
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bfs::WayLength");

    LOG_TRACE("Starting Bfs::WayLength");
    LOG_TRACE("Parsing parameters...");

    auto& start_vector = args.data[0];
    auto& aim_vector = args.data[1];
    auto& graphar_info_vector = args.data[2];
    const auto& number = args.size();

    std::string path_graph = graphar_info_vector.GetValue(0).GetValue<std::string>();

    LOG_DEBUG("Read Graph info: " + path_graph);

    auto graph_info = graphar::GraphInfo::Load(path_graph).value();
    std::string type = graph_info->GetVertexInfoByIndex(0)->GetType();

    LOG_DEBUG("Making Vertex reader: " + type);
    if (time_logging) {
        t.print("graph info");
    }

    auto maybe_vertices = graphar::VerticesCollection::Make(graph_info, type);
    auto& vertices = maybe_vertices.value();
    auto vert_num = vertices->size();

    LOG_DEBUG("Vertices number: " + std::to_string(vert_num));
    if (time_logging) {
        t.print("vertices collection");
    }

    std::string src_type = "Person", edge_type = "knows", dst_type = "Person";

    LOG_DEBUG("Read Edge info");

    {
        auto edge_info = graph_info->GetEdgeInfoByIndex(0);
        src_type = edge_info->GetSrcType();
        edge_type = edge_info->GetEdgeType();
        dst_type = edge_info->GetDstType();
    }

    LOG_DEBUG("Making Edge reader: " + src_type + "--" + edge_type + "->" + dst_type);

    auto maybe_edges = graphar::EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
                                                      graphar::AdjListType::ordered_by_source);
    auto& edges = maybe_edges.value();

    LOG_DEBUG("Edges number: " + std::to_string(edges->size()));
    if (time_logging) {
        t.print("edges collection");
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<long long>(result);

    if (time_logging) {
        t.print("preprocessing", true);
    }

    for (idx_t i = 0; i < number; i++) {
        auto start = start_vector.GetValue(i).GetValue<int64_t>();
        auto aim = aim_vector.GetValue(i).GetValue<int64_t>();

        LOG_DEBUG("BFS start: " + std::to_string(start) + "->" + std::to_string(aim));

        std::vector<int16_t> visited(vert_num, -1);
        std::queue<int64_t> q;
        q.push(start);
        visited[start] = 0;
        auto last = 0;
        auto was = 0;

        while (!q.empty()) {
            auto now = visited[q.front()];
            if (now != last) {
                LOG_DEBUG("Hop " + std::to_string(now) + " finished. Will: " + std::to_string(q.size()));
                last = now;
            }
            ++now;
            was = q.front();
            auto iter = edges->find_src(q.front(), edges->begin());
            q.pop();
            if (iter != edges->end()) {
                do {
                    if (visited[iter.destination()] == -1) {
                        visited[iter.destination()] = now;
                        q.push(iter.destination());
                        if (iter.destination() == aim) {
                            clear(q);
                            break;
                        }
                    }
                } while (iter.next_src());
            }
        }
        LOG_DEBUG("BFS finished. Distance " + std::to_string(visited[aim]));
        result_data[i] = visited[aim];
        if (time_logging) {
            t.print("one finished");
        }
    }
    if (time_logging) {
        t.print();
    }
}

void Bfs::WayExists(DataChunk& args, ExpressionState& state, Vector& result) {
    auto& context = state.GetContext();
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("BfsWayExists");

    LOG_TRACE("Starting Bfs::WayExists");
    LOG_TRACE("Running auxiliary Bfs::WayLength function");

    const auto& number = args.size();
    auto result_temp = Vector(LogicalType::BIGINT, true, true, number);
    Bfs::WayLength(args, state, result_temp);

    LOG_DEBUG("Bfs::WayLength finished. Converting to boolean");

    auto result_data = FlatVector::GetData<long long>(result);
    for (idx_t i = 0; i < number; i++) {
        result_data[i] = (result_data[i] != -1);
    }
    LOG_DEBUG("Converting finished");
    if (time_logging) {
        t.print();
    }
}

ScalarFunction Bfs::GetFunctionExists() {
    ScalarFunction bfs_exist("bfs_exist", {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
                             LogicalType::BOOLEAN, WayExists);

    return bfs_exist;
}

ScalarFunction Bfs::GetFunctionLength() {
    ScalarFunction bfs_length("bfs_length", {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
                              LogicalType::BIGINT, WayLength);

    return bfs_length;
}

void Bfs::Register(DatabaseInstance& db) {
    ExtensionUtil::RegisterFunction(db, GetFunctionExists());
    ExtensionUtil::RegisterFunction(db, GetFunctionLength());
}
}  // namespace duckdb