#include "functions/table/read_vertices.hpp"

#include "utils/benchmark.hpp"
#include "utils/func.hpp"

#include <set>

#include <arrow/c/bridge.h>

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>

#include <graphar/api/arrow_reader.h>
#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/expression.h>
#include <graphar/filesystem.h>
#include <graphar/fwd.h>

namespace duckdb {
//-------------------------------------------------------------------
// GetBindData
//-------------------------------------------------------------------
void ReadVertices::SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info,
                               std::shared_ptr<graphar::VertexInfo> vertex_info, unique_ptr<ReadBindData>& bind_data) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::SetBindData");
    ReadBase::SetBindData(graph_info, vertex_info, bind_data, "read_vertices", 1, 0, {GID_COLUMN_INTERNAL});
}
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadVertices::Bind(ClientContext& context, TableFunctionBindInput& input,
                                            vector<LogicalType>& return_types, vector<string>& names) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bind");

    DUCKDB_GRAPHAR_LOG_DEBUG("ReadVertices::Bind Parse parameters");

    const auto file_path = StringValue::Get(input.inputs[0]);
    const std::string v_type = StringValue::Get(input.named_parameters.at("type"));

    DUCKDB_GRAPHAR_LOG_DEBUG("Get type " + v_type + '\n' + "Load Graph Info and Vertex Info");

    auto bind_data = make_uniq<ReadBindData>();
    auto maybe_graph_info = graphar::GraphInfo::Load(file_path);
    if (maybe_graph_info.has_error()) {
        throw IOException("Failed to load graph info from path: %s", file_path);
    }
    auto graph_info = maybe_graph_info.value();

    auto vertex_info = graph_info->GetVertexInfo(v_type);

    if (!vertex_info) {
        throw BinderException("No vertices of this type");
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("Fill bind data");

    SetBindData(graph_info, vertex_info, bind_data);

    names = bind_data->flatten_prop_names;
    std::transform(bind_data->flatten_prop_types.begin(), bind_data->flatten_prop_types.end(),
                   std::back_inserter(return_types),
                   [](const auto& return_type) { return GraphArFunctions::graphArT2duckT(return_type); });

    DUCKDB_GRAPHAR_LOG_DEBUG("Bind finish");
    if (time_logging) {
        t.print();
    }

    return bind_data;
}
//-------------------------------------------------------------------
// GetBaseReader
//-------------------------------------------------------------------
BaseReaderPtr ReadVertices::GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                          const std::string& filter_column,
                                          std::shared_ptr<graphar::SharedChunkCounter> counter) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::GetBaseReader");
    auto vertex_info = *std::get_if<std::shared_ptr<graphar::VertexInfo>>(&gstate.type_info);
    if (!vertex_info) {
        throw InternalException("Failed to get vertex info");
    }
    const auto& prefix = gstate.graph_info->GetPrefix();
    if (gstate.pgs[ind]->GetFileType() == graphar::FileType::PARQUET) {
        DUCKDB_GRAPHAR_LOG_DEBUG("Making duckdb reader");
        return ConvertBaseReader(graphar::VertexPropertyChunkInfoReader::Make(vertex_info, gstate.pgs[ind], prefix),
                                 counter);
    } else {
        DUCKDB_GRAPHAR_LOG_DEBUG("Making arrow reader");
        return ConvertBaseReader(graphar::VertexPropertyArrowChunkReader::Make(vertex_info, gstate.pgs[ind], prefix),
                                 counter);
    }
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
void ReadVertices::SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                             const vector<std::pair<int64_t, int64_t>>& vid_ranges, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::SetFilter");
    auto vertex_info = *std::get_if<std::shared_ptr<graphar::VertexInfo>>(&gstate.type_info);
    if (!vertex_info) {
        throw InternalException("Failed to get vertex info");
    }
    int64_t vertex_num = GetCountClass::GetCount(vertex_info, gstate.graph_info->GetPrefix());
    for (idx_t r = 0; r < vid_ranges.size(); ++r) {
        const auto& vid_range = vid_ranges[r];
        if (vid_range.first < 0 || vid_range.first >= vertex_num || vid_range.second <= 0 ||
            vid_range.second > vertex_num) {
            throw BinderException("Invalid filter vertex id range");
        }
        FilterByRangeVertex(gstate.base_readers[ind][r], vid_range, filter_column, vertex_info);
    }
}
//-------------------------------------------------------------------
// GetReader
//-------------------------------------------------------------------
ReaderPtr ReadVertices::GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                                  ReadBaseLocalTableFunctionState& lstate, idx_t ind,
                                  const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::GetReader");
    auto vertex_info = *std::get_if<std::shared_ptr<graphar::VertexInfo>>(&gstate.type_info);
    if (!vertex_info) {
        throw InternalException("Failed to get vertex info");
    }
    const auto& prefix = gstate.graph_info->GetPrefix();
    if (gstate.pgs[ind]->GetFileType() == graphar::FileType::PARQUET) {
        DUCKDB_GRAPHAR_LOG_DEBUG("Making duckdb reader");
        std::vector<std::shared_ptr<graphar::TSVertexPropertyChunkInfoReader>> base_readers;
        base_readers.reserve(gstate.base_readers[ind].size());
        for (const auto& base_reader : gstate.base_readers[ind]) {
            base_readers.push_back(std::get<std::shared_ptr<graphar::TSVertexPropertyChunkInfoReader>>(base_reader));
        }
        return ConvertReader(graphar::DuckVertexPropertyChunkReader::Make(context, lstate.file_reader, vertex_info,
                                                                          gstate.pgs[ind], prefix, base_readers));
    } else {
        DUCKDB_GRAPHAR_LOG_DEBUG("Making arrow reader");
        std::vector<std::shared_ptr<graphar::TSVertexPropertyArrowChunkReader>> base_readers;
        base_readers.reserve(gstate.base_readers[ind].size());
        for (const auto& base_reader : gstate.base_readers[ind]) {
            base_readers.push_back(std::get<std::shared_ptr<graphar::TSVertexPropertyArrowChunkReader>>(base_reader));
        }
        return ConvertReader(graphar::DuckVertexPropertyArrowChunkReader::Make(context, base_readers));
    }
}
//-------------------------------------------------------------------
// GetStatistics
//-------------------------------------------------------------------
unique_ptr<BaseStatistics> ReadVertices::GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                       column_t column_index) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::GetStatistics");
    auto read_bind_data = bind_data->Cast<ReadBindData>();
    if (column_index < 0 || column_index >= read_bind_data.GetFlattenPropTypes().size()) {
        return nullptr;
    }
    auto duck_type = GraphArFunctions::graphArT2duckT(read_bind_data.GetFlattenPropTypes()[column_index]);
    auto column_name = read_bind_data.GetFlattenPropNames()[column_index];
    auto stats = BaseStatistics::CreateUnknown(duck_type);
    return stats.ToUnique();
}
//-------------------------------------------------------------------
// PushdownComplexFilter
//-------------------------------------------------------------------
void ReadVertices::PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                         vector<unique_ptr<Expression>>& filters) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::PushdownComplexFilter");
    if (!bind_data) {
        throw InternalException("Bind data is nullptr");
    }
    auto read_bind_data = dynamic_cast<ReadBindData*>(bind_data);
    for (auto& pg : read_bind_data->pgs) {
        if (pg->GetFileType() != graphar::FileType::PARQUET) {
            return;
        }
    }
    
    auto vertex_info = *std::get_if<std::shared_ptr<graphar::VertexInfo>>(&read_bind_data->type_info);
    const auto vertex_num = GetCountClass::GetCount(read_bind_data->type_info, read_bind_data->GetGraphInfo()->GetPrefix());
    
    // Validation lambda for vertices
    auto validate = [&](const std::string& col, const Value& val) -> bool {
        if (col != GID_COLUMN_INTERNAL) return false;
        return true;
    };
    
    ReadBase<ReadVertices>::PushdownComplexFilterImpl(context, *read_bind_data, filters, validate, vertex_num);
}
//-------------------------------------------------------------------
// InitFunction
//-------------------------------------------------------------------
static void InitFunction(TableFunction& read_vertices) {
    read_vertices.init_global = ReadVertices::Init;
    read_vertices.init_local = ReadVertices::InitLocal;

    read_vertices.filter_pushdown = false;
    read_vertices.projection_pushdown = true;
    read_vertices.statistics = ReadVertices::GetStatistics;
    read_vertices.pushdown_complex_filter = ReadVertices::PushdownComplexFilter;

    read_vertices.get_partition_data = ReadBase<ReadVertices>::GetPartitionData;
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunction ReadVertices::GetFunction() {
    TableFunction read_vertices("read_vertices", {LogicalType::VARCHAR}, Execute, Bind);
    InitFunction(read_vertices);

    read_vertices.named_parameters["type"] = LogicalType::VARCHAR;

    return read_vertices;
}
//-------------------------------------------------------------------
// GetScanFunction
//-------------------------------------------------------------------
TableFunction ReadVertices::GetScanFunction() {
    TableFunction read_vertices({}, Execute, Bind);
    InitFunction(read_vertices);

    return read_vertices;
}
}  // namespace duckdb
