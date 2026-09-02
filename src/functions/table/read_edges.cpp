#include "functions/table/read_edges.hpp"

#include "utils/benchmark.hpp"
#include "utils/func.hpp"

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
#include <graphar/fwd.h>

#include <set>

namespace duckdb {
//-------------------------------------------------------------------
// GetBindData
//-------------------------------------------------------------------
void ReadEdges::SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info,
                            std::shared_ptr<graphar::EdgeInfo> edge_info, unique_ptr<ReadBindData>& bind_data) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::SetBindData");
    ReadBase::SetBindData(graph_info, edge_info, bind_data, GetFunctionName(), 0, 1, {SRC_GID_COLUMN, DST_GID_COLUMN});
}
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadEdges::Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bind");

    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::Bind Parse parameters");

    const auto file_path = StringValue::Get(input.inputs[0]);
    const std::string src_type = StringValue::Get(input.named_parameters.at("src"));
    const std::string dst_type = StringValue::Get(input.named_parameters.at("dst"));
    const std::string e_type = StringValue::Get(input.named_parameters.at("type"));

    DUCKDB_GRAPHAR_LOG_DEBUG(src_type + "--" + e_type + "->" + dst_type + "\nLoad Graph Info and Edge Info");

    auto bind_data = make_uniq<ReadBindData>();
    DUCKDB_GRAPHAR_LOG_DEBUG("file path " + file_path);
    auto maybe_graph_info = graphar::GraphInfo::Load(file_path);
    if (maybe_graph_info.has_error()) {
        throw IOException("Failed to load graph info from path: %s", file_path);
    }
    auto graph_info = maybe_graph_info.value();

    auto edge_info = graph_info->GetEdgeInfo(src_type, e_type, dst_type);
    if (!edge_info) {
        throw BinderException("Edges of this type are not found");
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("Fill bind data");

    SetBindData(graph_info, edge_info, bind_data);

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
BaseReaderPtr ReadEdges::GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                       const std::string& filter_column,
                                       std::shared_ptr<graphar::SharedChunkCounter> counter) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetBaseReader");
    graphar::AdjListType adj_list_type;
    if (filter_column == "" or filter_column == SRC_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_source;
    } else if (filter_column == DST_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_dest;
    } else {
        throw NotImplementedException("Only src and dst filters are supported");
    }
    auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&gstate.type_info);
    if (!edge_info) {
        throw InternalException("Failed to get edge info");
    }
    const auto& prefix = gstate.graph_info->GetPrefix();
    const bool is_parquet = edge_info->GetAdjacentList(adj_list_type)->GetFileType() == graphar::FileType::PARQUET;
    const bool use_duck = GraphArSettings::use_duck_reader(context, is_parquet);
    if (ind == 0) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetBaseReader: making src and dst reader...");
        if (use_duck) {
            DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetBaseReader: making duckdb reader...");
            return ConvertBaseReader(graphar::AdjListChunkInfoReader::Make(edge_info, adj_list_type, prefix), counter);
        } else {
            DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetBaseReader: making arrow reader...");
            return ConvertBaseReader(graphar::AdjListArrowChunkReader::Make(edge_info, adj_list_type, prefix), counter);
        }
    }
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetBaseReader: making property reader...");
    if (use_duck) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetBaseReader: making duckdb reader...");
        return ConvertBaseReader(
            graphar::AdjListPropertyChunkInfoReader::Make(edge_info, gstate.pgs[ind - 1], adj_list_type, prefix),
            counter);
    } else {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetBaseReader: making arrow reader...");
        return ConvertBaseReader(
            graphar::AdjListPropertyArrowChunkReader::Make(edge_info, gstate.pgs[ind - 1], adj_list_type, prefix),
            counter);
    }
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
void ReadEdges::SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                          const vector<std::pair<int64_t, int64_t>>& vid_ranges, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::SetFilter");
    auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&gstate.type_info);
    if (!edge_info) {
        throw InternalException("Failed to get edge info");
    }
    const int64_t vertex_num = (filter_column == SRC_GID_COLUMN)
                                   ? GetCountClass::GetCount(gstate.graph_info->GetVertexInfo(edge_info->GetSrcType()),
                                                             gstate.graph_info->GetPrefix())
                                   : GetCountClass::GetCount(gstate.graph_info->GetVertexInfo(edge_info->GetDstType()),
                                                             gstate.graph_info->GetPrefix());
    const auto& prefix = gstate.graph_info->GetPrefix();
    for (idx_t r = 0; r < vid_ranges.size(); ++r) {
        const auto& vid_range = vid_ranges[r];
        if (vid_range.first < 0 || vid_range.first >= vertex_num || vid_range.second <= 0 ||
            vid_range.second > vertex_num) {
            throw BinderException("Invalid filter vertex id range");
        }
        FilterByRangeEdge(gstate.base_readers[ind][r], vid_range, filter_column, edge_info, prefix);
    }
}
//-------------------------------------------------------------------
// GetReader
//-------------------------------------------------------------------
ReaderPtr ReadEdges::GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                               ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader");
    graphar::AdjListType adj_list_type;
    if (filter_column == "" or filter_column == SRC_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_source;
    } else if (filter_column == DST_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_dest;
    } else {
        throw NotImplementedException("Only src and dst filters are supported");
    }
    auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&gstate.type_info);
    if (!edge_info) {
        throw InternalException("Failed to get edge info");
    }
    const auto& prefix = gstate.graph_info->GetPrefix();
    const bool is_parquet = edge_info->GetAdjacentList(adj_list_type)->GetFileType() == graphar::FileType::PARQUET;
    const bool use_duck = GraphArSettings::use_duck_reader(context, is_parquet);
    if (ind == 0) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making src and dst reader...");
        if (use_duck) {
            DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making duckdb reader...");
            std::vector<std::shared_ptr<graphar::TSAdjListChunkInfoReader>> base_readers;
            base_readers.reserve(gstate.base_readers[ind].size());
            for (const auto& base_reader : gstate.base_readers[ind]) {
                base_readers.push_back(std::get<std::shared_ptr<graphar::TSAdjListChunkInfoReader>>(base_reader));
            }
            return ConvertReader(graphar::DuckAdjListChunkReader::Make(context, lstate.file_reader, edge_info,
                                                                       adj_list_type, prefix, base_readers));
        } else {
            DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making arrow reader...");
            std::vector<std::shared_ptr<graphar::TSAdjListArrowChunkReader>> base_readers;
            base_readers.reserve(gstate.base_readers[ind].size());
            for (const auto& base_reader : gstate.base_readers[ind]) {
                base_readers.push_back(std::get<std::shared_ptr<graphar::TSAdjListArrowChunkReader>>(base_reader));
            }
            return ConvertReader(graphar::DuckAdjListArrowChunkReader::Make(context, base_readers));
        }
    }
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making property reader...");
    if (use_duck) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making duckdb reader...");
        std::vector<std::shared_ptr<graphar::TSAdjListPropertyChunkInfoReader>> base_readers;
        base_readers.reserve(gstate.base_readers[ind].size());
        for (const auto& base_reader : gstate.base_readers[ind]) {
            base_readers.push_back(std::get<std::shared_ptr<graphar::TSAdjListPropertyChunkInfoReader>>(base_reader));
        }
        return ConvertReader(graphar::DuckAdjListPropertyChunkReader::Make(
            context, lstate.file_reader, edge_info, gstate.pgs[ind - 1], adj_list_type, prefix, base_readers));
    } else {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making arrow reader...");
        std::vector<std::shared_ptr<graphar::TSAdjListPropertyArrowChunkReader>> base_readers;
        base_readers.reserve(gstate.base_readers[ind].size());
        for (const auto& base_reader : gstate.base_readers[ind]) {
            base_readers.push_back(std::get<std::shared_ptr<graphar::TSAdjListPropertyArrowChunkReader>>(base_reader));
        }
        return ConvertReader(graphar::DuckAdjListPropertyArrowChunkReader::Make(context, base_readers));
    }
}
//-------------------------------------------------------------------
// GetStatistics
//-------------------------------------------------------------------
unique_ptr<BaseStatistics> ReadEdges::GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                    column_t column_index) {
    return ReadBase<ReadEdges>::GetStatistics(context, bind_data, column_index);
}
//-------------------------------------------------------------------
// PushdownComplexFilter
//-------------------------------------------------------------------
void ReadEdges::PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                      vector<unique_ptr<Expression>>& filters) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::PushdownComplexFilter");
    if (!bind_data) {
        throw InternalException("Bind data is nullptr");
    }
    auto read_bind_data = dynamic_cast<ReadBindData*>(bind_data);
    for (auto& pg : read_bind_data->pgs) {
        if (pg->GetFileType() != graphar::FileType::PARQUET) {
            return;
        }
    }

    auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&read_bind_data->type_info);

    auto validate = [&](const std::string& col, const Value& val) -> bool {
        if (col != SRC_GID_COLUMN && col != DST_GID_COLUMN) return false;
        if (col == SRC_GID_COLUMN &&
            (!edge_info->HasAdjacentListType(graphar::AdjListType::ordered_by_source) ||
             edge_info->GetAdjacentList(graphar::AdjListType::ordered_by_source)->GetFileType() !=
                 graphar::FileType::PARQUET))
            return false;
        if (col == DST_GID_COLUMN &&
            (!edge_info->HasAdjacentListType(graphar::AdjListType::ordered_by_dest) ||
             edge_info->GetAdjacentList(graphar::AdjListType::ordered_by_dest)->GetFileType() !=
                 graphar::FileType::PARQUET))
            return false;
        return true;
    };

    ReadBase<ReadEdges>::PushdownComplexFilterImpl(context, *read_bind_data, filters, validate);
}
//-------------------------------------------------------------------
// InitFunction
//-------------------------------------------------------------------
static void InitFunction(TableFunction& read_edges) {
    read_edges.init_global = ReadEdges::Init;
    read_edges.init_local = ReadEdges::InitLocal;

    read_edges.filter_pushdown = false;
    read_edges.projection_pushdown = true;
    read_edges.statistics = ReadEdges::GetStatistics;
    read_edges.pushdown_complex_filter = ReadEdges::PushdownComplexFilter;

    read_edges.get_partition_data = ReadBase<ReadEdges>::GetPartitionData;
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunction ReadEdges::GetFunction() {
    TableFunction read_edges(Identifier(GetFunctionName()), {LogicalType::VARCHAR}, Execute, Bind);
    InitFunction(read_edges);

    read_edges.named_parameters["src"] = LogicalType::VARCHAR;
    read_edges.named_parameters["dst"] = LogicalType::VARCHAR;
    read_edges.named_parameters["type"] = LogicalType::VARCHAR;

    return read_edges;
}
//-------------------------------------------------------------------
// GetScanFunction
//-------------------------------------------------------------------
TableFunction ReadEdges::GetScanFunction() {
    TableFunction read_edges("", {LogicalType::VARCHAR}, Execute, Bind);
    InitFunction(read_edges);

    return read_edges;
}
}  // namespace duckdb
