#include "readers/duck_read_edges_reader.hpp"

namespace {
constexpr std::string_view SQL_SELECT_CLAUSE = "SELECT";
constexpr std::string_view SQL_FROM_CLAUSE = "FROM";
constexpr std::string_view SQL_WHERE_CLAUSE = "WHERE";
constexpr std::string_view SQL_IN_CLAUSE = "IN";
constexpr std::string_view SQL_AND_CLAUSE = "AND";
constexpr std::string_view READ_EDGES_FUNCTION = "read_edges";
constexpr std::string_view READ_EDGES_SOURCE_ARG = "src";
constexpr std::string_view READ_EDGES_EDGE_TYPE_ARG = "type";
constexpr std::string_view READ_EDGES_DESTINATION_ARG = "dst";

}  // namespace

namespace duckdb {

std::string QueryReadEdgesStringConstructor::GetQueryReadString(const std::string& graph_info_path,
                                                                std::shared_ptr<graphar::EdgeInfo> info) {
    DUCKDB_GRAPHAR_LOG_TRACE("QueryReadEdgesStringConstructor::GetQueryBaseString");

    std::ostringstream ss;

    ss << READ_EDGES_FUNCTION << "('" << graph_info_path << "', " << READ_EDGES_SOURCE_ARG << "=" << info->GetSrcType()
       << ", " << READ_EDGES_EDGE_TYPE_ARG << "=" << info->GetEdgeType() << ", " << READ_EDGES_DESTINATION_ARG << "="
       << info->GetDstType() << ")";

    return ss.str();
}

std::string const QueryReadEdgesStringConstructor::GetQueryFilterString(const std::vector<graphar::IdType>& vids) {
    DUCKDB_GRAPHAR_LOG_TRACE("QueryReadEdgesStringConstructor::GetQueryFilterString");

    std::ostringstream ss;

    switch (query_filter_type) {
        case QueryFilterType::SRC:
            ss << SRC_GID_COLUMN << " " << SQL_IN_CLAUSE << " (";
            break;
        case QueryFilterType::DST:
            ss << DST_GID_COLUMN << " " << SQL_IN_CLAUSE << " (";
            break;
        default:
            throw NotImplementedException("Unsupported edge direction filter type");
    }

    for (size_t i = 0; i < vids.size(); ++i) {
        if (i) ss << ',';
        ss << vids[i];
    }
    ss << ')';

    if (!query_filter.empty()) {
        ss << " " << SQL_AND_CLAUSE << " " << query_filter;
    }

    return ss.str();
}

std::string QueryReadEdgesStringConstructor::GetQueryColumnsString(const std::vector<column_t>& proj_columns) {
    DUCKDB_GRAPHAR_LOG_TRACE("QueryReadEdgesStringConstructor::GenerateQueryColumnsString");

    std::ostringstream ss;

    for (idx_t i = 0; i + 1 < proj_columns.size(); ++i) {
        ss << "#" << proj_columns[i] + 1 << ", ";
    }
    ss << "#" << proj_columns.back() + 1 << ' ';

    return ss.str();
}

void QueryReadEdgesStringConstructor::GenerateQueryTableString(const std::string& edge_table_name,
                                                               const std::string& graph_info_path,
                                                               std::shared_ptr<graphar::EdgeInfo> info) {
    DUCKDB_GRAPHAR_LOG_TRACE("QueryReadEdgesStringConstructor::GenerateQueryTableString");

    if (edge_table_name.empty()) {
        query_table = GetQueryReadString(graph_info_path, info);
    } else {
        query_table = edge_table_name;
    }
}

std::string QueryReadEdgesStringConstructor::GetQueryString(const std::vector<column_t>& proj_columns,
                                                            const std::vector<graphar::IdType>& vids) {
    DUCKDB_GRAPHAR_LOG_TRACE("QueryReadEdgesStringConstructor::GetQueryString");

    if (!isReady()) {
        throw InternalException("QueryReadEdgesStringConstructor is not ready!");
    }

    std::ostringstream ss;
    ss << SQL_SELECT_CLAUSE << " " << GetQueryColumnsString(proj_columns) << " " << SQL_FROM_CLAUSE << " "
       << query_table << " " << SQL_WHERE_CLAUSE << " " << GetQueryFilterString(vids);
    return ss.str();
}

}  // namespace duckdb