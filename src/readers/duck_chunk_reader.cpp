#include "readers/duck_chunk_reader.hpp"

namespace {
constexpr std::string_view SQL_SELECT_CLAUSE = "SELECT";
constexpr std::string_view SQL_FROM_CLAUSE = "FROM";
constexpr std::string_view SQL_WHERE_CLAUSE = "WHERE";
constexpr std::string_view SQL_BETWEEN_CLAUSE = "BETWEEN";
constexpr std::string_view SQL_LIMIT_CLAUSE = "LIMIT";
constexpr std::string_view SQL_OFFSET_CLAUSE = "OFFSET";
constexpr std::string_view READ_PARQUET_FUNCTION = "read_parquet";
constexpr std::string_view FILE_ROW_NUMBER_CLAUSE = "file_row_number";
}  // namespace

namespace duckdb {

std::string QueryStringConstructor::GetMainQueryString(const std::vector<column_t>& proj_columns,
                                                       std::pair<int64_t, int64_t> range) {
    QueryType query_type = QueryType::MIDDLE;
    if (range.first != -1 && range.second != -1) {
        query_type = QueryType::SINGLE;
    } else if (range.first != -1) {
        query_type = QueryType::FIRST;
    } else if (range.second != -1) {
        query_type = QueryType::LAST;
    }
    std::ostringstream ss;
    ss << SQL_SELECT_CLAUSE << " ";
    switch (file_type) {
        case graphar::FileType::PARQUET:
            for (idx_t i = 0; i + 1 < proj_columns.size(); ++i) {
                ss << "#" << to_string(proj_columns[i] + 1) << ", ";
            }
            ss << "#" << to_string(proj_columns.back() + 1);
            ss << " " << SQL_FROM_CLAUSE << " ";
            ss << READ_PARQUET_FUNCTION;
            ss << "($1, " << FILE_ROW_NUMBER_CLAUSE << "=true)";
            switch (query_type) {
                case QueryType::FIRST:
                    ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " >= " << range.first;
                    break;
                case QueryType::LAST:
                    ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " < " << range.second;
                    break;
                case QueryType::SINGLE:
                    ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " >= " << range.first << " AND "
                       << FILE_ROW_NUMBER_CLAUSE << " < " << range.second;
                    break;
                case QueryType::MIDDLE:
                    // No WHERE clause needed
                    break;
            }
            break;
        default:
            throw NotImplementedException("Unsupported file format");
    }
    ss << ";";
    return ss.str();
}

}  // namespace duckdb
