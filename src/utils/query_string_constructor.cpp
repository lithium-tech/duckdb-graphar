#include "utils/query_string_constructor.hpp"

namespace duckdb {

std::string QueryStringConstructor::GetMainQueryString(vector<std::string> &column_names, vector<LogicalType>& column_types, std::string& projected_columns_string, vector<std::string>& id_columns, idx_t reader_i, idx_t columns_to_remove, QueryType query_type) {
    std::ostringstream ss;
    ss << SQL_SELECT_CLAUSE << " " << projected_columns_string << " " << SQL_FROM_CLAUSE << " ";
    switch (file_type) {
        case graphar::FileType::PARQUET:
            ss << READ_PARQUET_FUNCTION;
            ss << "($1, " << FILE_ROW_NUMBER_CLAUSE << "=true)";
            if (query_type == QueryType::FIRST) {
                ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " >= $2";
            } else if (query_type == QueryType::LAST) {
                ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " < $2";
            } else if (query_type == QueryType::SINGLE) {
                ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " >= $2 AND " << FILE_ROW_NUMBER_CLAUSE
                << " < $3";
            }
            break;
        case graphar::FileType::CSV:
            ss << READ_CSV_FUNCTION;
            ss << "($1, columns={";
            if (reader_i != 0) {
                for (idx_t i = 0; i < columns_to_remove; i++) {
                    ss << "'" << id_columns[i] << "': 'BIGINT', ";
                }
            }
            for (idx_t i = 0; i < column_names.size(); i++) {
                ss << "'" << column_names[i] << "': '" << column_types[i].ToString() << "'";
                if (i != column_names.size() - 1) {
                    ss << ", ";
                }
            }
            ss << "})";
            if (query_type == QueryType::FIRST) {
                ss << " " << SQL_OFFSET_CLAUSE << " $2";
            } else if (query_type == QueryType::LAST) {
                ss << " " << SQL_LIMIT_CLAUSE << " $2";
            } else if (query_type == QueryType::SINGLE) {
                ss << " " << SQL_OFFSET_CLAUSE << " $2" << " " << SQL_LIMIT_CLAUSE << " ($3 - $2)";
            }
            break;
        case graphar::FileType::JSON:
            throw NotImplementedException("JSON file format is not supported yet");
            break;
        case graphar::FileType::ORC:
            throw NotImplementedException("ORC file format is not supported yet");
            break;
        default:
            throw NotImplementedException("Unknown file type");
    }
    ss << ";";
    return ss.str();
};

std::string QueryStringConstructor::GetGrapharOffsetQueryString() {
    std::ostringstream ss;
    ss << SQL_SELECT_CLAUSE << " " << "*" << " " << SQL_FROM_CLAUSE << " ";
    switch (file_type) {
        case graphar::FileType::PARQUET:
            ss << READ_PARQUET_FUNCTION;
            ss << "($1, " << FILE_ROW_NUMBER_CLAUSE << "=true)";
            ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " " << SQL_BETWEEN_CLAUSE << " $2 AND ($2 + 1)";
            break;
        case graphar::FileType::CSV:
            ss << READ_CSV_FUNCTION;
            ss << "($1)";
            ss << " " << SQL_OFFSET_CLAUSE << " $2" << " " << SQL_LIMIT_CLAUSE << " 2";
            break;
        case graphar::FileType::JSON:
            throw NotImplementedException("JSON file format is not supported yet");
        case graphar::FileType::ORC:
            throw NotImplementedException("ORC file format is not supported yet");
        default:
            throw NotImplementedException("Unknown file type");
    }
    ss << ";";
    return ss.str();
}

}  // namespace duckdb