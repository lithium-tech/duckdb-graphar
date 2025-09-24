#include "utils/query_string_constructor.hpp"

namespace duckdb {

std::string QueryStringConstructor::GetMainQueryString(const vector<std::string>& column_names,
                                                       const vector<LogicalType>& column_types,
                                                       const vector<column_t>& projected_inds,
                                                       QueryType query_type) {
    std::ostringstream ss;
    ss << SQL_SELECT_CLAUSE << " ";
    switch (file_type) {
        case graphar::FileType::PARQUET:
            for (idx_t i = 0; i + 1 < projected_inds.size(); ++i) {
                ss << "#" << to_string(projected_inds[i] + 1) << ", ";
            }
            ss << "#" << to_string(projected_inds.back() + 1);
            ss << SQL_FROM_CLAUSE << " ";
            ss << READ_PARQUET_FUNCTION;
            ss << "($1, " << FILE_ROW_NUMBER_CLAUSE << "=true)";
            if (query_type == QueryType::FIRST) {
                ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " >= $2";
            } else if (query_type == QueryType::LAST) {
                ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " < $2";
            } else if (query_type == QueryType::SINGLE) {
                ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " >= $2 AND "
                   << FILE_ROW_NUMBER_CLAUSE << " < $3";
            }
            break;
        case graphar::FileType::CSV:
            for (idx_t i = 0; i + 1 < projected_inds.size(); ++i) {
                ss << "#" << to_string(projected_inds[i] + 1) << ", ";
            }
            ss << "#" << to_string(projected_inds.back() + 1);
            ss << " " << SQL_FROM_CLAUSE << " ";
            ss << READ_CSV_FUNCTION;
            ss << "($1, columns={";
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
            ss << "*" << " " << SQL_FROM_CLAUSE << " ";
            ss << READ_JSON_FUNCTION;
            ss << "($1, columns={";
            for (idx_t i = 0; i < projected_inds.size(); i++) {
                ss << "'" << column_names[projected_inds[i]] << "': '" << column_types[projected_inds[i]].ToString() << "'";
                if (i != projected_inds.size() - 1) {
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
        case graphar::FileType::ORC:
            throw NotImplementedException("ORC file format is not supported yet");
        default:
            throw NotImplementedException("Unknown file format");
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
            ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " " << SQL_BETWEEN_CLAUSE
               << " $2 AND ($2 + 1)";
            break;
        case graphar::FileType::CSV:
            ss << READ_CSV_FUNCTION;
            ss << "($1)";
            ss << " " << SQL_OFFSET_CLAUSE << " $2" << " " << SQL_LIMIT_CLAUSE << " 2";
            break;
        case graphar::FileType::JSON:
            ss << READ_JSON_FUNCTION;
            ss << "($1)";
            ss << " " << SQL_OFFSET_CLAUSE << " $2" << " " << SQL_LIMIT_CLAUSE << " 2";
            break;
        case graphar::FileType::ORC:
            throw NotImplementedException("ORC file format is not supported yet");
        default:
            throw NotImplementedException("Unknown file type");
    }
    ss << ";";
    return ss.str();
}

}  // namespace duckdb