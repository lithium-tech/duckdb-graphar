#include <graphar/fwd.h>

#include <duckdb.hpp>

namespace duckdb {

class QueryStringConstructor {
public:
    enum class QueryType { FIRST, MIDDLE, LAST, SINGLE };
    std::string GetMainQueryString(const vector<std::string>& column_names, const vector<LogicalType>& column_types,
                                   const std::string& projected_columns_string, const vector<std::string>& id_columns,
                                   idx_t reader_i, idx_t columns_to_remove, QueryType query_type);

    std::string GetGrapharOffsetQueryString();

    void SetFileType(graphar::FileType file_type_) { file_type = file_type_; }

private:
    static constexpr std::string_view SQL_SELECT_CLAUSE = "SELECT";
    static constexpr std::string_view SQL_FROM_CLAUSE = "FROM";
    static constexpr std::string_view SQL_WHERE_CLAUSE = "WHERE";
    static constexpr std::string_view SQL_BETWEEN_CLAUSE = "BETWEEN";
    static constexpr std::string_view SQL_LIMIT_CLAUSE = "LIMIT";
    static constexpr std::string_view SQL_OFFSET_CLAUSE = "OFFSET";
    static constexpr std::string_view READ_PARQUET_FUNCTION = "read_parquet";
    static constexpr std::string_view READ_CSV_FUNCTION = "read_csv";
    static constexpr std::string_view READ_JSON_FUNCTION = "read_json";
    static constexpr std::string_view FILE_ROW_NUMBER_CLAUSE = "file_row_number";

private:
    graphar::FileType file_type = graphar::FileType::PARQUET;
};

}  // namespace duckdb