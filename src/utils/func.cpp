#include "utils/func.hpp"

#include "utils/global_log_manager.hpp"
#include "utils/type_info.hpp"

#include <arrow/c/bridge.h>

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>

#include <graphar/expression.h>
#include <graphar/filesystem.h>
#include <graphar/graph_info.h>
#include <graphar/types.h>

#include <duckdb.hpp>
#include <iostream>

namespace duckdb {

LogicalTypeId GraphArFunctions::graphArT2duckT(const std::string& name) {
    if (name == "bool") return LogicalTypeId::BOOLEAN;
    if (name == "int32") return LogicalTypeId::INTEGER;
    if (name == "int64") return LogicalTypeId::BIGINT;
    if (name == "float") return LogicalTypeId::FLOAT;
    if (name == "double") return LogicalTypeId::DOUBLE;
    if (name == "string") return LogicalTypeId::VARCHAR;
    if (name == "date") return LogicalTypeId::DATE;
    if (name == "timestamp") return LogicalTypeId::TIMESTAMP;
    if (name == "timestamp_tz") return LogicalTypeId::TIMESTAMP_TZ;

    throw NotImplementedException("Unsupported type for conversion to duck: " + name);
}

std::shared_ptr<arrow::DataType> GraphArFunctions::graphArT2arrowT(const std::string& name) {
    if (name == "bool") return arrow::boolean();
    if (name == "int32") return arrow::int32();
    if (name == "int64") return arrow::int64();
    if (name == "float") return arrow::float32();
    if (name == "double") return arrow::float64();
    if (name == "string") return arrow::utf8();
    if (name == "date") return arrow::date64();
    if (name == "timestamp") return arrow::timestamp(arrow::TimeUnit::MILLI);

    throw NotImplementedException("Unsupported type for conversion to arrow: " + name);
}

unique_ptr<ArrowTypeInfo> GraphArFunctions::graphArT2ArrowTypeInfo(const std::string& name) {
    if (name == "string") {
        return make_uniq<ArrowTypeInfo>(ArrowTypeInfoType::STRING);
    } else {
        return nullptr;
    }
}

Value GraphArFunctions::ArrowScalar2DuckValue(const std::shared_ptr<arrow::Scalar>& scalar) {
    DUCKDB_GRAPHAR_LOG_WARN("ArrowScalar2DuckValue");
    if (!scalar->is_valid) {
        return Value();
    }

    switch (scalar->type->id()) {
        case arrow::Type::BOOL:
            return Value::BOOLEAN(static_cast<const arrow::BooleanScalar&>(*scalar).value);
        case arrow::Type::INT32:
            return Value::INTEGER(static_cast<const arrow::Int32Scalar&>(*scalar).value);
        case arrow::Type::INT64:
            return Value::BIGINT(static_cast<const arrow::Int64Scalar&>(*scalar).value);
        case arrow::Type::FLOAT:
            return Value::FLOAT(static_cast<const arrow::FloatScalar&>(*scalar).value);
        case arrow::Type::DOUBLE:
            return Value::DOUBLE(static_cast<const arrow::DoubleScalar&>(*scalar).value);
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
            return Value(static_cast<const arrow::StringScalar&>(*scalar).value->ToString());
        case arrow::Type::DATE64:
            return Value::DATE(date_t(static_cast<const arrow::Date64Scalar&>(*scalar).value));
        case arrow::Type::TIMESTAMP: {
            return Value::TIMESTAMP(timestamp_t(static_cast<const arrow::TimestampScalar&>(*scalar).value));
        }
        default:
            throw duckdb::NotImplementedException("Arrow scalar type not supported: " + scalar->type->ToString());
    }
}

template <typename Info>
std::string GraphArFunctions::GetNameFromInfo(const Info& info) {
    throw InternalException("Unsupported info");
}

template <>
std::string GraphArFunctions::GetNameFromInfo(const std::shared_ptr<graphar::VertexInfo>& info) {
    return info->GetType();
}

template <>
std::string GraphArFunctions::GetNameFromInfo(const std::shared_ptr<graphar::EdgeInfo>& info) {
    return info->GetSrcType() + "_" + info->GetEdgeType() + "_" + info->GetDstType();
}

std::shared_ptr<graphar::Expression> GraphArFunctions::GetFilter(const std::string& filter_type,
                                                                 const std::string& filter_value,
                                                                 const std::string& filter_column) {
    if (filter_type == "int32") {
        return graphar::_Equal(graphar::_Property(filter_column), graphar::_Literal(std::stoi(filter_value)));
    }
    if (filter_type == "int64") {
        // Bug: stoll -> long long int, need only int64_t == long long
        return graphar::_Equal(graphar::_Property(filter_column),
                               graphar::_Literal((int64_t)(std::stoll(filter_value))));
    }
    if (filter_type == "string") {
        return graphar::_Equal(graphar::_Property(filter_column),
                               graphar::_Literal(filter_value.substr(1, filter_value.size() - 2)));
    }
    if (filter_type == "float") {
        return graphar::_Equal(graphar::_Property(filter_column), graphar::_Literal(std::stof(filter_value)));
    }
    if (filter_type == "double") {
        return graphar::_Equal(graphar::_Property(filter_column), graphar::_Literal(std::stod(filter_value)));
    }
    // TODO: bool?

    throw NotImplementedException("Unsupported filter type: " + filter_type);
}

std::string GetYamlContent(const std::string& path) {
    std::string no_url_path;
    auto fs = graphar::FileSystemFromUriOrPath(path, &no_url_path).value();
    std::string yaml_content = fs->ReadFileToValue<std::string>(no_url_path).value();
    return yaml_content;
}

void ConvertArrowTableToDataChunk(const arrow::Table& table, DataChunk& output, const std::vector<column_t>& column_ids,
                                  ClientContext& context) {
    auto schema = table.schema();

    ArrowSchema c_schema;
    if (auto export_schema_status = arrow::ExportSchema(*schema, &c_schema); !export_schema_status.ok()) {
        throw std::runtime_error("Failed to export schema: " + export_schema_status.message());
    }

    ArrowTableSchema arrow_table_schema;
    ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table_schema, c_schema);

    if (output.ColumnCount() == 0) {
        vector<LogicalType> types;
        types.reserve(column_ids.size());
        for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
            auto& arrow_type = *arrow_table_schema.GetColumns().at(column_ids[col_idx]);
            types.emplace_back(arrow_type.GetDuckType());
        }
        output.Initialize(context, types, table.num_rows());
    }

    const auto num_rows = table.num_rows();
    output.SetCapacity(num_rows);
    output.SetCardinality(num_rows);
    for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
        auto& arrow_type = *arrow_table_schema.GetColumns().at(column_ids[col_idx]);
        if (arrow_type.GetDuckType().id() == LogicalTypeId::VARCHAR) {
            for (idx_t row_i = 0; row_i < num_rows; row_i++) {
                auto maybe_value = table.column(column_ids[col_idx])->GetScalar(row_i);
                if (!maybe_value.ok()) {
                    throw std::runtime_error("Failed to get value from table: " + maybe_value.status().ToString());
                }
                auto value = maybe_value.ValueUnsafe();
                auto duckdb_value = GraphArFunctions::ArrowScalar2DuckValue(value);
                output.SetValue(col_idx, row_i, duckdb_value);
            }
            continue;
        }

        auto arrow_column = table.column(column_ids[col_idx]);

        auto flatten_result = arrow::Concatenate(arrow_column->chunks());
        if (!flatten_result.ok()) {
            throw std::runtime_error("Failed to flatten Arrow column");
        }
        auto arrow_array = flatten_result.ValueUnsafe();

        ArrowArray c_array;
        auto export_array_status = arrow::ExportArray(*arrow_array, &c_array);
        if (!export_array_status.ok()) {
            throw std::runtime_error("Failed to export Arrow array: " + export_array_status.message());
        }

        ArrowArrayScanState array_state(context);
        array_state.owned_data = make_shared_ptr<ArrowArrayWrapper>();
        array_state.owned_data->arrow_array = std::move(c_array);

        ArrowToDuckDBConversion::SetValidityMask(output.data[col_idx], array_state.owned_data->arrow_array, 0,
                                                 output.size(), 0, -1);

        ArrowToDuckDBConversion::ColumnArrowToDuckDB(output.data[col_idx], array_state.owned_data->arrow_array, 0,
                                                     array_state, output.size(), arrow_type);
    }
}

}  // namespace duckdb
