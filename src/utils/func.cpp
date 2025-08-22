#include "utils/func.hpp"

#include "utils/global_log_manager.hpp"

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>

#include <graphar/expression.h>
#include <graphar/filesystem.h>
#include <graphar/graph_info.h>
#include <graphar/types.h>

#include <duckdb.hpp>
#include <iostream>

namespace duckdb {

LogicalTypeId GraphArFunctions::graphArT2duckT(const std::string& name) {
    if (name == "int32") return LogicalTypeId::INTEGER;
    if (name == "int64") return LogicalTypeId::BIGINT;
    if (name == "string") return LogicalTypeId::VARCHAR;
    if (name == "float") return LogicalTypeId::FLOAT;
    if (name == "double") return LogicalTypeId::DOUBLE;
    if (name == "bool") return LogicalTypeId::BOOLEAN;
    if (name == "date") return LogicalTypeId::DATE;

    throw NotImplementedException("Unsupported type: " + name);
}

std::shared_ptr<arrow::DataType> GraphArFunctions::graphArT2arrowT(const std::string& name) {
    if (name == "int32") return arrow::int32();
    if (name == "int64") return arrow::int64();
    if (name == "string") return arrow::utf8();
    if (name == "float") return arrow::float32();
    if (name == "double") return arrow::float64();
    if (name == "bool") return arrow::boolean();
    if (name == "date") return arrow::date64();

    throw NotImplementedException("Unsupported type: " + name);
}

unique_ptr<ArrowTypeInfo> GraphArFunctions::graphArT2ArrowTypeInfo(const std::string& name) {
    if (name == "string") {
        return make_uniq<ArrowTypeInfo>(ArrowTypeInfoType::STRING);
    } else {
        return nullptr;
    }
}

template <typename Info>
std::string GraphArFunctions::GetNameFromInfo(const std::shared_ptr<Info>& info) {
    throw InternalException("Unsupported info");
}

template <>
std::string GraphArFunctions::GetNameFromInfo(const std::shared_ptr<graphar::VertexInfo>& info) {
    return info->GetType() + ".vertex";
}

template <>
std::string GraphArFunctions::GetNameFromInfo(const std::shared_ptr<graphar::EdgeInfo>& info) {
    return info->GetSrcType() + "_" + info->GetEdgeType() + "_" + info->GetDstType() + ".edge";
}

int64_t GraphArFunctions::GetVertexNum(std::shared_ptr<graphar::GraphInfo> graph_info, std::string& type) {
    auto vertex_info = graph_info->GetVertexInfo(type);
    GAR_ASSIGN_OR_RAISE_ERROR(auto num_file_path, vertex_info->GetVerticesNumFilePath());
    num_file_path = graph_info->GetPrefix() + num_file_path;
    GAR_ASSIGN_OR_RAISE_ERROR(auto fs, graphar::FileSystemFromUriOrPath(num_file_path));
    GAR_ASSIGN_OR_RAISE_ERROR(auto vertex_num, fs->ReadFileToValue<graphar::IdType>(num_file_path));
    return vertex_num;
}

graphar::Result<std::shared_ptr<arrow::Schema>> GraphArFunctions::NamesAndTypesToArrowSchema(
    const vector<std::string>& names, const vector<std::string>& types) {
    DUCKDB_GRAPHAR_LOG_TRACE("NamesAndTypesToArrowSchema");
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (idx_t i = 0; i < names.size(); ++i) {
        fields.push_back(std::make_shared<arrow::Field>(names[i], graphArT2arrowT(types[i])));
    }
    DUCKDB_GRAPHAR_LOG_TRACE("NamesAndTypesToArrowSchema: returning...");
    return arrow::schema(fields);
}

std::shared_ptr<arrow::Table> GraphArFunctions::EmptyTableFromNamesAndTypes(const vector<std::string>& names,
                                                                            const vector<std::string>& types) {
    auto maybe_schema = NamesAndTypesToArrowSchema(names, types);
    if (maybe_schema.has_error()) {
        throw InternalException(maybe_schema.error().message());
    }
    auto maybe_table = arrow::Table::MakeEmpty(maybe_schema.value());
    if (!maybe_table.ok()) {
        throw InternalException(maybe_table.status().message());
    }
    return maybe_table.ValueUnsafe();
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

// Function from incubator-graphar
std::string GetDirectory(const std::string& path) {
    if (path.rfind("s3://", 0) == 0) {
        int t = path.find_last_of('?');
        std::string prefix = path.substr(0, t);
        std::string suffix = path.substr(t);
        const size_t last_slash_idx = prefix.rfind('/');
        if (std::string::npos != last_slash_idx) {
            return prefix.substr(0, last_slash_idx + 1) + suffix;
        }
    } else {
        const size_t last_slash_idx = path.rfind('/');
        if (std::string::npos != last_slash_idx) {
            return path.substr(0, last_slash_idx + 1);  // +1 to include the slash
        }
    }
    return path;
}

std::int64_t GetCount(const std::string& path) {
    std::string no_url_path;
    auto fs = graphar::FileSystemFromUriOrPath(path, &no_url_path).value();
    return fs->ReadFileToValue<graphar::IdType>(path).value();
}

std::int64_t GetVertexCount(const std::shared_ptr<graphar::EdgeInfo>& edge_info, const std::string& directory) {
    std::string vertex_num_path = edge_info->GetVerticesNumFilePath(graphar::AdjListType::ordered_by_source).value();

    return GetCount(directory + vertex_num_path);
}

}  // namespace duckdb
