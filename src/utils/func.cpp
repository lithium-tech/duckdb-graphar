#include "utils/func.hpp"

#include <iostream>

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "graphar/expression.h"
#include "graphar/filesystem.h"
#include "graphar/graph_info.h"
#include "graphar/types.h"
#include "utils/global_log_manager.hpp"

namespace duckdb {

LogicalTypeId GraphArFunctions::graphArT2duckT(const std::string& name) {
    if (name == "bool") {
        return LogicalTypeId::BOOLEAN;
    } else if (name == "int32") {
        return LogicalTypeId::INTEGER;
    } else if (name == "int64") {
        return LogicalTypeId::BIGINT;
    } else if (name == "float") {
        return LogicalTypeId::FLOAT;
    } else if (name == "double") {
        return LogicalTypeId::DOUBLE;
    } else if (name == "string") {
        return LogicalTypeId::VARCHAR;
    }
    throw NotImplementedException("Unsupported type");
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

std::shared_ptr<graphar::Expression> GraphArFunctions::GetFilter(const std::string filter_type,
                                                                 const std::string filter_value,
                                                                 const std::string filter_column) {
    if (filter_type == "string") {
        return graphar::_Equal(graphar::_Property(filter_column),
                               graphar::_Literal(filter_value.substr(1, filter_value.size() - 2)));
    } else if (filter_type == "int32") {
        return graphar::_Equal(graphar::_Property(filter_column), graphar::_Literal(std::stoi(filter_value)));
    } else if (filter_type == "int64") {
        // Bug: stoll -> long long int, need only int64_t == long long
        return graphar::_Equal(graphar::_Property(filter_column),
                               graphar::_Literal((int64_t)(std::stoll(filter_value))));
    } else if (filter_type == "float") {
        return graphar::_Equal(graphar::_Property(filter_column), graphar::_Literal(std::stof(filter_value)));
    } else if (filter_type == "double") {
        return graphar::_Equal(graphar::_Property(filter_column), graphar::_Literal(std::stod(filter_value)));
    }
    throw NotImplementedException("Unsupported filter type");
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

std::int64_t GetVertexCount(const std::shared_ptr<graphar::EdgeInfo>& edge_info, std::string& directory) {
    std::string vertex_num_path = edge_info->GetVerticesNumFilePath(graphar::AdjListType::ordered_by_source).value();

    return GetCount(directory + vertex_num_path);
}

}  // namespace duckdb
