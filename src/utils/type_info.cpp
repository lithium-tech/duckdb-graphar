#include "utils/type_info.hpp"

namespace duckdb {

std::unordered_map<std::string, int64_t> duckdb::GetCountClass::count_cache;
std::mutex duckdb::GetCountClass::count_cache_mutex;

int64_t GetCountClass::GetCount(const TypeInfoPtr& type_info, const std::string& graph_prefix) {
    std::lock_guard<std::mutex> lock(count_cache_mutex);
    DUCKDB_GRAPHAR_LOG_TRACE("GetCount");
    auto name = std::visit([&](auto& t) { return GraphArFunctions::GetNameFromInfo(t); }, type_info);
    if (count_cache.find(name) != count_cache.end()) {
        return count_cache[name];
    }
    return count_cache[name] = std::visit(
               [&graph_prefix](auto& type_info) {
                   if constexpr (std::is_same_v<std::decay_t<decltype(type_info)>,
                                                std::shared_ptr<graphar::VertexInfo>>) {
                       GAR_ASSIGN_OR_RAISE_ERROR(auto num_file_path, type_info->GetVerticesNumFilePath());
                       num_file_path = graph_prefix + num_file_path;
                       GAR_ASSIGN_OR_RAISE_ERROR(auto fs, graphar::FileSystemFromUriOrPath(num_file_path));
                       GAR_ASSIGN_OR_RAISE_ERROR(auto vertex_num,
                                                 fs->template ReadFileToValue<graphar::IdType>(num_file_path));
                       return vertex_num;
                   } else {
                       for (const auto& adj_list_type : all_adj_list_types) {
                           if (!type_info->HasAdjacentListType(adj_list_type)) {
                               continue;
                           }
                           GAR_ASSIGN_OR_RAISE_ERROR(auto vertices_num_file_path,
                                                     type_info->GetVerticesNumFilePath(adj_list_type));
                           vertices_num_file_path = graph_prefix + vertices_num_file_path;
                           GAR_ASSIGN_OR_RAISE_ERROR(auto fs, graphar::FileSystemFromUriOrPath(vertices_num_file_path));
                           GAR_ASSIGN_OR_RAISE_ERROR(
                               auto vertex_num, fs->template ReadFileToValue<graphar::IdType>(vertices_num_file_path));
                           const auto vertex_chunk_size = type_info->GetSrcChunkSize();
                           const auto vertex_chunk_count = (vertex_num + vertex_chunk_size - 1) / vertex_chunk_size;
                           int64_t edge_num = 0;
                           for (int64_t i = 0; i < vertex_chunk_count; ++i) {
                               GAR_ASSIGN_OR_RAISE_ERROR(auto chunk_path,
                                                         type_info->GetEdgesNumFilePath(i, adj_list_type));
                               chunk_path = graph_prefix + chunk_path;
                               GAR_ASSIGN_OR_RAISE_ERROR(auto chunk_edge_num,
                                                         fs->template ReadFileToValue<graphar::IdType>(chunk_path));
                               edge_num += chunk_edge_num;
                           }
                           return edge_num;
                       }
                       throw std::runtime_error("No adj list type found");
                   }
               },
               type_info);
}

}  // namespace duckdb