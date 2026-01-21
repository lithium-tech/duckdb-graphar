#pragma once

#include "utils/global_log_manager.hpp"

#include <arrow/api.h>

#include <graphar/api/info.h>

namespace duckdb {

class OffsetReader {
public:
    OffsetReader(const std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& prefix,
                 graphar::AdjListType adj_list_type)
        : edge_info(edge_info), prefix(prefix), adj_list_type(adj_list_type), current_offset_chunk_index(-1) {
        DUCKDB_GRAPHAR_LOG_TRACE("OffsetReader::Constructor");

        if (adj_list_type == graphar::AdjListType::ordered_by_source) {
            vertex_chunk_size = edge_info->GetSrcChunkSize();
        } else if (adj_list_type == graphar::AdjListType::ordered_by_dest) {
            vertex_chunk_size = edge_info->GetDstChunkSize();
        } else {
            throw std::runtime_error("The adj list type has to be ordered_by_source or ordered_by_dest, but got " +
                                     std::string(graphar::AdjListTypeToString(adj_list_type)));
        }

        fs = graphar::FileSystemFromUriOrPath(prefix, &out_prefix).value();
        auto adjacent_list = edge_info->GetAdjacentList(adj_list_type);
        if (adjacent_list == nullptr) {
            throw std::runtime_error("The adjacent list is not set for adj list type " +
                                     std::string(graphar::AdjListTypeToString(adj_list_type)));
        }

        file_type = adjacent_list->GetFileType();
    }

    void OpenTable(graphar::IdType vid) {
        DUCKDB_GRAPHAR_LOG_TRACE("OffsetReader::OpenTable");
        graphar::IdType offset_chunk_index = vid / vertex_chunk_size;

        if (offset_chunk_index != current_offset_chunk_index) {
            auto offset_file_path = edge_info->GetAdjListOffsetFilePath(offset_chunk_index, adj_list_type).value();

            std::string path = out_prefix + offset_file_path;

            current_table = fs->ReadFileToTable(path, file_type).value();
        }
    }

    std::pair<graphar::IdType, graphar::IdType> GetOffset(graphar::IdType vid) {
        DUCKDB_GRAPHAR_LOG_TRACE("OffsetReader::GetOffset");
        OpenTable(vid);

        graphar::IdType offset_in_file = vid % vertex_chunk_size;

        auto array =
            std::static_pointer_cast<arrow::Int64Array>(current_table->column(0)->Slice(offset_in_file, 2)->chunk(0));

        return std::make_pair(static_cast<graphar::IdType>(array->Value(0)),
                              static_cast<graphar::IdType>(array->Value(1)));
    }

public:
    graphar::IdType vertex_chunk_size;
    const std::shared_ptr<graphar::EdgeInfo> edge_info;
    const std::string& prefix;
    const graphar::AdjListType adj_list_type;
    std::shared_ptr<arrow::Table> current_table;
    std::shared_ptr<graphar::FileSystem> fs;
    std::string out_prefix;
    graphar::IdType current_offset_chunk_index;
    graphar::FileType file_type;
};
}  // namespace duckdb