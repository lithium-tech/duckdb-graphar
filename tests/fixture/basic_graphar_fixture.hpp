#pragma once
#include <catch2/catch_test_macros.hpp>

#include "duckdb.hpp"

#include <string>

#include <graphar/api/high_level_writer.h>
#include <graphar/graph_info.h>
#include <graphar/status.h>


#include <cassert>
#include <iostream>
#include <sstream>
#include <variant>
#include <fstream>
#include <filesystem>
#include <atomic>
#include <concepts>

using PropertyValue = std::variant<int32_t, int64_t, float, double, std::string, bool>;

struct PropertySchema{
    std::string name;
    std::string data_type;
    bool is_nullable;
    bool is_primary;

    PropertySchema(std::string name, std::string data_type, bool is_nullable, bool is_primary)
        : name(name), data_type(data_type), is_nullable(is_nullable), is_primary(is_primary) {}
};
struct EdgeData {
    int64_t src;
    int64_t dst;
    std::unordered_map<std::string, PropertyValue> properties;

    EdgeData(int64_t src, int64_t dst, std::unordered_map<std::string, PropertyValue> properties)
        : src(src), dst(dst), properties(properties) {}
    EdgeData(int64_t src, int64_t dst)
        : src(src), dst(dst) {}
    EdgeData() = default;
};
struct EdgesSchema {
    std::string src_type;
    std::string type;
    std::string dst_type;
    graphar::IdType chunk_size;
    bool directed;
    std::vector<PropertySchema> properties;
    std::vector<EdgeData> edges;
    graphar::IdType num_vertices;

    EdgesSchema(std::string src_type, std::string type, std::string dst_type, graphar::IdType chunk_size, bool directed, std::vector<PropertySchema> properties, std::vector<EdgeData> edges, graphar::IdType num_vertices)
        : src_type(src_type), type(type), dst_type(dst_type), chunk_size(chunk_size), directed(directed), properties(properties), edges(edges), num_vertices(num_vertices) {}
};
struct VertexData {
    int64_t ind;
    std::unordered_map<std::string, PropertyValue> properties;

    VertexData(int64_t ind, std::unordered_map<std::string, PropertyValue> properties)
        : ind(ind), properties(properties) {}
    VertexData(int64_t ind)
        : ind(ind) {}
    VertexData() = default;
};
struct VerticesSchema {
    std::string type;
    graphar::IdType chunk_size;
    std::vector<PropertySchema> properties;
    std::vector<VertexData> vertices;

    VerticesSchema(std::string type, graphar::IdType chunk_size, std::vector<PropertySchema> properties, std::vector<VertexData> vertices)
        : type(type), chunk_size(chunk_size), properties(properties), vertices(vertices) {}
};


template <typename T>
requires (std::same_as<T, graphar::builder::Vertex> || std::same_as<T, graphar::builder::Edge>)
static void FillProperties(T& obj, const std::vector<PropertySchema>& propeties_schema, const std::unordered_map<std::string, PropertyValue>& properties_data) {
    for (auto ind_p = 0; ind_p < propeties_schema.size(); ++ind_p){
        const auto& prop_schema = propeties_schema[ind_p];
        auto it = properties_data.find(prop_schema.name);
        if (it == properties_data.end()) {
            throw std::runtime_error("Property not found in data: " + prop_schema.name);
        }
        const auto& prop_val = it->second;

        if (prop_schema.data_type == "int32") {
            if (std::holds_alternative<int32_t>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<int32_t>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected int32");
            }
        }
        else if (prop_schema.data_type == "int64") {
            if (std::holds_alternative<int64_t>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<int64_t>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected int64");
            }
        }
        else if (prop_schema.data_type == "float") {
            if (std::holds_alternative<float>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<float>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected float");
            }
        }
        else if (prop_schema.data_type == "double") {
            if (std::holds_alternative<double>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<double>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected double");
            }
        }
        else if (prop_schema.data_type == "string") {
            if (std::holds_alternative<std::string>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<std::string>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected string");
            }
        }
        else if (prop_schema.data_type == "bool") {
            if (std::holds_alternative<bool>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<bool>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected bool");
            }
        }
        else {
            throw std::runtime_error("Unsupported data type: " + prop_schema.data_type);
        }
    } 
}

struct FileTypeJson { FileTypeJson(){}; };
struct FileTypeOrc { FileTypeOrc(){}; };
struct FileTypeParquet { FileTypeParquet(){}; };
struct FileTypeCsv { FileTypeCsv(){}; };

#define FILE_TYPES_FOR_TEST FileTypeCsv,FileTypeParquet

namespace {
    const std::string GraphVersion = "gar/v1";
    const std::string VertexPathPrefix = "vertex/";
    const std::string EdgePathPrefix = "edge/";
    const std::string PrefixToRemove = "prefix:";
    const std::string GraphFileExtension = ".graph.yaml";
    const std::string VertexFileExtension = ".vertex.yaml";
    const std::string EdgeFileExtension = ".edge.yaml";
}

template <typename FileTypeTag> 
class BasicGrapharFixture { 
private:
    inline static std::atomic<size_t> num_graph = 0;
    std::filesystem::path tmp_folder;
    std::vector<std::filesystem::path> graph_folders;
    static constexpr graphar::FileType GetFileType() {
        if constexpr (std::is_same_v<FileTypeTag, FileTypeParquet>) return graphar::FileType::PARQUET;
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeCsv>) return graphar::FileType::CSV;
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeOrc>) return graphar::FileType::ORC;
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeJson>) return graphar::FileType::JSON;
        else throw std::logic_error("Unsupported file type tag");
    };
    static constexpr std::string GetFileTypeName() {
        if constexpr (std::is_same_v<FileTypeTag, FileTypeParquet>) return "parquet";
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeCsv>) return "csv";
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeOrc>) return "orc";
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeJson>) return "json";
        else throw std::logic_error("Unsupported file type tag");
    };
    bool RemovePrefix(const std::string& path) const noexcept{        
        struct Guard {
            std::string tmp_path;
            bool cleanup = false;

            Guard(const std::string& path) : tmp_path(path + ".tmp") {}
            ~Guard() {
                if (cleanup) std::filesystem::remove(tmp_path);
            }
        } guard{path}; 
        
        bool removed = false;  
        
        {
            std::ifstream in(path);
            if (!in) return false;

            std::ofstream out(guard.tmp_path, std::ios::trunc);
            if (!out) return false;
            
            guard.cleanup = true;

            std::string line;
            while (std::getline(in, line)) {
                if (line.find(PrefixToRemove) == std::string::npos) {
                    out << line << '\n';
                } else {
                    removed = true;
                }
            }
        }

        if (removed) {
            std::error_code ec;  
            std::filesystem::rename(guard.tmp_path, path, ec);  
            if (!ec) {  
                guard.cleanup = false;
                return true;  
            }
        } 
        return false;
    }
protected:
    duckdb::DuckDB db;
    duckdb::Connection conn;
    
    std::string CreateTestGraph(
            const std::string& graph_name, 
            const std::vector<VerticesSchema>& vertices_list,
            const std::vector<EdgesSchema>& edges_list 
        ){
        std::filesystem::path graph_folder;
        do {
            graph_folder = tmp_folder / GetFileTypeName() / (std::to_string(num_graph++) + "_" + graph_name);
        } while(std::filesystem::exists(graph_folder));
        
        graph_folders.push_back(graph_folder);
        const std::string output_path = graph_folder.string() + "/";
        const std::string graph_path = output_path + graph_name + GraphFileExtension;
        INFO("Creating graph: " + graph_name + " in " + graph_path);
        auto graph_info = graphar::GraphInfo::Load(graph_path).value_or(nullptr);
        if (graph_info != nullptr) {
            throw std::runtime_error("Graph already exists");
        }

        auto version = graphar::InfoVersion::Parse(GraphVersion).value();

        // Vertex Info
        std::unordered_map<std::string, const VerticesSchema*> type_schema;
        graphar::VertexInfoVector vertex_infos = {};
        for (const auto& vertices_schema: vertices_list){
            std::vector<graphar::Property> properties;
            for (const auto& prop_schema: vertices_schema.properties){
                properties.push_back(graphar::Property(
                    prop_schema.name, graphar::DataType::TypeNameToDataType(prop_schema.data_type),
                    prop_schema.is_primary, prop_schema.is_nullable)
                );
            }
            const graphar::PropertyGroupVector pgs = {
                std::make_shared<graphar::PropertyGroup>(properties, GetFileType(), "")
            };
            vertex_infos.push_back(graphar::CreateVertexInfo(
                vertices_schema.type, vertices_schema.chunk_size,
                pgs, {}, 
                VertexPathPrefix + vertices_schema.type + "/", version));
            REQUIRE(!vertex_infos.back()->Dump().has_error());
            REQUIRE(vertex_infos.back()->Save(output_path + vertices_schema.type + VertexFileExtension).ok());
            type_schema[vertices_schema.type] = &vertices_schema;
        }

        // Edge Info
        graphar::EdgeInfoVector edges_infos = {};

        const auto adjacent_types = {graphar::AdjListType::ordered_by_source, graphar::AdjListType::ordered_by_dest};
        graphar::AdjacentListVector adjacent_lists = {};
        for (const auto& adjacent_type : adjacent_types){ 
            adjacent_lists.push_back(graphar::CreateAdjacentList(adjacent_type, GetFileType())); 
        }

        for (const auto& edges_schema: edges_list){

            std::vector<graphar::Property> properties;
            for (const auto& prop_schema: edges_schema.properties){
                properties.push_back(graphar::Property(
                    prop_schema.name, graphar::DataType::TypeNameToDataType(prop_schema.data_type),
                    prop_schema.is_primary, prop_schema.is_nullable)
                );
            }
            graphar::PropertyGroupVector pgs = {};
            if (properties.size() > 0){
                pgs.push_back(std::make_shared<graphar::PropertyGroup>(properties, GetFileType(), "test_pg"));
            }

            graphar::IdType src_chunk_size = type_schema[edges_schema.src_type]->chunk_size;
            graphar::IdType dst_chunk_size = type_schema[edges_schema.dst_type]->chunk_size;
            auto edge_chunk_size = (edges_schema.chunk_size > 0) ? edges_schema.chunk_size : src_chunk_size * dst_chunk_size;
            edges_infos.push_back(graphar::CreateEdgeInfo(
                edges_schema.src_type, edges_schema.type, edges_schema.dst_type, 
                edge_chunk_size, src_chunk_size, dst_chunk_size, edges_schema.directed,
                adjacent_lists,
                pgs, EdgePathPrefix + edges_schema.src_type + "_" + edges_schema.type + "_" + edges_schema.dst_type + "/", version));
            REQUIRE(!edges_infos.back()->Dump().has_error());
            REQUIRE(edges_infos.back()->Save(output_path + edges_schema.src_type + "_" + edges_schema.type + "_" + edges_schema.dst_type + EdgeFileExtension).ok());
        }

        // Graph Info
        graph_info = graphar::CreateGraphInfo(graph_name, vertex_infos, edges_infos, {}, "./", version);
        REQUIRE(!graph_info->Dump().has_error());
        REQUIRE(graph_info->Save(graph_path).ok());

        REQUIRE(RemovePrefix(graph_path));

        // vertices_list
        for (auto ind_v = 0; ind_v < vertices_list.size(); ++ind_v){
            const auto& vertices_schema = vertices_list[ind_v];
            auto v_builder = graphar::builder::VerticesBuilder::Make(vertex_infos[ind_v], output_path, 0).value();
            for (const auto& vertex_data : vertices_schema.vertices) {
                auto vertex = graphar::builder::Vertex(vertex_data.ind);
                FillProperties<graphar::builder::Vertex>(vertex, vertices_schema.properties, vertex_data.properties);

                REQUIRE(v_builder->AddVertex(vertex).ok());
            }
            REQUIRE(v_builder->Dump().ok());
        }

        // edges_list
        for (auto ind_e = 0; ind_e < edges_list.size(); ++ind_e){
            const auto& edges_schema = edges_list[ind_e];
            auto num_vertices = edges_schema.num_vertices;
            for (const auto& adjacent_type : adjacent_types){
                auto e_builder = graphar::builder::EdgesBuilder::Make(edges_infos[ind_e], output_path, adjacent_type, num_vertices).value();
                for (const auto& edge_data : edges_schema.edges) {
                    auto edge = graphar::builder::Edge(edge_data.src, edge_data.dst);
                    FillProperties<graphar::builder::Edge>(edge, edges_schema.properties, edge_data.properties);
                    REQUIRE(e_builder->AddEdge(edge).ok());
                }
                REQUIRE(e_builder->Dump().ok());
            }
        }   

        return graph_folder.string();
    }

public:
    BasicGrapharFixture(): tmp_folder(std::filesystem::temp_directory_path() / "duckdb_graphar/data/"), db(nullptr), conn(db) {};
    ~BasicGrapharFixture(){
        for (const auto& graph_folder : graph_folders){
            REQUIRE_NOTHROW(std::filesystem::remove_all(graph_folder));
        }
    }
};
