#pragma once

#include "functions/table/read_base.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_schema_entry.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/function/table_function.hpp>

#include <graphar/graph_info.h>

namespace duckdb {

enum class DirectionType { DIRECTED, REVERSED };

class HopBaseBindData : public ReadBindData {
public:
    HopBaseBindData() = default;

    std::string GetFullTableName() const {
        if (!catalog_name.empty() && !schema_name.empty() && !table_name.empty()) {
            return catalog_name + "." + schema_name + "." + table_name;
        }
        if (!catalog_name.empty() && !table_name.empty()) {
            return catalog_name + "." + table_name;
        }
        return "";
    }

    std::string GetSrcName() const {
        switch (direction_type) {
            case DirectionType::DIRECTED:
                return SRC_GID_COLUMN;
            case DirectionType::REVERSED:
                return DST_GID_COLUMN;
            default:
                throw NotImplementedException("Unsupported edge direction type");
        }
    }

    std::string GetDstName() const {
        switch (direction_type) {
            case DirectionType::REVERSED:
                return SRC_GID_COLUMN;
            case DirectionType::DIRECTED:
                return DST_GID_COLUMN;
            default:
                throw NotImplementedException("Unsupported edge direction type");
        }
    }

    std::shared_ptr<graphar::EdgeInfo> edge_info;
    std::vector<graphar::IdType> vids;
    std::string catalog_name;
    std::string schema_name;
    std::string table_name;

    DirectionType direction_type = DirectionType::DIRECTED;
    column_t dst_column_idx;

    friend class HopBase;
};

class HopBaseGlobalTableFunctionState : public ReadBaseGlobalTableFunctionState {
public:
    HopBaseGlobalTableFunctionState() = default;
    HopBaseGlobalTableFunctionState(ReadBaseGlobalTableFunctionState& gstate)
        : ReadBaseGlobalTableFunctionState(gstate) {}

    std::string vertexesToString() {
        auto q = vertexes;

        std::ostringstream ss;
        ss << "vertexes: size=" << q.size() << " {";
        while (!q.empty()) {
            ss << q.front() << ",";
            q.pop();
        }
        ss << "}";
        return ss.str();
    }

public:
    std::shared_ptr<graphar::EdgeInfo> edge_info;

    std::queue<graphar::IdType> vertexes;
    std::unordered_set<graphar::IdType> _vertexes;
    size_t cur_idx = 0;
    size_t next_hop_idx;

    DirectionType direction_type = DirectionType::DIRECTED;
    column_t dst_column_idx;
    bool dst_column_found;

    std::pair<size_t, size_t> special_dst = {-1, -1};

    friend class HopBase;
};

class HopBase {
public:
    static bool IsCatalogMode(TableFunctionBindInput& input) {
        ;
        DUCKDB_GRAPHAR_LOG_TRACE("HopBase::IsCatalogMode");

        bool has_src = input.named_parameters.find("src") != input.named_parameters.end();
        bool has_dst = input.named_parameters.find("dst") != input.named_parameters.end();
        bool has_edge = input.named_parameters.find("type") != input.named_parameters.end();
        bool has_catalog = input.named_parameters.find("catalog") != input.named_parameters.end();

        bool is_path_mode = has_src || has_dst || has_edge;

        if (is_path_mode && has_catalog) {
            throw BinderException(
                "%s: 'catalog' cannot be used together with 'src', 'dst', or 'type'. "
                "Use either:\n"
                "  %s('path.yaml', src='...', dst='...', type='...')\n"
                "  %s('table_name', catalog='...')",
                input.table_function.name, input.table_function.name, input.table_function.name);
        }

        return !is_path_mode;
    }
    static void SetBindDataByEdgeTable(ClientContext& context, TableFunctionBindInput& input,
                                       HopBaseBindData& bind_data) {
        DUCKDB_GRAPHAR_LOG_TRACE("HopBase::SetDataByEdgeTable");

        bind_data.table_name = StringValue::Get(input.inputs[0]);
        std::string catalog_name;

        auto catalog_entry = input.named_parameters.find("catalog");
        if (catalog_entry != input.named_parameters.end()) {
            catalog_name = StringValue::Get(catalog_entry->second);
        }
        auto& catalog = Catalog::GetCatalog(context, catalog_name);
        if (catalog.GetCatalogType() != GraphArCatalog::TYPE) {
            throw BinderException("Expecting a GraphAr catalog, but got %s", catalog.GetCatalogType());
        }
        bind_data.catalog_name = catalog.GetName();

        auto& graphar_catalog = catalog.Cast<GraphArCatalog>();
        bind_data.graph_info = graphar_catalog.GetGraphInfo();

        auto& schema = graphar_catalog.GetMainSchema();
        bind_data.schema_name = schema.Name;

        auto& tables = schema.tables;
        auto table_info = tables.GetTableInfo(context, schema, bind_data.table_name);
        if (!table_info) {
            throw BinderException("Table %s not found", bind_data.table_name);
        }
        if (table_info->GetType() != GraphArTableType::Edge) {
            throw BinderException("Table %s is not an edge table", bind_data.table_name);
        }
        bind_data.edge_info = std::get<std::shared_ptr<graphar::EdgeInfo>>(table_info->GetTypeInfo());

        DUCKDB_GRAPHAR_LOG_DEBUG("HopBase using edge table: " + bind_data.GetFullTableName());
    }
    static void SetBindDataByGraphPath(ClientContext& context, TableFunctionBindInput& input,
                                       HopBaseBindData& bind_data) {
        DUCKDB_GRAPHAR_LOG_TRACE("HopBase::SetBindDataByGraphPath");

        const auto file_path = StringValue::Get(input.inputs[0]);
        const auto src_type = StringValue::Get(input.named_parameters.at("src"));
        std::string dst_type;
        auto dst_entry = input.named_parameters.find("dst");
        if (dst_entry == input.named_parameters.end()) {
            dst_type = src_type;
        } else {
            dst_type = StringValue::Get(dst_entry->second);
            if (dst_type != src_type) {
                throw BinderException("Expecting src and dst to be the same type");
            }
        }
        const std::string e_type = StringValue::Get(input.named_parameters.at("type"));

        DUCKDB_GRAPHAR_LOG_DEBUG("HopBase looking for: " + src_type + " " + e_type + " " + dst_type + " at " +
                                 file_path);

        auto maybe_graph_info = graphar::GraphInfo::Load(file_path);
        if (maybe_graph_info.has_error()) {
            throw IOException("Failed to load graph info from path: %s", file_path);
        }
        bind_data.graph_info = maybe_graph_info.value();

        bind_data.edge_info = bind_data.graph_info->GetEdgeInfo(src_type, e_type, dst_type);
        if (!bind_data.edge_info) {
            throw BinderException("Edges of this type are not found");
        }
    }
    static void SetBindDataVids(TableFunctionBindInput& input, HopBaseBindData& bind_data) {
        DUCKDB_GRAPHAR_LOG_TRACE("HopBase::SetBindDataVids");

        duckdb::vector<duckdb::Value> duck_vids;
        auto vids_entry = input.named_parameters.find("vids");
        if (vids_entry == input.named_parameters.end()) {
            auto vid_entry = input.named_parameters.find("vid");
            if (vid_entry != input.named_parameters.end() && !vid_entry->second.IsNull()) {
                duck_vids = {vid_entry->second};
            } else {
                throw BinderException("Expecting a named parameter vids or vid");
            }
        } else {
            duck_vids = ListValue::GetChildren(vids_entry->second);
            if (duck_vids.empty()) {
                throw BinderException("Expecting non empty vids");
            }
        }

        bind_data.vids.resize(duck_vids.size());
        for (size_t i = 0; i < duck_vids.size(); ++i) {
            bind_data.vids[i] = IntegerValue::Get(duck_vids[i]);
        }

        bind_data.vid_ranges.resize(bind_data.vids.size());
        std::transform(bind_data.vids.begin(), bind_data.vids.end(), bind_data.vid_ranges.begin(),
                       [](const auto& vid) { return std::make_pair(vid, vid + 1); });
    }

    static void SetBindDataFilter(HopBaseBindData& bind_data) {
        DUCKDB_GRAPHAR_LOG_TRACE("HopBase::SetBindDataDst");

        bind_data.filter_column = bind_data.GetSrcName();
    }

    static void SetBindDataDstIdx(vector<string>& names, HopBaseBindData& bind_data) {
        DUCKDB_GRAPHAR_LOG_TRACE("HopBase::SetBindDataDstIdx");
        auto dst_col = bind_data.GetDstName();
        for (size_t i = 0; i < names.size(); ++i) {
            if (names[i] == dst_col) {
                bind_data.dst_column_idx = i;
                break;
            }
        }

        DUCKDB_GRAPHAR_LOG_DEBUG("HopBase::SetBindDataDstIdx: dst_column_idx = " +
                                 std::to_string(bind_data.dst_column_idx));
    }

    static void SetGlobalState(const HopBaseBindData& bind_data, HopBaseGlobalTableFunctionState& gstate) {
        DUCKDB_GRAPHAR_LOG_TRACE("HopBase::SetGlobalState");

        gstate.graph_info = bind_data.GetGraphInfo();
        gstate.edge_info = bind_data.edge_info;

        DUCKDB_GRAPHAR_LOG_DEBUG("HopBase::SetGlobalState: vids size=" + std::to_string(bind_data.vids.size()));
        for (auto& vid : bind_data.vids) {
            gstate._vertexes.insert(vid);
            if (gstate.vertexes.size() != gstate._vertexes.size()) {
                gstate.vertexes.push(vid);
            }
        }

        gstate.next_hop_idx = gstate.vertexes.size();
        gstate.direction_type = bind_data.direction_type;
        DUCKDB_GRAPHAR_LOG_DEBUG("HopBase::SetGlobalState: dst_column_idx=" + std::to_string(bind_data.dst_column_idx));
        gstate.dst_column_idx = bind_data.dst_column_idx;

        idx_t column_idx;
        if (gstate.column_ids.empty()) {
            DUCKDB_GRAPHAR_LOG_DEBUG("HopBase::SetGlobalState: EMPTY column_ids");
            column_idx = gstate.dst_column_idx;
        } else {
            auto column_it = std::find(gstate.column_ids.begin(), gstate.column_ids.end(), gstate.dst_column_idx);
            if (column_it == gstate.column_ids.end()) {
                throw InternalException("dst_column_idx(" + std::to_string(gstate.dst_column_idx) +
                                        ") not found in column_ids");
            }

            column_idx = std::distance(gstate.column_ids.begin(), column_it);
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("HopBase::SetGlobalState: column_idx=" + std::to_string(column_idx));

        auto columns_pref_num = 0;
        for (auto pg_i = 0; pg_i < gstate.prop_types.size();
             columns_pref_num += gstate.prop_types[pg_i].size(), ++pg_i) {
            if (columns_pref_num > gstate.dst_column_idx ||
                gstate.dst_column_idx >= columns_pref_num + gstate.prop_types[pg_i].size()) {
                continue;
            }

            auto projected_ind = gstate.dst_column_idx - columns_pref_num;
            if (!bind_data.pg_for_id && pg_i > 0) {
                projected_ind += gstate.id_columns_num;
            }

            auto global_projected_i = std::find(gstate.global_projected_inds[pg_i].begin(),
                                                gstate.global_projected_inds[pg_i].end(), column_idx);
            if (global_projected_i == gstate.global_projected_inds[pg_i].end()) {
                throw InternalException("Column DST " + std::to_string(gstate.dst_column_idx) +
                                        " not found in the global projected inds");
            }
            gstate.special_dst = {pg_i, global_projected_i - gstate.global_projected_inds[pg_i].begin()};
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("HopBase::SetGlobalState: special_dst=" + std::to_string(gstate.special_dst.first) +
                                 ',' + std::to_string(gstate.special_dst.second));
    }

    static void SetFunctionParams(TableFunction& fun) {
        fun.named_parameters["src"] = LogicalType::VARCHAR;
        fun.named_parameters["dst"] = LogicalType::VARCHAR;
        fun.named_parameters["type"] = LogicalType::VARCHAR;

        fun.named_parameters["catalog"] = LogicalType::VARCHAR;

        fun.named_parameters["vids"] = LogicalType::LIST(LogicalType::BIGINT);
        fun.named_parameters["vid"] = LogicalType::BIGINT;
    }
};

}  // namespace duckdb