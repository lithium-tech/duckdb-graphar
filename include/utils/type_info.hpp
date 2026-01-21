#pragma once

#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <graphar/graph_info.h>

#include <mutex>
#include <unordered_map>
#include <variant>

namespace duckdb {

using TypeInfoPtr = std::variant<std::shared_ptr<graphar::VertexInfo>, std::shared_ptr<graphar::EdgeInfo>>;

static const graphar::PropertyGroupVector& GetPropertyGroups(TypeInfoPtr& type_info) {
    return std::visit([&](auto& t) -> const graphar::PropertyGroupVector& { return t->GetPropertyGroups(); },
                      type_info);
}

static std::string GetVertexTypeName(TypeInfoPtr& type_info, const std::string& column_name) {
    return std::visit(
        [&](auto& t) {
            if constexpr (requires { t->GetSrcType(); }) {
                if (column_name == SRC_GID_COLUMN) {
                    return t->GetSrcType();
                } else {
                    return t->GetDstType();
                }
            } else {
                return t->GetType();
            }
        },
        type_info);
}

class GetCountClass {
public:
    static int64_t GetCount(const TypeInfoPtr& type_info, const std::string& graph_prefix);

private:
    static std::unordered_map<std::string, int64_t> count_cache;
    static std::mutex count_cache_mutex;
};

}  // namespace duckdb
