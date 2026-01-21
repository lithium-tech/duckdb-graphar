#pragma once

#include "utils/global_log_manager.hpp"

#include <arrow/api.h>

#include <graphar/api/arrow_reader.h>
#include <graphar/api/info.h>

namespace duckdb {

inline std::pair<int64_t, int64_t> GetChunkAndOffset(graphar::IdType chunk_size, graphar::IdType offset) {
    int64_t chunk_num = offset / chunk_size;
    int64_t offset_in_chunk = offset % chunk_size;
    return std::make_pair(chunk_num, offset_in_chunk);
}

class MyAdjReaderOrdSrc {
public:
    MyAdjReaderOrdSrc(const std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& prefix)
        : edge_info_(edge_info),
          prefix_(prefix),
          reader_(graphar::AdjListArrowChunkReader::Make(edge_info, graphar::AdjListType::ordered_by_source, prefix)
                      .value()) {}
    void find_src(graphar::IdType src) {
        reader_->seek_src(src);
        std::pair<graphar::IdType, graphar::IdType> range =
            (graphar::util::GetAdjListOffsetOfVertex(edge_info_, prefix_, graphar::AdjListType::ordered_by_source, src)
                 .value());
        auto chunk_size = edge_info_->GetChunkSize();
        size_ = range.second - range.first;
        start_ = GetChunkAndOffset(chunk_size, range.first);
        end_ = GetChunkAndOffset(chunk_size, range.second);
        if (start_.first == end_.first) {
            if (start_.second == end_.second) {
                chunk_count_ = 0;
            } else {
                chunk_count_ = 1;
            }
        } else {
            chunk_count_ = end_.first - start_.first;
            if (end_.second != 0) {
                chunk_count_++;
            }
        }
        is_empty_ = true;
    }

    bool has_next_table() { return chunk_i_ + 1 < chunk_count_; }

    bool finish_table() const { return chunk_i_ >= chunk_count_; }

    std::shared_ptr<arrow::Table> get_table() {
        auto table = reader_->GetChunk().value();

        if (chunk_i_ == chunk_count_ - 1 && end_.second != 0) {
            if (start_.first == end_.first) {
                return table->Slice(0, end_.second - start_.second);
            }
            return table->Slice(0, end_.second);
        }
        return table;
    }

    void next_table() {
        reader_->next_chunk();
        chunk_i_++;
    }

    std::shared_ptr<arrow::Table> get(int64_t count) {
        if (is_empty_) {
            table_ = get_table();
            offset_ = 0;
            is_empty_ = false;
        }

        auto result = table_->Slice(offset_, count);
        offset_ += count;
        if (offset_ >= table_->num_rows()) {
            if (has_next_table()) {
                next_table();
                offset_ = 0;
            }
        }
        return result;
    }

    bool finish() {
        if (is_empty_) {
            if (finish_table()) {
                return true;
            }
            table_ = get_table();
            offset_ = 0;
            is_empty_ = false;
        }
        if (offset_ >= table_->num_rows()) {
            return true;
        }
        return false;
    }

    int64_t size() { return size_; }

private:
    const std::shared_ptr<graphar::EdgeInfo> edge_info_;
    const std::string prefix_;
    int64_t size_ = 0;
    std::shared_ptr<graphar::AdjListArrowChunkReader> reader_;
    std::pair<int64_t, int64_t> start_;
    std::pair<int64_t, int64_t> end_;
    int64_t chunk_count_ = 0, chunk_i_ = 0;
    int64_t offset_ = 0;
    std::shared_ptr<arrow::Table> table_;
    bool is_empty_ = false;
};
}  // namespace duckdb