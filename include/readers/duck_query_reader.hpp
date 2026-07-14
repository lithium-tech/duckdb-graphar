// #pragma once

// #include "readers/base_reader.hpp"
// #include "utils/func.hpp"
// #include "utils/global_log_manager.hpp"

// #include <graphar/result.h>

// #include <duckdb.hpp>

// namespace duckdb {

// class DuckQueryChunkReader {
// public:
//     DuckQueryChunkReader(std::shared_ptr<graphar::TSQueryChunkReader> init_base, ClientContext& init_context)
//         : base(std::move(init_base)), context(init_context) {}

//     static graphar::Result<std::shared_ptr<DuckQueryChunkReader>> Make(
//         ClientContext& context, std::shared_ptr<graphar::TSQueryChunkReader> base_ptr) {
//         if (!base_ptr) {
//             return graphar::Status::Invalid("base_ptr can't be null!");
//         }
//         return std::make_shared<DuckQueryChunkReader>(std::move(base_ptr), context);
//     }

//     template <typename... Args>
//     static graphar::Result<std::shared_ptr<DuckQueryChunkReader>> Make(ClientContext& context, Args&&... args) {
//         GAR_ASSIGN_OR_RAISE(auto base_ptr, graphar::TSQueryChunkReader::Make(std::forward<Args>(args)...));
//         return std::make_shared<DuckQueryChunkReader>(std::move(base_ptr), context);
//     }

//     const inline bool NoMoreRows() {
//         if (cur_chunk && read_rows < cur_chunk->size()) {
//             return false;
//         }
//         // if (cur_result) {
//         //     cur_chunk = cur_result->Fetch();
//         //     if (cur_chunk && cur_chunk->size() > 0) {
//         //         return false;
//         //     }
//         //     cur_result = nullptr;
//         // }
//         return true;
//     }

//     graphar::Result<graphar::GetChunkFinalResult> GetChunk(idx_t num_rows) {
//         if (GetRowsNum() == 0) {
//             throw graphar::Status::IndexError("No more chunks to read!");
//         }
//         if (num_rows > cur_chunk->size() - read_rows) {
//             throw graphar::Status::IndexError("Could read at most " + std::to_string(cur_chunk->size() - read_rows) +
//                                               " rows, but " + std::to_string(num_rows) + " were requested");
//         }
//         auto res = duckdb::make_uniq<duckdb::DataChunk>();
//         res->Initialize(context, cur_chunk->GetTypes());
//         res->Reference(*cur_chunk);
//         res->Slice(read_rows, num_rows);
//         read_rows += num_rows;
//         cur_read_idx++;
//         return std::make_pair(std::move(res), GetChunkIdx(0, cur_read_idx));
//     }

//     bool CheckIfNewFileNeeded() { return NoMoreRows(); }

//     void AcquirePathUnderLock() { throw NotImplementedException("AcquirePathUnderLock is not implemented"); }

//     void SelectColumns(std::vector<duckdb::column_t> proj_columns_) { 
//         DUCKDB_GRAPHAR_LOG_TRACE("DuckQueryChunkReader::SelectColumns");
//         std::ostringstream ss;
//         ss << "proj_cols: size=" << proj_columns_.size() << ": ";
//         for (auto& col : proj_columns_) {
//             ss << col << ' ';
//         }
//         DUCKDB_GRAPHAR_LOG_WARN("ignore: " + ss.str());
//      }

//     idx_t GetRowsNum() {
//         if (NoMoreRows()) {
//             auto gc_result = base->GetChunk();
//             if (gc_result.no_more_chunks || gc_result.chunk == nullptr) {
//                 return 0;
//             }
//             read_rows = 0;
//             cur_read_idx = 0;
//             cur_chunk = std::move(gc_result.chunk);
//         }

//         return cur_chunk->size() - read_rows;
//     }
// private:
//     ClientContext& context;
//     std::shared_ptr<graphar::TSQueryChunkReader> base;

//     duckdb::idx_t read_rows = 0;
//     duckdb::unique_ptr<duckdb::DataChunk> cur_chunk = nullptr;
//     duckdb::idx_t cur_read_idx = 0;
//     // duckdb::unique_ptr<duckdb::QueryResult> cur_result = nullptr;
//     duckdb::idx_t cur_result_idx = 0;
//     bool next_acquired = false;
// };

// }  // namespace duckdb

// namespace graphar {

// using DuckQueryChunkReader = duckdb::DuckQueryChunkReader;

// }  // namespace graphar