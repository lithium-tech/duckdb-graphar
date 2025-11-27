#pragma once

#include <graphar/arrow/chunk_reader.h>
#include <graphar/chunk_info_reader.h>
#include <graphar/result.h>

#include <mutex>

namespace graphar {

template <typename StoredReader>
class ThreadSafeReader {
public:
    using GetChunkResult = decltype(std::declval<StoredReader>().GetChunk());

    ThreadSafeReader(std::shared_ptr<StoredReader> reader) : reader(std::move(reader)) {}

    template <typename... Args>
    static graphar::Result<std::shared_ptr<ThreadSafeReader>> Make(Args&&... args) {
        GAR_ASSIGN_OR_RAISE(auto stored_reader, StoredReader::Make(std::forward<Args>(args)...));
        return std::make_shared<ThreadSafeReader>(std::move(stored_reader));
    }

    GetChunkResult GetChunk() {
        std::lock_guard<std::mutex> lock(mtx);
        if (reading_started) {
            if (!reader->next_chunk().ok()) {
                return graphar::Status::IndexError("No more chunks to read!");
            }
        } else {
            reading_started = true;
        }
        return reader->GetChunk();
    }

    // Status next_chunk() {
    //     std::lock_guard<std::mutex> lock(mtx);
    //     return reader->next_chunk();
    // }

private:
    std::shared_ptr<StoredReader> reader;
    std::mutex mtx;
    bool reading_started = false;
};

using TSVertexPropertyArrowChunkReader = ThreadSafeReader<VertexPropertyArrowChunkReader>;
using TSAdjListArrowChunkReader = ThreadSafeReader<AdjListArrowChunkReader>;
using TSAdjListPropertyArrowChunkReader = ThreadSafeReader<AdjListPropertyArrowChunkReader>;
using TSVertexPropertyChunkInfoReader = ThreadSafeReader<VertexPropertyChunkInfoReader>;
using TSAdjListChunkInfoReader = ThreadSafeReader<AdjListChunkInfoReader>;
using TSAdjListPropertyChunkInfoReader = ThreadSafeReader<AdjListPropertyChunkInfoReader>;

}  // namespace graphar