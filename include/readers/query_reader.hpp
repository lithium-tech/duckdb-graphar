#pragma once

#include <duckdb/main/connection.hpp>

#include <graphar/arrow/chunk_reader.h>

namespace duckdb {

class QueryChunkReader {
public:
    explicit QueryChunkReader(std::shared_ptr<Connection> conn, std::string& query_string) : conn(std::move(conn)), query(query_string) {}

    template <bool Stream = false, typename... Args>
    void callQuery(Args&&... args) {
        if constexpr (Stream) {
            result = conn->SendQuery(query);
        } else {
            result = conn->Query(query, args...);
        }
        chunk = result->Fetch();
    }

    graphar::Status next_chunk() {
        if (chunk != nullptr) {
            return graphar::Status::OK();
        }
        return graphar::Status::IndexError("No more chunks to read!!");
    }

    duckdb::unique_ptr<duckdb::DataChunk> GetChunk() {
        auto cur_chunk = std::move(chunk);
        chunk = result->Fetch();
        return cur_chunk;
    }

    static graphar::Result<std::shared_ptr<QueryChunkReader>> Make(std::shared_ptr<Connection> conn,
                                                                   std::string& query_string) {
        auto reader = std::make_shared<QueryChunkReader>(std::move(conn), query_string);
        // reader->callQuery(query_string, vid);
        return reader;
    }

    void updateQuery(std::string &query_string) {
        query = query_string;
    }

private:
    std::string query;

    std::shared_ptr<Connection> conn;
    duckdb::unique_ptr<QueryResult> result;
    duckdb::unique_ptr<duckdb::DataChunk> chunk;
};

}  // namespace duckdb