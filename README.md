> **Disclaimer:**  This extension is currently in an **experimental state**.
> While functional, it may contain unstable features, unexpected behavior, or breaking changes in future releases.
> Use with appropriate caution in non-production environments.

# DuckDB <img src="docs/logo.png" alt="DuckDB-GraphAr" width="28" height="28"/> GraphAr

A [DuckDB](https://duckdb.org/) extension that enables reading data stored in the
[Apache GraphAr](https://graphar.apache.org) format.
It allows you to query vertex and edge tables using SQL, with support for simple filtering.

## DeepWiki

A high-level introduction to the DuckDB GraphAr extension, explaining its architecture and key components, as well as a description of the overall system design, major subsystems, and how they integrate to enable SQL querying of Apache GraphAr data through DuckDB is contained in the [DeepWiki](https://deepwiki.com/lithium-tech/duckdb-graphar)

## Dependencies

This extension requires the following dependencies:

- [DuckDB](https://duckdb.org) - An in-process SQL OLAP database management system.
- [Apache GraphAr](https://graphar.apache.org/) - An open source, standard data file format for graph data storage and retrieval.
- [Apache Arrow](https://arrow.apache.org) - A cross-language development platform for in-memory data.

## SAST Tools

- [Cppcheck](https://cppcheck.sourceforge.io/) - static analysis tool for C/C++ code
- [PVS-Studio](https://pvs-studio.com/pvs-studio/?utm_source=website&utm_medium=github&utm_campaign=open_source) - static analyzer for C, C++, C#, and Java code.

## Building From Source

```shell
# Clone the repo and its dependencies
git clone --recurse-submodules git@github.com:lithium-tech/duckdb-graphar.git
```

Initialize submodules before building:

```shell
git submodule update --init --recursive
```

Build the extension:

```shell
make release
```

For a debug build:

```shell
make debug
```

## Run the extension

After building, the `duckdb` binary (with the extension statically linked) is
produced at `build/release/duckdb`. Launch it and attach to a GraphAr graph
directory to query its vertex/edge tables:

```bash
./build/release/duckdb -c "attach '/path/to/Graph.yaml' (type duckdb_graphar); select * from person limit 20;"
```

The `data/` directory is not tracked in full; the source datasets must be
converted to GraphAr format before use. This is done by the same scripts the CI
pipeline runs:

```bash
# Install the GraphAr CLI (builds against the locally built arrow/graphar)
./scripts/install-cli.sh

# Generate the GraphAr graphs into data/<graph>/graphar/ (e.g. Git.graph.yaml)
./scripts/generate-graphar-data.sh
```

After running these, example graphs are available under
`data/<graph>/graphar/` (e.g. `data/snap-musae-github/graphar/Git.graph.yaml`).

The following graphs are produced:

- `data/snap-musae-github` — the full GitHub dataset: 37,700 vertices and
  289,003 edges in a **single chunk** (chunk sizes exceed the data size).
- `data/snap-musae-github-csv` — the same data, stored with CSV (instead of
  parquet) file type.
- `data/snap-musae-github-multichunk` — a small **multichunk** graph (3,789
  vertices, 2,560 edges) derived from `data/snap-musae-github` by keeping a
  contiguous vertex window. It uses `vertex_chunk_size: 64` and
  `edge_chunk_size: 256`, so vertex data spans 60 chunks and edge adj_lists are
  split across the 60 source-vertex chunks. Its purpose is to exercise
  cross-chunk reads and to expose fail-potential scenarios that a single-chunk
  graph cannot reproduce (see `test/sql/graphar/multichunk.test`).

## S3 warning note

When using S3-backed data, DuckDB may print the warning

```
arrow::fs::FinalizeS3 was not called even though S3 was initialized. This could
lead to a segmentation fault at exit
```

To avoid a possible segmentation fault on exit, call the
`duckdb_graphar_finalize_s3()` function (registered by this extension) to explicitly
finalize the S3 filesystem before the process ends.

## Telemetry

The extension always logs product-usage events (e.g. graph attach/detach and
graph read operations) as JSONL files. It is always enabled and cannot be turned
off. The JSONL spool directory and rotation behavior are controlled by these
`SET` options:

- `graphar_pua_sink_jsonl_file_path` - output directory for the JSONL files.
- `graphar_pua_sink_jsonl_file_rotation_size_bytes` - file segment size (default 16 MB).
- `graphar_pua_sink_jsonl_file_rotation_interval_seconds` - file segment age (default 24 h).

```sql
SET graphar_pua_sink_jsonl_file_path = '/path/to/analytics';
```

These options are read lazily at the first telemetry event (the first graph
attach or read), so you can `SET` them any time before performing the first
graph operation. After the first event the settings are fixed for the rest of
the process; changing them later has no effect.

## Running tests

The extension has two test suites, both built by default: SQL end-to-end
[SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) under
`test/sql/` (run by DuckDB's `unittest` binary), and C++ unit tests (Catch2)
under `test/cpp/` (own `unittest_graphar` binary). Run all of them, or each
suite separately:

```bash
make test          # both suites
make test-sql      # SQL tests (test/sql/)
make test-unit     # C++ unit tests (test/cpp/)
```

When building with CMake directly, both suites are gated by two options:
`-DBUILD_UNITTESTS` (DuckDB's own, for the SQL-test runner) and
`-DBUILD_EXTENSION_UNIT_TESTS` (for the extension's C++ unit-test binary).
Both default to enabled via the Makefile.
