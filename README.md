> **Disclaimer:** This extension is currently in an experimental state.
> Feel free to try it out, but be aware that things may not work as expected

# GraphArDuck

A DuckDB extension that enables reading data stored in the
[Apache GraphAr](https://graphar.apache.org) format.
It allows you to query vertex and edge tables using SQL, with support for simple filtering.

## Supported Functions

The full list of functions and their documentation is available in the [function reference](docs/functions.md).

## Benchmarking

The extension was benchmarked on the snap and ldbc datasets.
All about benchmarking you can find [here](docs/benchmarks.md).

## Building from source

This extension is built using the [DuckDB Extension Template](https://github.com/duckdb/extension-template).

### Dependencies

Extension need DuckDB, GraphAr and Arrow.

### Build extension:

Development Build:
```bash
mkdir build && cd build
cmake .. -DLOAD_TESTS=ON
make
```

Production/CI Build:
```bash
cmake -B build -DBUILD_LOADABLE_EXTENSION=ON -DLOAD_TESTS=OFF
cmake --build build --target install
```
