# Function index

## Scalar Functions

## Table Functions

| Function                        | Description                                            |
|---------------------------------|--------------------------------------------------------|
| [read_vertices](#read_vertices) | Returns a Table of Vertices by Type                    |
| [read_edges](#read_edges)       | Returns a Table of Edges by Type of src, edge, dst     |
| [edges_vertex](#edges_vertex)   | Returns a Table with Degree of vertex for src vertices |
| [two_hop](#two_hop)             | Returns a Table with 2-hop edges of vertex             |
| [shortest_path](#shortest_path) | Returns a Table with the shortest path between two vertices |

### read_vertices

#### Signatures
```sql
TABLE read_vertices(VARCHAR graph_path, VARCHAR type);
```

#### DESCRIPTION
Reads and returns a vertex table from a GraphAr dataset.

`graph_path` - Path to the GraphAr YAML schema file describing the graph. \
`type` - The name of the vertex type to load (as defined in the schema).

The function returns a DuckDB table representing the vertex data, allowing SQL filtering.

#### Examples
```sql
SELECT * 
FROM read_vertices('test/data/git/Git.yaml', type='Person');
-- Table vid (graphId), all properties of vertex;
```

### read_edges

#### Signatures
```sql
TABLE read_edges(VARCHAR graph_path, VARCHAR src, VARCHAR type, VARCHAR dst);
```

#### DESCRIPTION
Reads and returns a vertex table from a GraphAr dataset.

`graph_path` - Path to the GraphAr YAML schema file describing the **graph**. \
`src` - The name of the source vertex type. \
`type` - The name of the edge type to load (as defined in the schema). \
`dst` - The name of the destination vertex type.

This function returns a DuckDB table representing the edges between vertex types src and dst of type. 
The returned table includes the edge properties, as well as source and destination vertex IDs.

#### Examples
```sql
SELECT * 
FROM read_edges('test/data/git/Git.yaml', src='Person', type='knows', dst='Person');
-- Table src (_graphArSrcIndex), dst (_graphArDstIndex), all properties of edge;

SELECT *
FROM read_edges('test/data/git/Git.yaml', src='Person', type='knows', dst='Person') WHERE _graphArSrcIndex=42;
-- Table -  1-hop neighbors of vertex with GraphAr ID = 42
```

### edges_vertex

#### Signatures
```sql
TABLE edges_vertex(VARCHAR edge_path);
```

#### DESCRIPTION
Returns a vertex degree table based on the provided edge data.

`edge_path` - Path to the GraphAr YAML schema file describing the **edge**.

This function scans the edge data and computes the out-degree (number of outgoing edges) for each source vertex.
The result is a table containing one row per source vertex with its corresponding degree.

#### Examples
```sql
SELECT * 
FROM edges_vertex('test/data/git/Person_knows_Person.yaml');
-- Table ;
```

### two_hop

#### Signatures
```sql
TABLE two_hop(VARCHAR edge_path, BIGINT vid);
```

#### DESCRIPTION
Returns all 2-hop edge paths starting from a given vertex.

`edge_path` - Path to the GraphAr YAML schema file describing the **edge**. \
`vid` - Source vertex ID from which to compute 2-hop paths.

This function finds all edges from vid to its 1-hop neighbors, and all edges from those neighbors to their neighbors (i.e., 2-hop paths). The result is a table of edge pairs, where each row represents a two-edge path: one from the source vertex to an intermediate vertex, and one from that intermediate vertex to a final destination.

#### Examples
```sql
SELECT * 
FROM edges_vertex('test/data/git/Person_knows_Person.yaml', vid=42);
-- Table with src (_graphArSrcIndex), dst (_graphArDstIndex);
```

### shortest_path

#### Signatures
```sql
TABLE shortest_path(BIGINT src_vertex_id, BIGINT dst_vertex_id, VARCHAR path);
TABLE shortest_path(BIGINT src_vertex_id, BIGINT dst_vertex_id, VARCHAR graph_path, src VARCHAR, type VARCHAR, dst VARCHAR);
```

#### DESCRIPTION
Returns a table representing the shortest path between two vertices in a GraphAr graph. The result contains two columns: `step_number` (row index) and `_graphArVertexIndex` (vertex ID at that step).

Two signatures are supported:

1. `TABLE shortest_path(BIGINT src_vertex_id, BIGINT dst_vertex_id, VARCHAR edge_table_name)` — uses an edge table name from an attached GraphAr catalog.
2. `TABLE shortest_path(BIGINT src_vertex_id, BIGINT dst_vertex_id, VARCHAR graph_path, src VARCHAR, type VARCHAR, dst VARCHAR)` — uses a YAML schema file path with named parameters `src`, `type`, and `dst` to specify the edge type.

`src` — The source vertex type name. \
`type` — The edge type name. \
`dst` — The destination vertex type name.

This function uses a bidirectional BFS algorithm. It returns the **first** path found, which may not be unique when multiple shortest paths exist. **The result is not guaranteed to be deterministic** across different runs or graph configurations.

#### Examples
```sql
SELECT * FROM shortest_path(0, 23977, Person_knows_Person);

SELECT * FROM shortest_path(0, 23977,
    'test/data/git/Git.yaml',
    src='Person', type='knows', dst='Person');
```
