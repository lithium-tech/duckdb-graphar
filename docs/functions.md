# Function index

## Scalar Functions

## Table Functions

| Function                                  | Description                                            |
|-------------------------------------------|--------------------------------------------------------|
| [read_vertices](#read_vertices)           | Returns a Table of Vertices by Type                    |
| [read_edges](#read_edges)                 | Returns a Table of Edges by Type of src, edge, dst     |
| [edges_vertex](#edges_vertex)             | Returns a Table with Degree of vertex for src vertices |
| [two_hop](#two_hop)                       | Returns a Table with 2-hop edges of vertex             |
| [read_hop](#read_hop)                     | Returns 1-hop edges from a vertex with optional 2-hop expansion |
| [read_hop_filtered](#read_hop_filtered)   | Returns 1-hop edges with filter pushdown optimization  |
| [shortest_path](#shortest_path)           | Returns a Table with the shortest path between two vertices |

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
TABLE two_hop(VARCHAR graph_path, BIGINT vid, src VARCHAR, type VARCHAR, dst VARCHAR);
```

#### DESCRIPTION
Returns all 2-hop edge paths starting from a given vertex.

`edge_path` - Path to the GraphAr YAML schema file describing the **edge** (EdgeInfo.yaml). \
`graph_path` - Path to the GraphAr YAML schema file describing the **graph** (GraphInfo.yaml). \
`vid` - Source vertex ID from which to compute 2-hop paths. \
`src` - The name of the source vertex type. \
`type` - The name of the edge type to load (as defined in the schema). \
`dst` - The name of the destination vertex type (must be the same as `src`).

This function finds all edges from vid to its 1-hop neighbors, and all edges from those neighbors to their neighbors (i.e., 2-hop paths). The result is a table of edge pairs, where each row represents a two-edge path: one from the source vertex to an intermediate vertex, and one from that intermediate vertex to a final destination.

**Important**: The function returns only the source and destination vertex indices for each 2-hop edge. Edge properties are NOT included.

#### Examples
```sql
-- Using EdgeInfo.yaml directly
SELECT * 
FROM two_hop('test/data/git/Person_knows_Person.yaml', vid=42);
-- Table with src (_graphArSrcIndex), dst (_graphArDstIndex) for 2-hop neighbors

-- Using GraphInfo.yaml with edge type specification
SELECT _graphArSrcIndex, _graphArDstIndex 
FROM two_hop('test/data/git/Git.yaml', vid=42, src='Person', type='knows', dst='Person');
-- Table with 2-hop edges for Person->Person via 'knows' relationship
```

### read_hop

#### Signatures
```sql
TABLE read_hop(VARCHAR edge_path, BIGINT vid);
TABLE read_hop(VARCHAR graph_path, BIGINT vid, src VARCHAR, type VARCHAR, dst VARCHAR);
TABLE read_hop(VARCHAR edge_table_name, catalog VARCHAR, BIGINT vid);
```

#### DESCRIPTION
Reads and returns 1-hop edges starting from a given vertex, with optional 2-hop expansion.

`edge_path` - Path to the GraphAr YAML schema file describing the **edge** (EdgeInfo.yaml). \
`graph_path` - Path to the GraphAr YAML schema file describing the **graph** (GraphInfo.yaml). \
`edge_table_name` - Name of the edge table in an attached GraphAr catalog. \
`catalog` - Name of the GraphAr catalog containing the edge table. \
`vid` - Source vertex ID from which to read edges. \
`src` - The name of the source vertex type. \
`type` - The name of the edge type to load (as defined in the schema). \
`dst` - The name of the destination vertex type (must be the same as `src`).

This function returns a DuckDB table representing the edges from the specified vertex. The returned table includes the edge properties, as well as source and destination vertex IDs (`_graphArSrcIndex`, `_graphArDstIndex`).

**Filtering behavior**:
- WHERE clause filtering is applied AFTER all 1-hop edges are retrieved
- The same filter applies to both 1-hop and 2-hop edges (if 2-hop expansion is performed internally)
- For more efficient filtering with filter pushdown, use `read_hop_filtered`

#### Examples
```sql
-- Using EdgeInfo.yaml directly
SELECT * 
FROM read_hop('test/data/git/Person_knows_Person.yaml', vid=42);
-- Table with src (_graphArSrcIndex), dst (_graphArDstIndex), all edge properties

-- Using GraphInfo.yaml with edge type specification
SELECT *
FROM read_hop('test/data/git/Git.yaml', src='Person', type='knows', dst='Person', vid=42);
-- Table with 1-hop edges from vertex 42

-- Filtering after retrieval (less efficient)
SELECT * 
FROM read_hop('test/data/git/Person_knows_Person.yaml', vid=42) 
WHERE weight > 10;
-- Filter applied after fetching all 1-hop edges

-- Using catalog mode
SELECT * 
FROM read_hop('Person_knows_Person', catalog='graphar', vid=42);
```

### read_hop_filtered

#### Signatures
```sql
TABLE read_hop_filtered(VARCHAR graph_path, BIGINT vid, src VARCHAR, type VARCHAR, dst VARCHAR);
TABLE read_hop_filtered(VARCHAR edge_table_name, catalog VARCHAR, BIGINT vid);
```

#### DESCRIPTION
Reads and returns 1-hop edges with filter pushdown optimization.

`graph_path` - Path to the GraphAr YAML schema file describing the **graph** (GraphInfo.yaml). \
`edge_table_name` - Name of the edge table in an attached GraphAr catalog. \
`catalog` - Name of the GraphAr catalog containing the edge table. \
`vid` - Source vertex ID from which to read edges. \
`src` - The name of the source vertex type. \
`type` - The name of the edge type to load (as defined in the schema). \
`dst` - The name of the destination vertex type (must be the same as `src`).

This function is similar to `read_hop` but supports **filter pushdown**: WHERE clause filters are pushed down into the data source, allowing more efficient query execution.

**Filtering behavior**:
- 2-hop edges are searched only for vertices obtained AFTER 1-hop filtering
- Filter is applied to 1-hop edges first, then 2-hop expansion uses the filtered results
- This is more efficient than `read_hop` when filtering is needed
- **Limitation**: The same filter applies to both 1-hop and 2-hop edges (cannot specify different filters)

#### Examples
```sql
-- Using GraphInfo.yaml with filter pushdown
SELECT *
FROM read_hop_filtered('test/data/git/Git.yaml', src='Person', type='knows', dst='Person', vid=42)
WHERE weight > 10;
-- Filter pushed down: only 1-hop edges with weight > 10 are retrieved,
-- then 2-hop edges are searched only from those filtered neighbors

-- Using catalog mode with filter
SELECT *
FROM read_hop_filtered('Person_knows_Person', catalog='graphar', vid=42)
WHERE creationDate > '2020-01-01';
-- Filter on edge property pushed down to the data source
```

### shortest_path

#### Signatures
```sql
TABLE shortest_path(BIGINT src_vertex_id, BIGINT dst_vertex_id, VARCHAR edge_table_name);
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
