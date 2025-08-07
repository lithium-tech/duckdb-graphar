# Function index

## Scalar Functions

| Function                  | Description                                              |
|---------------------------|----------------------------------------------------------|
| [bfs_exist](#bfs_exist)   | Returns true if there is a path between two vertices     |
| [bfs_length](#bfs_length) | Returns the length of shortest path between two vertices |

### bfs_exist

#### Signatures
```sql
BOOL bfs_exist(BIGINT src_vertex_id, BIGINT dst_vertex_id, VARCHAR graph_path)
```

#### DESCRIPTION

Returns answer whether there is a path between two vertices or not.

`graph_path` - relative or absolute path to the GraphAr graph info file. 
Now use first edge type in graph.

This function is implemented using the standard BFS algorithm.

#### Examples
```sql
SELECT bfs_exist(31890, 33914, 'test/data/git/Person_knows_Person.yaml');
-- true;
```

### bfs_length

#### Signatures
```sql
BIGINT bfs_length(BIGINT src_vertex_id, BIGINT dst_vertex_id, VARCHAR graph_path)
```

#### DESCRIPTION
Return the length of the shortest path between two vertices:
- -1: no path found
- 0: start vertices = end person
- \> 0: base case

`graph_path` - relative or absolute path to the GraphAr graph info file. Now use first edge type in graph.

This function is implemented using the standard BFS algorithm.

#### Examples
```sql
SELECT bfs_length(31890, 33914, 'test/data/git/Person_knows_Person.yaml');
-- 2;
```

## Table Functions

| Function                        | Description                                            |
|---------------------------------|--------------------------------------------------------|
| [read_vertices](#read_vertices) | Returns a Table of Vertices by Type                    |
| [read_edges](#read_edges)       | Returns a Table of Edges by Type of src, edge, dst     |
| [edges_vertex](#edges_vertex)   | Returns a Table with Degree of vertex for src vertices |
| [two_hop](#two_hop)             | Returns a Table with 2-hop edges of vertex             |

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