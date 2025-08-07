# Benchmarking Queries

The extension was tested using a set of simple query scenarios:

* **Vertex count** – Returns the total number of vertices. Internally, this is equivalent to selecting all vertices.

  ```sql
  SELECT count(*) as 'vertex count' FROM read_vertices('/data/git/Git.yaml', type='Person');
  ```

* **Edge count** – Returns the total number of edges. Internally, this is equivalent to selecting all edges.

  ```sql
  SELECT count(*) as 'edge count' FROM read_edges('/data/git/Git.yaml', src='Person', type='knows', dst='Person');
  ```

* **3 vertices** – Retrieve 3 vertices with all of their properties.

  ```sql
  SELECT * FROM read_vertices('/data/git/Git.yaml', type='Person') LIMIT 3;
  ```

* **1-hop** – Find immediate neighbors of a specific vertex.

  ```sql
  SELECT _graphArDstIndex as 'dst' FROM read_edges('/data/git/Git.yaml', src='Person', type='knows', dst='Person') WHERE _graphArSrcIndex=42;
  ```

* **2-hop** – Find immediate neighbors of a specific vertex, and then the neighbors of those neighbors.

  ```sql
  SELECT _graphArSrcIndex as 'src', _graphArDstIndex as 'dst' FROM two_hop('/data/git/Person_knows_Person.edge.yaml', vid=42);
  ```

* **Max degree** – Find the vertex with the highest degree (i.e., most connections).

  ```sql
  SELECT max(degree), * FROM edges_vertex('/data/git/Person_knows_Person.yaml');
  ```

* **Path existence** – Check whether there is a path between two vertices.

  ```sql
  SELECT bfs_exist(1, 2, '/data/git/Git.yaml');
  ```

When filtering by vertex ID, the tests used the vertex with the highest degree.

To select target vertices for path queries, the following procedure was used:

1. A 1-hop query was performed from the high-degree vertex – the first and last vertices from the result were selected.
2. A 1-hop query was performed from the last vertex from the previous step – again, the first and last were selected.
3. This was repeated one more time.

As a result, 6 target vertices were chosen:

* 2 vertices within 1 hop,
* 2 within 2 hops,
* 2 within 3 hops.

**Test Environment:**
All benchmarks were performed on a machine with **16 GB RAM** and **4 CPU cores**.

## Benchmark results

- Social network of [GitHub](benchmarks/git.csv) developers
- Social circles from Twitter
- YouTube online social network 
- Social network of Twitch users
- Social circles from Google+
- Pokec online social network
- LiveJournal online social network
- LDBC [sf-30](benchmarks/ldbc-sf-30.csv)
- LDBC sf-300