# Aster — Enhanced Key-value Store for Graph Database

Aster is a high-performance and embeddable key-value storage engine designed for update-intensive + lookup-heavy graph workloads (e.g., OLTP-style neighbor queries + frequent edge/vertex mutations).

> Paper: **“Aster: Enhancing LSM-structures for Scalable Graph Database”** (PACMMOD / SIGMOD 2025)  
> https://arxiv.org/abs/2501.06570

Our implementation is based on RocksDB and extends it with **RocksGraph**, a graph storage and traversal layer implemented based on our Aster paper. This README focuses on:

1. **All features implemented**
2. **The test/driver functionality supported under `graph_test/`**

> Core implementation entry points: `include/rocksdb/graph.h` and `db/graph.cc`.

---

## Feature Overview

### 1. Data Model & Storage Layout

- **Vertex / edge identifiers**: Positive numbers in `node_id_t` (`int64_t`).
- **Adjacency lists**:
  - Each vertex key maps to a value containing its sorted out-edges and in-edges (`Edges`).
  - Serialization handled by `encode_edges` / `decode_edges`.
- **Properties**:
  - Vertex and edge properties live in separate column families: `vprop_val` and `eprop_val`.
  - Each property is a key/value pair (`Property{name, value}`) and multiple properties are concatenated with `\0` separators.
- **Graph metadata**:
  - `GraphMeta` persists vertex and edge counts (`n`, `m`) at `db_path/GraphMeta.log`.
  - Metadata is loaded on startup and written on shutdown.

Relevant code:
- `include/rocksdb/graph.h` (data structures, encoding helpers, metadata I/O)
- `db/graph.cc` (storage logic and graph operations)

### 2. Edge Update Policies

RocksGraph supports multiple edge update policies via `edge_update_policy_`:

- `EDGE_UPDATE_EAGER`
  - Reads the existing adjacency list, merges, and writes it back via `Put`.
- `EDGE_UPDATE_LAZY`
  - Uses merge operators (`AdjacentListMergeOp`) for incremental writes.
- `EDGE_UPDATE_ADAPTIVE`
  - Dynamically chooses eager vs lazy based on update/lookup ratios and degree estimates.


Relevant code:
- `include/rocksdb/graph.h` (`AdaptPolicy`, policy constants)
- `db/graph.cc` (`AddEdge`, `DeleteEdge`)

### 3. Adjacency Encoding

Encoding is controlled by `encoding_type_`:

- `ENCODING_TYPE_NONE`
  - Raw adjacency lists.
- `ENCODING_TYPE_EFP`
  - Elias–Fano–style compression via `graph_encoder.h`.

Relevant code:
- `include/rocksdb/graph.h` (`encode_edges`, `decode_edges`)
- `include/rocksdb/graph_encoder.h`

### 4. Degree Tracking & Approximation

- **Exact**: `GetOutDegree`, `GetInDegree` read the adjacency list.
- **Approximate**: `GetDegreeApproximate` supports:
  - Count-Min Sketch (`FILTER_TYPE_CMS`)
  - Morris Counter (`FILTER_TYPE_MORRIS`)

Relevant code:
- `include/rocksdb/graph.h`
- `db/graph.cc`

### 5. Graph APIs

**Structure & mutation**
- `AddVertex(node_id_t id)`
- `AddEdge(node_id_t from, node_id_t to)`
- `DeleteEdge(node_id_t from, node_id_t to)`
- `GetAllEdges(node_id_t src, Edges* edges)`
- `CountVertex()` / `CountEdge()`

**Bulk/utility**
- `AddEdges(node_id_t from, std::vector<node_id_t>& tos, std::vector<node_id_t>& froms)`
- `AddVertexForBulkLoad()`

**Properties**
- `AddVertexProperty(node_id_t id, Property prop)`
- `AddEdgeProperty(node_id_t from, node_id_t to, Property prop)`
- `GetVertexProperty(node_id_t id, std::vector<Property>& props)`
- `GetEdgeProperty(node_id_t from, node_id_t to, std::vector<Property>& props)`
- `GetVerticesWithProperty(Property prop)`
- `GetEdgesWithProperty(Property prop)`

**Traversal / sampling**
- `SimpleWalk(node_id_t start, float decay_factor)` (random walk)

**Diagnostics**
- `GetRocksDBStats(std::string& stat)`
- `printLSM(int column)`

### 6. Graph Benchmark Tool

The `tools/graph_benchmark.*` utility provides:

- Random graph loading (`LoadRandomGraph`)
- File-based loading (`LoadGraph`)
- Workload execution for `add`, `get`, `walk`
- Latency profiling and RocksDB stats output

---

## graph_test: tests and benchmarks

The `graph_test` directory provides the example driver and a collection of
tests/benchmarks. The main entry point is
`graph_test/graph_example.cc`, with most test logic in
`graph_test/graph_benchmark_new.h`.

### Build and run
1. Build the RocksDB static library: `make static_lib`
2. Build graph tests: `cd graph_test/ && make all`
3. Run the example: `./graph_example --load_mode=tiny`

### graph_example modes and flags
`graph_example` is driven by gflags (see `graph_example.cc`), including:
- `--load_mode=tiny|random|powerlaw`: tiny demo, random graph, or power-law.
- `--load_vertices` / `--load_edges`: random graph size.
- `--powerlaw_alpha`: power-law distribution parameter.
- `--run_lookups` / `--lookup_count`: post-load random lookups.
- `--update_policy` / `--encoding_type`: edge update policy and encoding.
- `--is_directed`: directed vs. undirected graph.
- `--reinit`: delete existing DB before run.
- `--enable_bloom_filter` / `--direct_io`: storage configuration toggles.

### Supported tests and benchmarks
All of the following are implemented in `graph_benchmark_new.h`:

- **TinyExample**: minimal end-to-end example (add vertices/edges, read
  adjacency lists, delete edges).
- **LoadRandomGraph**: random graph construction (fixed vertex/edge counts).
- **LoadPowerLawGraph / LoadPowerLawGraphNew**: power-law graph generation.
- **RandomLookups**: random adjacency reads + degree estimation output.
- **RunBenchmark**: mixed read/write workload based on `update_ratio`.
- **TradeOffTest**: segmented build with read/write timing + LSM stats.
- **RunDegreeFilterBenchmark**: CMS/Morris size and accuracy comparisons.
- **CompareDegreeFilterAccuracy**: sampled error statistics for estimators.
- **DeleteTest**: edge deletion behavior (directed/undirected).
- **PropertyTest**: edge property write/read validation.
- **VertexPropertyTest**: vertex property write + reverse lookup checks.
- **Execute**: replay workload files with `add/get/walk` and latency stats.

---

## License

Aster is licensed the Apache 2.0 License.
