#include "rocksdb/graph.h"

namespace ROCKSDB_NAMESPACE {

void inline MergeSortOutEdges(const Edges& existing_edges, const Edges& new_edges, Edges& merged_edges, bool is_partial = false){
  std::vector<node_id_t> delete_edges;
  node_id_t pivot_ex = 0;
  node_id_t pivot_new = 0;
  uint32_t edge_count = 0;
  // sorted_merge
  auto merge_edges_list = new Edge[merged_edges.num_edges_out];
  while (pivot_ex < existing_edges.num_edges_out ||
         pivot_new < new_edges.num_edges_out) {
    if (pivot_ex >= existing_edges.num_edges_out) {
      merge_edges_list[edge_count++].nxt = new_edges.nxts_out[pivot_new++].nxt;
      continue;
    }
    if (pivot_new >= new_edges.num_edges_out) {
      merge_edges_list[edge_count++].nxt = existing_edges.nxts_out[pivot_ex++].nxt;
      continue;
    }
    if (new_edges.nxts_out[pivot_new].nxt < 0 && !is_partial) {
      delete_edges.push_back(-new_edges.nxts_out[pivot_new++].nxt);
      continue;
    }
    for(auto delete_edge : delete_edges){
      if(delete_edge == existing_edges.nxts_out[pivot_ex].nxt){
        pivot_ex++;
        continue;
      }
    }
    if (existing_edges.nxts_out[pivot_ex].nxt == new_edges.nxts_out[pivot_new].nxt) {
      pivot_ex++;
    } else if (existing_edges.nxts_out[pivot_ex].nxt >
               new_edges.nxts_out[pivot_new].nxt) {
      merge_edges_list[edge_count++].nxt = new_edges.nxts_out[pivot_new++].nxt;
    } else {
      merge_edges_list[edge_count++].nxt = existing_edges.nxts_out[pivot_ex++].nxt;
    }
  }
  merged_edges.num_edges_out = edge_count;
  merged_edges.nxts_out = merge_edges_list;
}

void inline MergeSortInEdges(const Edges& existing_edges, const Edges& new_edges, Edges& merged_edges, bool is_partial = false){
  std::vector<node_id_t> delete_edges;
  node_id_t pivot_ex = 0;
  node_id_t pivot_new = 0;
  uint32_t edge_count = 0;
  // sorted_merge
  auto merge_edges_list = new Edge[merged_edges.num_edges_in];
  while (pivot_ex < existing_edges.num_edges_in ||
         pivot_new < new_edges.num_edges_in) {
    if (pivot_ex >= existing_edges.num_edges_in) {
      merge_edges_list[edge_count++].nxt = new_edges.nxts_in[pivot_new++].nxt;
      continue;
    }
    if (pivot_new >= new_edges.num_edges_in) {
      merge_edges_list[edge_count++].nxt = existing_edges.nxts_in[pivot_ex++].nxt;
      continue;
    }
    if (new_edges.nxts_in[pivot_new].nxt < 0 && !is_partial) {
      delete_edges.push_back(-new_edges.nxts_in[pivot_new++].nxt);
      continue;
    }
    for(auto delete_edge : delete_edges){
      if(delete_edge == existing_edges.nxts_in[pivot_ex].nxt){
        pivot_ex++;
        continue;
      }
    }
    if (existing_edges.nxts_in[pivot_ex].nxt == new_edges.nxts_in[pivot_new].nxt) {
      pivot_ex++;
    } else if (existing_edges.nxts_in[pivot_ex].nxt >
               new_edges.nxts_in[pivot_new].nxt) {
      merge_edges_list[edge_count++].nxt = new_edges.nxts_in[pivot_new++].nxt;
    } else {
      merge_edges_list[edge_count++].nxt = existing_edges.nxts_in[pivot_ex++].nxt;
    }
  }
  merged_edges.num_edges_in = edge_count;
  merged_edges.nxts_in = merge_edges_list;
}

bool RocksGraph::AdjacentListMergeOp::Merge(const Slice& key,
                                            const Slice* existing_value,
                                            const Slice& value,
                                            std::string* new_value,
                                            Logger* logger) const {
  if (key.size()) {
    if (!existing_value) {
      *new_value = value.ToString();
      return true;
    }
  }
  Edges new_edges, existing_edges, merged_edges;
  
  decode_edges(&new_edges, value.ToString());
  decode_edges(&existing_edges, existing_value->ToString());
  merged_edges.num_edges_out = existing_edges.num_edges_out + new_edges.num_edges_out;
  MergeSortOutEdges(existing_edges, new_edges, merged_edges);
  MergeSortInEdges(existing_edges, new_edges, merged_edges);
  free_edges(&existing_edges);
  free_edges(&new_edges);
  encode_edges(&merged_edges, new_value);
  free_edges(&merged_edges);
  return true;
  logger->Flush();
}

bool RocksGraph::AdjacentListMergeOp::PartialMerge(const Slice& key,
                                                   const Slice& existing_value,
                                                   const Slice& value,
                                                   std::string* new_value,
                                                   Logger* logger) const {
  //printf("Partial Merging\n");
  if (key.size()) {
    if (!existing_value.size()) {
      *new_value = value.ToString();
      return true;
    }
  }
  Edges new_edges, existing_edges, merged_edges;
  decode_edges(&new_edges, value.ToString());
  decode_edges(&existing_edges, existing_value.ToString());

  merged_edges.num_edges_out = existing_edges.num_edges_out + new_edges.num_edges_out;
  MergeSortOutEdges(existing_edges, new_edges, merged_edges, true);
  MergeSortInEdges(existing_edges, new_edges, merged_edges, true);
  free_edges(&existing_edges);
  free_edges(&new_edges);
  encode_edges(&merged_edges, new_value);
  free_edges(&merged_edges);
  return true;
  logger->Flush();
}

node_id_t RocksGraph::random_walk(node_id_t start, float decay_factor) {
  node_id_t cur = start;
  for (;;) {
    Edges edges;
    std::string key;
    std::string value;
    encode_node(VertexKey{.id = cur, .type = KEY_TYPE_ADJENCENT_LIST}, &key);
    Status s = db_->Get(ReadOptions(), adj_cf_, key, &value);
    if (!s.ok()) {
      return 0;
    }
    decode_edges(&edges, value);
    if (edges.num_edges_out == 0) {
      return cur;
    }
    float r = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
    if (r < decay_factor) {
      return cur;
    }
    int idx = rand() % edges.num_edges_out;
    cur = edges.nxts_out[idx].nxt;
    free_edges(&edges);
  }
}

node_id_t RocksGraph::CountVertex() { return n; }

node_id_t RocksGraph::CountEdge() { return m; }

Status RocksGraph::AddVertex(node_id_t id) {
  n++;
  VertexKey v{.id = id, .type = KEY_TYPE_ADJENCENT_LIST};
  std::string key, value;
  encode_node(v, &key);
  Edges edges{.num_edges_out = 0};
  edges.nxts_out = NULL;
  encode_edges(&edges, &value);
  free_edges(&edges);
  return db_->Put(WriteOptions(), adj_cf_, key, value);
}

Status RocksGraph::AddEdge(node_id_t from, node_id_t to) {
  cms_.UpdateSketch(from);

  m++;
  VertexKey v_out{.id = from, .type = KEY_TYPE_ADJENCENT_LIST};
  std::string key_out, value_out;
  encode_node(v_out, &key_out);

  // VertexKey v_in{.id = from, .type = KEY_TYPE_ADJENCENT_LIST};
  // std::string key_in, value_in;
  // encode_node(v_out, &key_in);

  if (is_lazy_) {
    Edges edges{.num_edges_out = 1, .num_edges_in = 0};
    edges.nxts_out = new Edge[1];
    edges.nxts_out[0] = Edge{.nxt = to};
    encode_edges(&edges, &value_out);
    free_edges(&edges);
    Status s = db_->Merge(WriteOptions(), adj_cf_, key_out, value_out);
    // if (!s.ok() && !s.IsNotFound()) {
       return s;
    // } 

    // Edges edges{.num_edges_out = 0, .num_edges_in = 1};
    // edges.nxts_in = new Edge[1];
    // edges.nxts_in[0] = Edge{.nxt = from};
    // encode_edges(&edges, &value_in);
    // free_edges(&edges);
    // return db_->Merge(WriteOptions(), adj_cf_, key_in, value_in);
  }
  Edges existing_edges{.num_edges_out = 0};
  Status s = GetAllEdges(from, &existing_edges);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  Edges new_edges{.num_edges_out = existing_edges.num_edges_out + 1};
  new_edges.nxts_out = new Edge[new_edges.num_edges_out];
  // copy nothing if existing_edges.num_edges_out == 0
  // memcpy(new_edges.nxts_out, existing_edges.nxts_out, existing_edges.num_edges_out *
  // sizeof(Edge));
  node_id_t pivot_ex = 0;
  uint32_t edge_count = 0;
  while (pivot_ex < existing_edges.num_edges_out) {
    if (edge_count == pivot_ex) {
      if (existing_edges.nxts_out[pivot_ex].nxt > to) {
        new_edges.nxts_out[edge_count++].nxt = to;
      } else {
        new_edges.nxts_out[edge_count++].nxt = existing_edges.nxts_out[pivot_ex++].nxt;
      }
    } else {
      new_edges.nxts_out[edge_count++].nxt = existing_edges.nxts_out[pivot_ex++].nxt;
    }
  }
  if (existing_edges.nxts_out[existing_edges.num_edges_out - 1].nxt < to ||
      existing_edges.num_edges_out == 0) {
    new_edges.nxts_out[existing_edges.num_edges_out].nxt = to;
  }
  // new_edges.nxts_out[existing_edges.num_edges_out] = Edge{.nxt = to};
  std::string new_value;
  encode_edges(&new_edges, &new_value);
  free_edges(&existing_edges);
  free_edges(&new_edges);
  return db_->Put(WriteOptions(), adj_cf_, key_out, new_value);
}

Status RocksGraph::DeleteEdge(node_id_t from, node_id_t to) {
  m--;
  VertexKey v{.id = from, .type = KEY_TYPE_ADJENCENT_LIST};
  std::string key, value;
  encode_node(v, &key);
  if (is_lazy_) {
    Edges edges{.num_edges_out = 1};
    edges.nxts_out = new Edge[1];
    edges.nxts_out[0] = Edge{.nxt = -to};
    encode_edges(&edges, &value);
    free_edges(&edges);
    return db_->Merge(WriteOptions(), adj_cf_, key, value);
  }
  Edges existing_edges{.num_edges_out = 0};
  Status s = GetAllEdges(from, &existing_edges);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  Edges new_edges{.num_edges_out = existing_edges.num_edges_out + 1};
  new_edges.nxts_out = new Edge[new_edges.num_edges_out];
  // copy nothing if existing_edges.num_edges_out == 0
  // memcpy(new_edges.nxts_out, existing_edges.nxts_out, existing_edges.num_edges_out *
  // sizeof(Edge));
  node_id_t pivot_ex = 0;
  uint32_t edge_count = 0;
  while (pivot_ex < existing_edges.num_edges_out) {
    if (existing_edges.nxts_out[pivot_ex].nxt == to) {
      pivot_ex++;
    } else {
      new_edges.nxts_out[edge_count++].nxt = existing_edges.nxts_out[pivot_ex++].nxt;
    }
  }
  // new_edges.nxts_out[existing_edges.num_edges_out] = Edge{.nxt = to};
  std::string new_value;
  encode_edges(&new_edges, &new_value);
  free_edges(&existing_edges);
  free_edges(&new_edges);
  return db_->Put(WriteOptions(), adj_cf_, key, new_value);
}

Status RocksGraph::GetAllEdges(node_id_t src, Edges* edges) {
  VertexKey v{.id = src, .type = KEY_TYPE_ADJENCENT_LIST};
  std::string key;
  encode_node(v, &key);
  std::string value;
  Status s = db_->Get(ReadOptions(), adj_cf_, key, &value);
  if (!s.ok()) {
    return s;
  }
  decode_edges(edges, value);
  return Status::OK();
}

node_id_t RocksGraph::OutDegree(node_id_t src) {
  VertexKey v{.id = src, .type = KEY_TYPE_ADJENCENT_LIST};
  std::string key;
  encode_node(v, &key);
  std::string value;
  db_->Get(ReadOptions(), adj_cf_, key, &value);
  Edges edges;
  decode_edges(&edges, value);
  return edges.num_edges_out;
}

// Status RocksGraph::GetVertexVal(node_id_t id, Value* val) {
//   VertexKey v{.id = id, .type = KEY_TYPE_VERTEX_VAL};
//   std::string key;
//   encode_node(v, &key);
//   std::string value;
//   Status s = db_->Get(ReadOptions(), val_cf_, key, &value);
//   if (!s.ok()) {
//     return s;
//   }
//   *val = *reinterpret_cast<const Value*>(value.data());
//   return Status::OK();
// }

// Status RocksGraph::SetVertexVal(node_id_t id, Value val) {
//   VertexKey v{.id = id, .type = KEY_TYPE_VERTEX_VAL};
//   std::string key;
//   encode_node(v, &key);
//   std::string value;
//   value.append(reinterpret_cast<const char*>(&val), sizeof(Value));
//   return db_->Put(WriteOptions(), val_cf_, key, value);
// }

// Status RocksGraph::SimpleWalk(node_id_t start, float decay_factor) {
//   random_walk(start, decay_factor);
//   return Status::OK();
// }

}  // namespace ROCKSDB_NAMESPACE