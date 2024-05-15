#include "rocksdb/graph.h"

namespace ROCKSDB_NAMESPACE {

void inline MergeSortOutEdges(const Edges& existing_edges,
                              const Edges& new_edges, Edges& merged_edges,
                              node_id_t vertex, node_id_t& m,
                              bool is_partial = true,
                              MorrisCounter* mor = NULL) {
  std::vector<node_id_t> delete_edges;
  node_id_t pivot_ex = 0;
  node_id_t pivot_new = 0;
  uint32_t edge_count = 0;
  // sorted_merge
  auto merge_edges_list = new Edge[merged_edges.num_edges_out];
  while (pivot_ex < existing_edges.num_edges_out ||
         pivot_new < new_edges.num_edges_out) {
    bool is_deleted = false;
    for (auto delete_edge : delete_edges) {
      if (delete_edge == existing_edges.nxts_out[pivot_ex].nxt) {
        pivot_ex++;
        is_deleted = true;
        mor->DecayCounter(vertex);
        m--;
      }
    }
    if (is_deleted) continue;
    if (pivot_ex >= existing_edges.num_edges_out) {
      merge_edges_list[edge_count++].nxt = new_edges.nxts_out[pivot_new++].nxt;
      continue;
    }
    if (pivot_new >= new_edges.num_edges_out) {
      merge_edges_list[edge_count++].nxt =
          existing_edges.nxts_out[pivot_ex++].nxt;
      continue;
    }
    if (new_edges.nxts_out[pivot_new].nxt < 0 && !is_partial) {
      delete_edges.push_back(-new_edges.nxts_out[pivot_new++].nxt);
      continue;
    }
    // for (auto delete_edge : delete_edges) {
    //   if (delete_edge == existing_edges.nxts_out[pivot_ex].nxt) {
    //     pivot_ex++;
    //     continue;
    //   }
    // }
    if (existing_edges.nxts_out[pivot_ex].nxt ==
        new_edges.nxts_out[pivot_new].nxt) {
      pivot_ex++;
      mor->DecayCounter(vertex);
      m--;
    } else if (existing_edges.nxts_out[pivot_ex].nxt >
               new_edges.nxts_out[pivot_new].nxt) {
      merge_edges_list[edge_count++].nxt = new_edges.nxts_out[pivot_new++].nxt;
    } else {
      merge_edges_list[edge_count++].nxt =
          existing_edges.nxts_out[pivot_ex++].nxt;
    }
  }
  merged_edges.num_edges_out = edge_count;
  merged_edges.nxts_out = merge_edges_list;
}

void inline MergeSortInEdges(const Edges& existing_edges,
                             const Edges& new_edges, Edges& merged_edges,
                             node_id_t vertex, bool is_partial = true,
                             MorrisCounter* mor = NULL) {
  std::vector<node_id_t> delete_edges;
  node_id_t pivot_ex = 0;
  node_id_t pivot_new = 0;
  uint32_t edge_count = 0;
  // sorted_merge
  auto merge_edges_list = new Edge[merged_edges.num_edges_in];
  while (pivot_ex < existing_edges.num_edges_in ||
         pivot_new < new_edges.num_edges_in) {
    bool is_deleted = false;
    for (auto delete_edge : delete_edges) {
      if (delete_edge == existing_edges.nxts_in[pivot_ex].nxt) {
        pivot_ex++;
        is_deleted = true;
        mor->DecayCounter(vertex);
      }
    }
    if (is_deleted) continue;
    if (pivot_ex >= existing_edges.num_edges_in) {
      merge_edges_list[edge_count++].nxt = new_edges.nxts_in[pivot_new++].nxt;
      continue;
    }
    if (pivot_new >= new_edges.num_edges_in) {
      merge_edges_list[edge_count++].nxt =
          existing_edges.nxts_in[pivot_ex++].nxt;
      continue;
    }
    if (new_edges.nxts_in[pivot_new].nxt < 0 && !is_partial) {
      delete_edges.push_back(-new_edges.nxts_in[pivot_new++].nxt);
      continue;
    }
    if (existing_edges.nxts_in[pivot_ex].nxt ==
        new_edges.nxts_in[pivot_new].nxt) {
      pivot_ex++;
      mor->DecayCounter(vertex);
    } else if (existing_edges.nxts_in[pivot_ex].nxt >
               new_edges.nxts_in[pivot_new].nxt) {
      merge_edges_list[edge_count++].nxt = new_edges.nxts_in[pivot_new++].nxt;
    } else {
      merge_edges_list[edge_count++].nxt =
          existing_edges.nxts_in[pivot_ex++].nxt;
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

  decode_edges(&new_edges, value.ToString(), encoding_type_);
  decode_edges(&existing_edges, existing_value->ToString(), encoding_type_);
  merged_edges.num_edges_out =
      existing_edges.num_edges_out + new_edges.num_edges_out;
  merged_edges.num_edges_in =
      existing_edges.num_edges_in + new_edges.num_edges_in;
  MergeSortOutEdges(existing_edges, new_edges, merged_edges,
                    decode_node(key.data()), *m_, false, morris_);
  MergeSortInEdges(existing_edges, new_edges, merged_edges,
                   decode_node(key.data()), false, morris_);
  free_edges(&existing_edges);
  free_edges(&new_edges);
  encode_edges(&merged_edges, new_value, encoding_type_);
  free_edges(&merged_edges);
  return true;
  logger->Flush();
}

bool RocksGraph::AdjacentListMergeOp::PartialMerge(const Slice& key,
                                                   const Slice& existing_value,
                                                   const Slice& value,
                                                   std::string* new_value,
                                                   Logger* logger) const {
  // printf("Partial Merging\n");
  if (key.size()) {
    if (!existing_value.size()) {
      *new_value = value.ToString();
      return true;
    }
  }
  Edges new_edges, existing_edges, merged_edges;
  decode_edges(&new_edges, value.ToString(), encoding_type_);
  decode_edges(&existing_edges, existing_value.ToString(), encoding_type_);

  merged_edges.num_edges_out =
      existing_edges.num_edges_out + new_edges.num_edges_out;
  merged_edges.num_edges_in =
      existing_edges.num_edges_in + new_edges.num_edges_in;
  MergeSortOutEdges(existing_edges, new_edges, merged_edges,
                    decode_node(key.data()), *m_, true, morris_);
  MergeSortInEdges(existing_edges, new_edges, merged_edges,
                   decode_node(key.data()), true, morris_);
  free_edges(&existing_edges);
  free_edges(&new_edges);
  encode_edges(&merged_edges, new_value, encoding_type_);
  free_edges(&merged_edges);
  return true;
  logger->Flush();
}

bool inline InsertToEdgeList(Edge*& new_list, const Edge* cur_list,
                             node_id_t cur_length, node_id_t insert_id) {
  new_list = new Edge[cur_length + 1];
  node_id_t pivot_ex = 0;
  uint32_t edge_count = 0;
  bool is_merge = false;
  while (pivot_ex < cur_length) {
    if (edge_count == pivot_ex) {
      if (cur_list[pivot_ex].nxt == insert_id) is_merge = true;
      if (cur_list[pivot_ex].nxt > insert_id && !is_merge) {
        new_list[edge_count++].nxt = insert_id;
      } else {
        new_list[edge_count++].nxt = cur_list[pivot_ex++].nxt;
      }
    } else {
      new_list[edge_count++].nxt = cur_list[pivot_ex++].nxt;
    }
  }
  if (cur_list[cur_length - 1].nxt < insert_id || cur_length == 0) {
    new_list[cur_length].nxt = insert_id;
  }
  return is_merge;
}

node_id_t RocksGraph::random_walk(node_id_t start, float decay_factor) {
  node_id_t cur = start;
  for (;;) {
    Edges edges;
    std::string key;
    std::string value;
    encode_node(VertexKey{.id = cur}, &key);
    Status s = db_->Get(ReadOptions(), adj_cf_, key, &value);
    if (!s.ok()) {
      return 0;
    }
    decode_edges(&edges, value, encoding_type_);
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
  VertexKey v{.id = id};
  std::string key, value;
  encode_node(v, &key);
  Edges edges{.num_edges_out = 0, .num_edges_in = 0};
  edges.nxts_out = NULL;
  edges.nxts_in = NULL;
  encode_edges(&edges, &value, encoding_type_);
  free_edges(&edges);
  // if (edge_update_policy_ == EDGE_UPDATE_FULL_LAZY)
  //   return db_->Merge(WriteOptions(), adj_cf_, key, value);
  return db_->Put(WriteOptions(), adj_cf_, key, value);
}

Status RocksGraph::AddEdge(node_id_t from, node_id_t to) {
  // if (filter_type_ == FILTER_TYPE_CMS || FILTER_TYPE_ALL) {
  //   cms_out.UpdateSketch(from);
  //   cms_in.UpdateSketch(to);
  // }
  // if (filter_type_ == FILTER_TYPE_MORRIS || FILTER_TYPE_ALL) {
  //   mor_out.AddCounter(from);
  //   mor_in.AddCounter(to);
  // }

  Status s;
  m++;

  // we insert vertex 'to' into the out edge list of vertex 'from' at first
  // VertexKey v_out{.id = from, .type = KEY_TYPE_ADJENCENT_LIST};
  VertexKey v_out{.id = from};
  std::string key_out, value_out;
  encode_node(v_out, &key_out);
  int out_policy = edge_update_policy_;
  if (out_policy == EDGE_UPDATE_FULL_LAZY) {
    encode_node_hash(v_out, to, &key_out);
  }
  if (out_policy == EDGE_UPDATE_ADAPTIVE) {
    out_policy = AdaptPolicy(from, update_ratio_, lookup_ratio_);
  }
  if (out_policy == EDGE_UPDATE_LAZY || out_policy == EDGE_UPDATE_FULL_LAZY) {
    mor.AddCounter(from);
    Edges edges{.num_edges_out = 1, .num_edges_in = 0};
    edges.nxts_out = new Edge[1];
    edges.nxts_out[0] = Edge{.nxt = to};
    encode_edges(&edges, &value_out, encoding_type_);
    free_edges(&edges);
    s = db_->Merge(WriteOptions(), adj_cf_, key_out, value_out);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  } else if (out_policy == EDGE_UPDATE_EAGER) {
    Edges existing_edges{.num_edges_out = 0, .num_edges_in = 0};
    s = GetAllEdges(from, &existing_edges);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    Edges new_edges{.num_edges_out = existing_edges.num_edges_out + 1,
                    .num_edges_in = existing_edges.num_edges_in};
    // copy existing in edges
    new_edges.nxts_in = new Edge[existing_edges.num_edges_in];
    memcpy(new_edges.nxts_in, existing_edges.nxts_in,
           existing_edges.num_edges_in * sizeof(Edge));
    // insert new out neighbor by order
    bool is_merge =
        InsertToEdgeList(new_edges.nxts_out, existing_edges.nxts_out,
                         existing_edges.num_edges_out, to);
    if (is_merge)
      new_edges.num_edges_out--;
    else
      mor.AddCounter(from);
    if (is_merge) m--;
    std::string new_value;
    encode_edges(&new_edges, &new_value, encoding_type_);
    free_edges(&existing_edges);
    free_edges(&new_edges);
    s = db_->Put(WriteOptions(), adj_cf_, key_out, new_value);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }

  VertexKey v_in{.id = to};
  std::string key_in, value_in;
  encode_node(v_in, &key_in);
  int in_policy = edge_update_policy_;
  if(in_policy == EDGE_UPDATE_FULL_LAZY){
    encode_node_hash(v_in, from, &key_in);
  }
  if (in_policy == EDGE_UPDATE_ADAPTIVE) {
    in_policy = AdaptPolicy(from, update_ratio_, lookup_ratio_);
  }
  if (in_policy == EDGE_UPDATE_LAZY || out_policy == EDGE_UPDATE_FULL_LAZY) {
    mor.AddCounter(to);
    Edges edges{.num_edges_out = 0, .num_edges_in = 1};
    edges.nxts_in = new Edge[1];
    edges.nxts_in[0] = Edge{.nxt = from};
    encode_edges(&edges, &value_in, encoding_type_);
    free_edges(&edges);
    s = db_->Merge(WriteOptions(), adj_cf_, key_in, value_in);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  } else if (in_policy == EDGE_UPDATE_EAGER) {
    Edges existing_edges{.num_edges_out = 0, .num_edges_in = 0};
    s = GetAllEdges(to, &existing_edges);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    Edges new_edges{.num_edges_out = existing_edges.num_edges_out,
                    .num_edges_in = existing_edges.num_edges_in + 1};
    // copy existing out edges
    new_edges.nxts_out = new Edge[existing_edges.num_edges_out];
    memcpy(new_edges.nxts_out, existing_edges.nxts_out,
           existing_edges.num_edges_out * sizeof(Edge));
    bool is_merge = InsertToEdgeList(new_edges.nxts_in,
    existing_edges.nxts_in,
                                     existing_edges.num_edges_in, from);
    if (is_merge)
      new_edges.num_edges_in--;
    else
      mor.AddCounter(to);
    std::string new_value;
    encode_edges(&new_edges, &new_value, encoding_type_);
    free_edges(&existing_edges);
    free_edges(&new_edges);
    s = db_->Put(WriteOptions(), adj_cf_, key_in, new_value);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }
  return s;
}

std::pair<std::string, std::string> RocksGraph::AddEdges(node_id_t v, std::vector<node_id_t>& tos, std::vector<node_id_t>& froms) {
  m += tos.size();
  Edges new_edges{.num_edges_out = tos.size(), .num_edges_in = froms.size()};
  VertexKey v_out{.id = v};
  new_edges.nxts_out = new Edge[new_edges.num_edges_out];
  new_edges.nxts_in = new Edge[new_edges.num_edges_in];
  for (size_t i = 0; i < tos.size(); i++) {
    new_edges.nxts_out[i].nxt = tos[i];
    mor.AddCounter(v);
  }
  for (size_t i =  0; i < froms.size(); i++) {
    new_edges.nxts_in[i].nxt = froms[i];
    mor.AddCounter(v);
  }
  std::string new_value;
  std::string key_out;
  encode_node(v_out, &key_out);
  encode_edges(&new_edges, &new_value, encoding_type_);
  free_edges(&new_edges);
  return std::make_pair(key_out, new_value);
}

Status RocksGraph::DeleteEdge(node_id_t from, node_id_t to) {
  Status s;
  VertexKey v{.id = from};
  std::string key_out, value_out;
  encode_node(v, &key_out);
  int out_policy = edge_update_policy_;
  if (out_policy == EDGE_UPDATE_ADAPTIVE) {
    out_policy = AdaptPolicy(from, update_ratio_, lookup_ratio_);
  }
  if (edge_update_policy_ == EDGE_UPDATE_LAZY &&
      encoding_type_ != ENCODING_TYPE_EFP) {
    Edges edges{.num_edges_out = 1, .num_edges_in = 0};
    edges.nxts_out = new Edge[1];
    edges.nxts_out[0] = Edge{.nxt = -to};
    encode_edges(&edges, &value_out, encoding_type_);
    free_edges(&edges);
    s = db_->Merge(WriteOptions(), adj_cf_, key_out, value_out);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  } else if (edge_update_policy_ == EDGE_UPDATE_EAGER ||
             encoding_type_ == ENCODING_TYPE_EFP) {
    Edges existing_edges{.num_edges_out = 0};
    s = GetAllEdges(from, &existing_edges);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    Edges new_edges{.num_edges_out = existing_edges.num_edges_out,
                    .num_edges_in = existing_edges.num_edges_in};
    new_edges.nxts_in = new Edge[existing_edges.num_edges_in];
    memcpy(new_edges.nxts_in, existing_edges.nxts_in,
           existing_edges.num_edges_in * sizeof(Edge));
    new_edges.nxts_out = new Edge[new_edges.num_edges_out];
    node_id_t pivot_ex = 0;
    uint32_t edge_count = 0;
    while (pivot_ex < existing_edges.num_edges_out) {
      if (existing_edges.nxts_out[pivot_ex].nxt == to) {
        mor.DecayCounter(from);
        m--;
        pivot_ex++;
      } else {
        new_edges.nxts_out[edge_count++].nxt =
            existing_edges.nxts_out[pivot_ex++].nxt;
      }
    }
    new_edges.num_edges_out = edge_count;
    std::string new_value;
    encode_edges(&new_edges, &new_value, encoding_type_);
    free_edges(&existing_edges);
    free_edges(&new_edges);
    s = db_->Put(WriteOptions(), adj_cf_, key_out, new_value);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }

  VertexKey v_in{.id = to};
  std::string key_in, value_in;
  encode_node(v_in, &key_in);
  int in_policy = edge_update_policy_;
  if (in_policy == EDGE_UPDATE_ADAPTIVE) {
    in_policy = AdaptPolicy(from, update_ratio_, lookup_ratio_);
  }
  if (in_policy == EDGE_UPDATE_LAZY && encoding_type_ != ENCODING_TYPE_EFP) {
    Edges edges{.num_edges_out = 0, .num_edges_in = 1};
    edges.nxts_in = new Edge[1];
    edges.nxts_in[0] = Edge{.nxt = -from};
    encode_edges(&edges, &value_in, encoding_type_);
    free_edges(&edges);
    s = db_->Merge(WriteOptions(), adj_cf_, key_in, value_in);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  } else if (in_policy == EDGE_UPDATE_EAGER ||
             encoding_type_ == ENCODING_TYPE_EFP) {
    Edges existing_edges{.num_edges_in = 0};
    s = GetAllEdges(to, &existing_edges);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    Edges new_edges{.num_edges_out = existing_edges.num_edges_out,
                    .num_edges_in = existing_edges.num_edges_in};
    // copy existing out edges
    new_edges.nxts_out = new Edge[existing_edges.num_edges_out];
    memcpy(new_edges.nxts_out, existing_edges.nxts_out,
           existing_edges.num_edges_out * sizeof(Edge));
    new_edges.nxts_in = new Edge[new_edges.num_edges_in];
    node_id_t pivot_ex = 0;
    uint32_t edge_count = 0;
    while (pivot_ex < existing_edges.num_edges_in) {
      if (existing_edges.nxts_in[pivot_ex].nxt == from) {
        mor.DecayCounter(to);
        pivot_ex++;
      } else {
        new_edges.nxts_in[edge_count++].nxt =
            existing_edges.nxts_in[pivot_ex++].nxt;
      }
    }
    new_edges.num_edges_in = edge_count;
    std::string new_value;
    encode_edges(&new_edges, &new_value, encoding_type_);
    free_edges(&existing_edges);
    free_edges(&new_edges);
    s = db_->Put(WriteOptions(), adj_cf_, key_in, new_value);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }
  return s;
}

Status RocksGraph::GetAllEdges(node_id_t src, Edges* edges) {
  VertexKey v{.id = src};
  std::string key;
  encode_node(v, &key);
  std::string value;
  if (edge_update_policy_ == EDGE_UPDATE_FULL_LAZY) {
    std::string start;
    std::string end;
    encode_node(v, &start);
    //start.push_back(static_cast<char>(0x00));
    encode_node(v, &end);
    std::string::iterator sit=end.begin();
    for( ; *(sit+1)!=0; ++sit){}
    *sit = *sit + 1;

    //end.push_back(static_cast<char>(0xFF));
    // std::cout<<"start: "<< start << std::endl;
    // std::cout<<"end: "<< end << std::endl;
    Slice lower_key((char*)start.c_str());
    Slice upper_key((char*)end.c_str());

    rocksdb::ReadOptions read_options;
    read_options.iterate_lower_bound = &lower_key;
    read_options.iterate_upper_bound = &upper_key;

    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));
    int edge_count = 0;
    for (it->Seek(start); it->Valid(); it->Next()) {
      edge_count++;
      it->value();
      // decode_edges(edges,  it->value().ToString(), encoding_type_);
    }
    // printf("edge_count: %d\t", edge_count);
    return Status::OK();
  }
  Status s = db_->Get(ReadOptions(), adj_cf_, key, &value);
  if (!s.ok()) {
    return s;
  }
  decode_edges(edges, value, encoding_type_);
  return Status::OK();
  // if(edge_update_policy_ != EDGE_UPDATE_EAGER){
  // GetMergeOperandsOptions merge_operands_info;
  // int number_of_operands = 0;
  // int maximum_levels = 10;
  // merge_operands_info.expected_max_number_of_operands = maximum_levels;
  // std::vector<PinnableSlice> values(maximum_levels);
  // s = db_->GetMergeOperands(ReadOptions(), adj_cf_, key,
  //                           values.data(), &merge_operands_info,
  //                           &number_of_operands);
  // }
}

node_id_t RocksGraph::GetOutDegree(node_id_t src) {
  VertexKey v{.id = src};
  std::string key;
  encode_node(v, &key);
  std::string value;
  db_->Get(ReadOptions(), adj_cf_, key, &value);
  Edges edges;
  decode_edges(&edges, value, encoding_type_);
  node_id_t result = edges.num_edges_out;
  free_edges(&edges);
  return result;
}

node_id_t RocksGraph::GetInDegree(node_id_t src) {
  VertexKey v{.id = src};
  std::string key;
  encode_node(v, &key);
  std::string value;
  db_->Get(ReadOptions(), adj_cf_, key, &value);
  Edges edges;
  decode_edges(&edges, value, encoding_type_);
  node_id_t result = edges.num_edges_in;
  free_edges(&edges);
  return result;
}

node_id_t RocksGraph::GetDegreeApproximate(node_id_t src,
                                           int filter_type_manual) {
  if (filter_type_manual > 0 && filter_type_ != FILTER_TYPE_ALL) {
    printf(
        "degree filter setting error: filter_type_manual > 0 should only be "
        "set when FILTER_TYPE_ALL\n");
  }
  if (filter_type_manual == FILTER_TYPE_CMS ||
      (filter_type_manual == 0 && filter_type_ == FILTER_TYPE_CMS)) {
    return cms_out.GetVertexCount(src) + cms_in.GetVertexCount(src);
  } else if (filter_type_manual == FILTER_TYPE_MORRIS ||
             (filter_type_manual == 0 && filter_type_ == FILTER_TYPE_MORRIS)) {
    return mor.GetVertexCount(src);
  }
  return 0;
}

// node_id_t RocksGraph::GetOutDegreeApproximate(node_id_t src,
//                                               int filter_type_manual) {
//   if (filter_type_manual > 0 && filter_type_ != FILTER_TYPE_ALL) {
//     printf(
//         "degree filter setting error: filter_type_manual > 0 should only be "
//         "set when FILTER_TYPE_ALL\n");
//   }
//   if (filter_type_manual == FILTER_TYPE_CMS ||
//       (filter_type_manual == 0 && filter_type_ == FILTER_TYPE_CMS)) {
//     return cms_out.GetVertexCount(src);
//   } else if (filter_type_manual == FILTER_TYPE_MORRIS ||
//              (filter_type_manual == 0 && filter_type_ == FILTER_TYPE_MORRIS))
//              {
//     return mor_out.GetVertexCount(src) - mor_out_delete.GetVertexCount(src);
//   }
//   return 0;
// }

// node_id_t RocksGraph::GetInDegreeApproximate(node_id_t src,
//                                              int filter_type_manual) {
//   if (filter_type_manual > 0 && filter_type_ != FILTER_TYPE_ALL) {
//     printf(
//         "degree filter setting error: filter_type_manual > 0 should only be "
//         "set when FILTER_TYPE_ALL\n");
//   }
//   if (filter_type_manual == FILTER_TYPE_CMS ||
//       (filter_type_manual == 0 && filter_type_ == FILTER_TYPE_CMS)) {
//     return cms_in.GetVertexCount(src);
//   } else if (filter_type_manual == FILTER_TYPE_MORRIS ||
//              (filter_type_manual == 0 && filter_type_ == FILTER_TYPE_MORRIS))
//              {
//     return mor_in.GetVertexCount(src) - mor_in_delete.GetVertexCount(src);
//   }
//   return 0;
// }

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