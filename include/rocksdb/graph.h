#pragma once
#include <fstream>
#include <iostream>
#include <limits>

#include "rocksdb/advanced_options.h"
#include "rocksdb/db.h"
#include "rocksdb/degree_approximate_counter.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/graph_encoder.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
using node_id_t = int64_t;
using edge_id_t = int64_t;

#define EDGE_UPDATE_EAGER 0x0
#define EDGE_UPDATE_LAZY 0x1
#define EDGE_UPDATE_ADAPTIVE 0x2

#define KEY_TYPE_ADJENCENT_LIST 0x0
#define KEY_TYPE_VERTEX_VAL 0x1

#define ENCODING_TYPE_NONE 0x0
#define ENCODING_TYPE_EFP 0x1

#define FILTER_TYPE_NONE 0x0
#define FILTER_TYPE_CMS 0x1
#define FILTER_TYPE_MORRIS 0x2
#define FILTER_TYPE_ALL 0x3  // this option is purely for comparison test

// a 4 byte value
union Value {
  uint32_t val;
  // float fval;
};

// 4+8=12byte
struct Edge {
  // Value val;
  node_id_t nxt;
};
// using Edge = int64_t;

// 4+12x bytes
struct Edges {
  uint32_t num_edges_out = 0;
  uint32_t num_edges_in = 0;
  Edge* nxts_out = NULL;
  Edge* nxts_in = NULL;
};

struct EdgesEncodedEF {
  uint32_t num_edges_out = 0;
  uint32_t num_edges_in = 0;
  bit_vector bv_out;
  bit_vector bv_in;
};

// 8 + 4 byte
struct VertexKey {
  node_id_t id;
  // int type;
};

// void decode_node(VertexKey* v, const std::string& key);
// void decode_edges(Edges* edges, const std::string& value);
// void encode_node(VertexKey v, std::string* key);
// void encode_edge(const Edge* edge, std::string* value);
// void encode_edges(const Edges* edges, std::string* value);

void inline encode_node(VertexKey v, std::string* key) {
  // key->append(reinterpret_cast<const char*>(&v), sizeof(VertexKey));
  int byte_to_fill = sizeof(v.id);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    key->push_back((v.id >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
  // byte_to_fill = sizeof(v.type);
  // for (int i = byte_to_fill - 1; i >= 0; i--) {
  //   key->push_back((v.type >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  // }
}

void inline decode_node(VertexKey* v, const std::string& key) {
  *v = *reinterpret_cast<const VertexKey*>(key.data());
}

node_id_t inline decode_node(const std::string& key) {
  return *reinterpret_cast<const node_id_t*>(key.data());
}

void inline encode_edge(const Edge* edge, std::string* value) {
  // int byte_to_fill = sizeof(Value);
  // for (int i = byte_to_fill - 1; i >= 0; i--) {
  //   value->push_back((edge->val.val >> ((byte_to_fill - i - 1) << 3)) &
  //   0xFF);
  // }
  // int byte_to_fill = sizeof(edge->nxt);
  // for (int i = byte_to_fill - 1; i >= 0; i--) {
  //   value->push_back((edge->nxt >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  // }
  int byte_to_fill = sizeof(edge->nxt);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    value->push_back((edge->nxt >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
}

void inline encode_edges(
    const Edges* edges, std::string* value,
    int encoding_type = ENCODING_TYPE_NONE,
    node_id_t universe = std::numeric_limits<uint32_t>::max()) {
  // copy the number of edges
  // value->append(reinterpret_cast<const char*>(edges), sizeof(int) +
  // edges->num_edges_out * sizeof(Edge));
  int byte_to_fill = sizeof(edges->num_edges_out);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    value->push_back((edges->num_edges_out >> ((byte_to_fill - i - 1) << 3)) &
                     0xFF);
  }
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    value->push_back((edges->num_edges_in >> ((byte_to_fill - i - 1) << 3)) &
                     0xFF);
  }
  // std::cout << "num_edges_out: " << edges->num_edges_out << std::endl;
  // Edges n;
  // decode_edges(&n, *value);
  // std::cout << "num_edges_out: " << n.num_edges_out << std::endl;
  // exit(0);
  if (encoding_type == ENCODING_TYPE_NONE) {
    for (uint32_t i = 0; i < edges->num_edges_out; i++) {
      encode_edge(&edges->nxts_out[i], value);
    }
    // std::cout << "num_edges_in: " << edges->num_edges_in << std::endl;
    for (uint32_t i = 0; i < edges->num_edges_in; i++) {
      encode_edge(&edges->nxts_in[i], value);
    }
  } else if (encoding_type == ENCODING_TYPE_EFP) {
    global_parameters params;
    {
      auto const ptr = reinterpret_cast<node_id_t*>(edges->nxts_out);
      std::vector<node_id_t> seq_out(ptr, ptr + edges->num_edges_out);
      bit_vector_builder bvb_out;
      if (edges->num_edges_out > 0) {
        uniform_partitioned_sequence<indexed_sequence>::write(
            bvb_out, seq_out.begin(), universe, seq_out.size(), params);
        bvb_out.encode(value);
      }
    }
    {
      auto const ptr = reinterpret_cast<node_id_t*>(edges->nxts_in);
      std::vector<node_id_t> seq_in(ptr, ptr + edges->num_edges_in);
      bit_vector_builder bvb_in;
      if (edges->num_edges_in > 0) {
        uniform_partitioned_sequence<indexed_sequence>::write(
            bvb_in, seq_in.begin(), universe, seq_in.size(), params);
        bvb_in.encode(value);
      }
    }
  }
}

void inline decode_edges(
    Edges* edges, const std::string& value,
    int encoding_type = ENCODING_TYPE_NONE,
    node_id_t universe = std::numeric_limits<uint32_t>::max()) {
  edges->num_edges_out = *reinterpret_cast<const uint32_t*>(value.data());
  edges->num_edges_in =
      *reinterpret_cast<const uint32_t*>(value.data() + sizeof(uint32_t));
  if (encoding_type == ENCODING_TYPE_NONE) {
    edges->nxts_out = new Edge[edges->num_edges_out];
    memcpy(edges->nxts_out, value.data() + sizeof(uint32_t) * 2,
           edges->num_edges_out * sizeof(Edge));
    edges->nxts_in = new Edge[edges->num_edges_in];
    memcpy(edges->nxts_in,
           value.data() + sizeof(uint32_t) * 2 +
               edges->num_edges_out * sizeof(Edge),
           edges->num_edges_in * sizeof(Edge));
  } else if (encoding_type == ENCODING_TYPE_EFP) {
    global_parameters params;
    typename uniform_partitioned_sequence<
        indexed_sequence>::enumerator::value_type val;
    size_t out_offset = 0;

    if (edges->num_edges_out > 0) {
      bit_vector_builder bvb_out;
      bvb_out.decode(value, sizeof(uint32_t) * 2);
      bit_vector bv_out(&bvb_out);
      typename uniform_partitioned_sequence<indexed_sequence>::enumerator r_out(
          bv_out, 0, universe, edges->num_edges_out, params);
      edges->nxts_out = new Edge[edges->num_edges_out];
      for (uint64_t i = 0; i < edges->num_edges_out; ++i) {
        val = r_out.move(i);
        edges->nxts_out[i].nxt = val.second;
      }
      out_offset = bvb_out.get_offset();
    }

    if (edges->num_edges_in > 0) {
      bit_vector_builder bvb_in;
      bvb_in.decode(value, sizeof(uint32_t) * 2 + out_offset);
      bit_vector bv_in(&bvb_in);
      typename uniform_partitioned_sequence<indexed_sequence>::enumerator r_in(
          bv_in, 0, universe, edges->num_edges_in, params);
      edges->nxts_in = new Edge[edges->num_edges_in];
      for (uint64_t i = 0; i < edges->num_edges_in; ++i) {
        val = r_in.move(i);
        edges->nxts_in[i].nxt = val.second;
      }
    }
  }
}

void inline free_edges(Edges* edges) {
  delete[] edges->nxts_out;
  delete[] edges->nxts_in;
}

struct GraphMeta {
  node_id_t n = 0;
  node_id_t m = 0;
};

void inline writeMeta(const std::string& filePath, GraphMeta meta) {
  std::ofstream outFile(filePath, std::ios::binary);
  if (!outFile) {
    return;
    // throw std::runtime_error("Cannot open file for writing");
  }
  outFile.write(reinterpret_cast<const char*>(&meta.n), sizeof(meta.n));
  outFile.write(reinterpret_cast<const char*>(&meta.m), sizeof(meta.m));
  outFile.close();
}

void inline readMeta(const std::string& filePath, GraphMeta& meta) {
  std::ifstream inFile(filePath, std::ios::binary);
  if (!inFile) {
    return;
    // throw std::runtime_error("Cannot open file for reading");
  }
  inFile.read(reinterpret_cast<char*>(&meta.n), sizeof(meta.n));
  inFile.read(reinterpret_cast<char*>(&meta.m), sizeof(meta.m));
  inFile.close();
}

class RocksGraph {
 public:
  node_id_t n, m;
  int filter_type_ = FILTER_TYPE_MORRIS;
  int encoding_type_ = ENCODING_TYPE_NONE;
  int edge_update_policy_ = EDGE_UPDATE_EAGER;
  bool auto_reinitialize_ = false;
  double update_ratio_ = 0.5;
  double lookup_ratio_ = 0.5;
  std::string db_path = "/tmp/demo";
  std::string meta_filename = "/GraphMeta.log";

  class AdjacentListMergeOp : public AssociativeMergeOperator {
   public:
    int encoding_type_;
    MorrisCounter* morris_out_delete_;
    MorrisCounter* morris_in_delete_;
    AdjacentListMergeOp(int encoding_type, MorrisCounter* morris_out_delete,
                        MorrisCounter* morris_in_delete)
        : encoding_type_(encoding_type),
          morris_out_delete_(morris_out_delete),
          morris_in_delete_(morris_in_delete) {}
    virtual ~AdjacentListMergeOp() override{};
    virtual bool Merge(const Slice& key, const Slice* existing_value,
                       const Slice& value, std::string* new_value,
                       Logger* logger) const override;
    virtual bool PartialMerge(const Slice& key, const Slice& existing_value,
                              const Slice& value, std::string* new_value,
                              Logger* logger) const override;
    virtual const char* Name() const override { return "AdjacentListMergeOp"; }
  };
  RocksGraph(Options& options, int edge_update_policy = EDGE_UPDATE_ADAPTIVE,
             int encoding_type = ENCODING_TYPE_NONE,
             bool auto_reinitialize = false)
      : n(0),
        m(0),
        encoding_type_(encoding_type),
        edge_update_policy_(edge_update_policy),
        auto_reinitialize_(auto_reinitialize),
        cms_out(),
        cms_in(),
        mor_out(),
        mor_out_delete(),
        mor_in(),
        mor_in_delete() {
    if (edge_update_policy != EDGE_UPDATE_EAGER) {
      options.merge_operator.reset(new AdjacentListMergeOp(
          encoding_type_, &mor_out_delete, &mor_in_delete));
    }
    auto table_options =
        options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
    table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(5, false));
    options.level_compaction_dynamic_level_bytes = false;
    if (filter_type_ == FILTER_TYPE_CMS || FILTER_TYPE_ALL) {
      cms_out = CountMinSketch(cms_delta, cms_epsilon);
      cms_in = CountMinSketch(cms_delta, cms_epsilon);
    }
    // else if(filter_type_ == FILTER_TYPE_MORRIS){
    // }
    options.create_missing_column_families = true;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(kDefaultColumnFamilyName, options);
    column_families.emplace_back("vertex_val", options);
    std::vector<ColumnFamilyHandle*> handles;
    if (auto_reinitialize_) {
      DestroyDB(db_path, options);
    } else {
      GraphMeta meta;
      readMeta(db_path + meta_filename, meta);
      n = meta.n;
      m = meta.m;
    }
    Status s = DB::Open(options, db_path, column_families, &handles, &db_);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      exit(1);
    }
    adj_cf_ = handles[0];
    val_cf_ = handles[1];
  }

  ~RocksGraph() {
    GraphMeta meta{.m = m, .n = n};
    writeMeta(db_path + meta_filename, meta);
    db_->DestroyColumnFamilyHandle(adj_cf_);
    db_->DestroyColumnFamilyHandle(val_cf_);
    db_->SyncWAL();
    db_->Close();
    // delete db_;
  }
  node_id_t CountVertex();
  node_id_t CountEdge();
  Status AddVertex(node_id_t id);
  Status AddEdge(node_id_t from, node_id_t to);
  Status DeleteEdge(node_id_t from, node_id_t to);
  Status GetAllEdges(node_id_t src, Edges* edges);
  node_id_t GetOutDegree(node_id_t id);
  node_id_t GetInDegree(node_id_t id);
  node_id_t GetOutDegreeApproximate(node_id_t id, int filter_type_manual = 0);
  node_id_t GetInDegreeApproximate(node_id_t id, int filter_type_manual = 0);
  Status SimpleWalk(node_id_t start, float decay_factor = 0.20);
  void GetRocksDBStats(std::string& stat) {
    db_->GetProperty("rocksdb.stats", &stat);
  }

  size_t GetDegreeFilterSize(int filter_type) {
    if (filter_type == FILTER_TYPE_CMS) {
      return cms_out.CalcMemoryUsage();
    } else if (filter_type == FILTER_TYPE_MORRIS) {
      return mor_out.CalcMemoryUsage();
    }
    return 0;
  }

  void SetRatio(double update_ratio, double lookup_ratio) {
    update_ratio_ = update_ratio;
    lookup_ratio_ = lookup_ratio;
  }

  int AdaptPolicy(node_id_t src, double update_ratio, double lookup_ratio) {
    node_id_t block_size = 2 << 11;
    node_id_t vertex_space = sizeof(node_id_t);
    node_id_t edge_space = sizeof(edge_id_t);
    // node_id_t degree = is_out_edge ? GetOutDegreeApproximate(src)
    //                                : GetInDegreeApproximate(src);
    node_id_t degree =
        GetOutDegreeApproximate(src) + GetInDegreeApproximate(src);
    double level_num = 1.9;
    double WA =
        db_->GetOptions().max_bytes_for_level_multiplier * level_num;
    double cache_miss_rate = 1.0;
    double left =
        cache_miss_rate * (1 + (double)(vertex_space + edge_space * degree) /
                                   (double)block_size) +
        (double)(edge_space * (degree - 1)) * WA / (double)(block_size);
    double right =
        cache_miss_rate * ((double)m / (double)n) *
        (lookup_ratio /
         double(db_->GetOptions().max_bytes_for_level_multiplier - 1) /
         update_ratio);
    // if (degree < 256) {
    //   return EDGE_UPDATE_EAGER;
    // } else {
    //   return EDGE_UPDATE_LAZY;
    // }
    if (left < right) {
      return EDGE_UPDATE_EAGER;
    } else {
      return EDGE_UPDATE_LAZY;
    }
  }

  // Status GetVertexVal(node_id_t id, Value* val);
  // Status SetVertexVal(node_id_t id, Value val);

  void printLSM() {
    std::cout << "n = " << n << std::endl;
    std::cout << "m = " << m << std::endl;
    ColumnFamilyMetaData cf_meta;
    db_->GetColumnFamilyMetaData(adj_cf_, &cf_meta);
    std::cout << "Print LSM" << std::endl;
    // int largest_used_level = 0;
    // for (auto level : cf_meta.levels) {
    //   if (level.files.size() > 0) {
    //     largest_used_level = level.level;
    //   }
    // }
    for (auto level : cf_meta.levels) {
      long level_size = 0;
      for (auto file : level.files) {
        level_size += file.size;
      }
      std::cout << "level " << level.level << ".  Size " << level_size
                << " bytes. " << std::endl;
      // for (auto file : level.files) {
      // printf("%s   ", file.name.c_str());
      // }
      std::cout << std::endl;
      for (auto file : level.files) {
        // cout << " \t " << file.size << " bytes \t " << file.smallestkey <<
        // "-"
        //      << file.largestkey;
        std::cout << "    " << file.name << std::endl;
      }
      // if (level.level == largest_used_level) {
      //   break;
      // }
    }
    std::cout << std::endl;
  }

 private:
  node_id_t random_walk(node_id_t start, float decay_factor = 0.20);
  DB* db_;
  // bool is_lazy_;
  ColumnFamilyHandle *val_cf_, *adj_cf_;
  CountMinSketch cms_out;
  CountMinSketch cms_in;
  MorrisCounter mor_out;
  MorrisCounter mor_out_delete;
  MorrisCounter mor_in;
  MorrisCounter mor_in_delete;
  double cms_delta = 0.1;
  double cms_epsilon = 1.0 / 12000;
};

}  // namespace ROCKSDB_NAMESPACE
