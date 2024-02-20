#pragma once
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/degree_approximate_counter.h"

#include <iostream>

namespace ROCKSDB_NAMESPACE {
using node_id_t = int64_t;
using edge_id_t = int64_t;

#define KEY_TYPE_ADJENCENT_LIST 0x0
#define KEY_TYPE_VERTEX_VAL 0x1

#define FILTER_TYPE_NONE 0x0
#define FILTER_TYPE_CMS 0x1
#define FILTER_TYPE_MORRIS 0x2

// a 4 byte value
union Value {
  uint32_t val;
  //float fval;
};

// 4+8=12byte
struct Edge {
  //Value val;
  node_id_t nxt;
};

// 4+12x bytes
struct Edges {
  uint32_t num_edges_out = 0;
  uint32_t num_edges_in = 0;
  Edge* nxts_out = NULL;
  Edge* nxts_in = NULL;
};

// 8 + 4 byte
struct VertexKey {
  node_id_t id;
  int type;
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
  byte_to_fill = sizeof(v.type);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    key->push_back((v.type >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
}

void inline decode_node(VertexKey* v, const std::string& key) {
  *v = *reinterpret_cast<const VertexKey*>(key.data());
}

void inline encode_edge(const Edge* edge, std::string* value) {
  // int byte_to_fill = sizeof(Value);
  // for (int i = byte_to_fill - 1; i >= 0; i--) {
  //   value->push_back((edge->val.val >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  // }
  int byte_to_fill = sizeof(edge->nxt);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    value->push_back((edge->nxt >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
}

void inline encode_edges(const Edges* edges, std::string* value) {
  // copy the number of edges
  // value->append(reinterpret_cast<const char*>(edges), sizeof(int) + edges->num_edges_out * sizeof(Edge));
  int byte_to_fill = sizeof(edges->num_edges_out);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    value->push_back((edges->num_edges_out >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    value->push_back((edges->num_edges_in >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
  // std::cout << "num_edges_out: " << edges->num_edges_out << std::endl;
  // Edges n;
  // decode_edges(&n, *value);
  // std::cout << "num_edges_out: " << n.num_edges_out << std::endl;
  // exit(0);
  for (uint32_t i = 0; i < edges->num_edges_out; i++) {
    encode_edge(&edges->nxts_out[i], value);
  }
  // std::cout << "num_edges_in: " << edges->num_edges_in << std::endl;
  for (uint32_t i = 0; i < edges->num_edges_in; i++) {
    encode_edge(&edges->nxts_in[i], value);
  }
}

void inline decode_edges(Edges* edges, const std::string& value) {
  edges->num_edges_out = *reinterpret_cast<const uint32_t*>(value.data());
  edges->num_edges_in = *reinterpret_cast<const uint32_t*>(value.data() + sizeof(uint32_t));
  edges->nxts_out = new Edge[edges->num_edges_out];
  memcpy(edges->nxts_out, value.data() + sizeof(uint32_t) * 2, edges->num_edges_out * sizeof(Edge));
  edges->nxts_in = new Edge[edges->num_edges_in];
  memcpy(edges->nxts_in, value.data() + sizeof(uint32_t) * 2 + edges->num_edges_out * sizeof(Edge), edges->num_edges_in * sizeof(Edge));
}

void inline free_edges(Edges* edges) {
  delete[] edges->nxts_out;
  delete[] edges->nxts_in;
}

class RocksGraph {
 public:
  class AdjacentListMergeOp : public AssociativeMergeOperator {
   public:
    virtual ~AdjacentListMergeOp() override {};
    virtual bool Merge(const Slice& key, const Slice* existing_value,
                     const Slice& value, std::string* new_value,
                     Logger* logger) const override;
    virtual bool PartialMerge(const Slice& key, const Slice& existing_value,
                     const Slice& value, std::string* new_value,
                     Logger* logger) const override;
    virtual const char* Name() const override {
      return "AdjacentListMergeOp";
    }
  };
  RocksGraph(Options& options, bool lazy=true): n(0), m(0), is_lazy_(lazy), cms_() {
    if (lazy) {
      options.merge_operator.reset(new AdjacentListMergeOp);
    }
    if(filter_type_ == FILTER_TYPE_CMS){
      cms_ = CountMinSketch(cms_delta, cms_epsilon);
    }
    options.create_missing_column_families = true;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(kDefaultColumnFamilyName, options);
    column_families.emplace_back("vertex_val", options);
    std::vector<ColumnFamilyHandle*> handles;
    std::string kDBPath = "/tmp/demo";
    DestroyDB(kDBPath, options);
    Status s = DB::Open(options, kDBPath, column_families, &handles, &db_); 
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      exit(1);
    }
    adj_cf_ = handles[0];
    val_cf_ = handles[1];
  }
  ~RocksGraph() {
    db_->DestroyColumnFamilyHandle(adj_cf_);
    db_->DestroyColumnFamilyHandle(val_cf_);
    db_->Close();
    delete db_;
  }
  node_id_t CountVertex();
  node_id_t CountEdge();
  Status AddVertex(node_id_t id);
  Status AddEdge(node_id_t from, node_id_t to);
  Status DeleteEdge(node_id_t from, node_id_t to);
  Status GetAllEdges(node_id_t src, Edges* edges);
  node_id_t OutDegree(node_id_t id);
  Status SimpleWalk(node_id_t start, float decay_factor=0.20);
  void GetRocksDBStats(std::string& stat) {
    db_->GetProperty("rocksdb.stats", &stat);
  }

  // Status GetVertexVal(node_id_t id, Value* val);
  // Status SetVertexVal(node_id_t id, Value val);

  void printLSM() {
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

      std::cout << "level " << level.level << ".  Size " << level_size << " bytes. "
          << std::endl;
      // for (auto file : level.files) {
      // printf("%s   ", file.name.c_str());
      // }
      std::cout << std::endl;
      for (auto file : level.files) {
        //cout << " \t " << file.size << " bytes \t " << file.smallestkey << "-"
        //     << file.largestkey;
        std::cout << "    " << file.name << std::endl;
      }
      // if (level.level == largest_used_level) {
      //   break;
      // }
    }
    std::cout << std::endl;
  }

 private:
  node_id_t random_walk(node_id_t start, float decay_factor=0.20);
  node_id_t n, m;
  DB* db_;
  bool is_lazy_;
  int filter_type_ = 1;
  ColumnFamilyHandle* val_cf_, *adj_cf_;
  CountMinSketch cms_;
  double cms_delta = 0.1;
  double cms_epsilon = 1.0/10000;

};


}
