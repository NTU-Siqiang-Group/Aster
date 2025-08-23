#include "rocksdb/sst_file_writer.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "rocksdb/graph.h"

#include <string>
#include <filesystem>
#include <vector>
#include <gflags/gflags.h>
#include <unordered_map>
#include <fstream>
#include <algorithm>

DEFINE_string(dataset, "", "graph dataset");
DEFINE_bool(is_undirected, false, "is undirected graph");

std::unordered_map<uint64_t, std::vector<rocksdb::node_id_t>> out_adj_lists;
std::unordered_map<uint64_t, std::vector<rocksdb::node_id_t>> in_adj_lists;

rocksdb::Options options;

void read_graph() {
  std::ifstream infile(FLAGS_dataset);
  int out, in;
  uint64_t degree = 0;
  while (infile >> out >> in) {
    out_adj_lists[out].push_back(in);
    in_adj_lists[in].push_back(out);
    degree ++;
    if (FLAGS_is_undirected) {
      out_adj_lists[in].push_back(out);
      in_adj_lists[out].push_back(in);
      degree ++;
    }
  }
  std::cout << "read graph finished: " << out_adj_lists.size() << " vertices and "
    << degree << " edges." << std::endl; 
}

rocksdb::SstFileWriter* get_sst_writer() {
  auto writer = new rocksdb::SstFileWriter (rocksdb::EnvOptions(), options);
  auto s = writer->Open("/tmp/ingest.sst");
  if (!s.ok()) {
    std::cout << "Fail to create sst writer: " << s.ToString() << std::endl;
    exit(1);
  }
  return writer;
}

void write_sst(rocksdb::SstFileWriter* writer, rocksdb::RocksGraph* db) {
  std::cout << "start to write sst..." << std::endl;
  std::unordered_map<std::string, std::string> tmp;
  std::vector<std::string> keys;
  for (size_t i = 0; i < out_adj_lists.size(); i++) {
    auto out_v = out_adj_lists[i];
    auto in_v = in_adj_lists[i];
    std::sort(out_v.begin(), out_v.end());
    std::sort(in_v.begin(), in_v.end());
    auto s = db->AddEdges(i, out_adj_lists[i], in_adj_lists[i]);
    db->AddVertexForBulkLoad();
    tmp.insert(s);
    keys.push_back(s.first);
    if (i % 100000 == 0) {
      std::cout << "Finish encoding " << i << " vertices..." << std::endl;
    }
  }
  std::sort(keys.begin(), keys.end());
  std::cout << "Start writing to sst..." << std::endl;
  for (size_t i = 0; i < keys.size(); i++) {
    writer->Put(keys[i], tmp[keys[i]]);
    if (i % 100000 == 0) {
      std::cout << "Finish writing " << i << " vertices..." << std::endl;
    }
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  read_graph();
  auto writer = get_sst_writer();
  options.create_if_missing = true;
  rocksdb::RocksGraph* db = new rocksdb::RocksGraph(options);
  // write sst file
  write_sst(writer, db);
  auto s = writer->Finish();
  if (!s.ok()) {
    std::cout << "Fail to finalize sst file: " << s.ToString() << std::endl;
  }
  // ingest sst file
  rocksdb::IngestExternalFileOptions ifo;
  auto file_path = "/tmp/ingest.sst";
  rocksdb::Status status = db->get_raw_db()->IngestExternalFile({file_path}, ifo);
  if (!status.ok()) {
    std::cout <<  "Fail to ingest file because: " << status.ToString() << std::endl;
  }
  delete writer;
  delete db;
  return 0;
}