#include <gflags/gflags.h>

#include <chrono>
#include <ctime>
#include <iomanip>

#include "graph_benchmark_new.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

DEFINE_bool(is_directed, true, "is directed graph");
DEFINE_bool(enable_bloom_filter, true, "enable bloom filter");

void setup_bloom_filter(rocksdb::Options& options) {
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(5, false));
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int edge_update_policy = EDGE_UPDATE_LAZY;

  rocksdb::Options options;
  if (FLAGS_enable_bloom_filter) {
    setup_bloom_filter(options);
  }
  options.level_compaction_dynamic_level_bytes = false;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.write_buffer_size = 4 * 1024 * 1024;
  rocksdb::GraphBenchmarkTool tool(options, FLAGS_is_directed, edge_update_policy);

  int load_n = 10000;
  int load_m = 10000;
  auto load_start = std::chrono::steady_clock::now();
  tool.LoadRandomGraph(load_n, load_m);
  //tool.LoadPowerLawGraph(40000, 2.5);
      
  auto load_end = std::chrono::steady_clock::now();
  std::cout << "put latency: "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(load_end -
                                                                load_start)
                   .count() / (double) load_m
            << std::endl;

  auto exec_start = std::chrono::steady_clock::now();
  int get_n = 1000000;
  tool.RandomLookups(load_n, get_n);
  // tool.CompareDegreeFilterAccuracy(40000, 40000);
  // tool.Execute("/home/junfeng/Desktop/dataset/soc-pokec/workload2.txt");
  auto exec_end = std::chrono::steady_clock::now();
  std::cout << "get latency: "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(exec_end -
                                                                exec_start)
                   .count() / (double) get_n
            << std::endl;

  // std::string stat;
  // tool.GetRocksGraphStats(stat);
  // std::cout << stat << std::endl;
  // std::cout << "statistics: " << options.statistics->ToString() << std::endl;

  tool.printLSM();
}