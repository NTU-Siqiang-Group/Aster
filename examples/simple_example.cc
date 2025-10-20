#include <gflags/gflags.h>

#include <chrono>
#include <ctime>
#include <iomanip>
#include <string>

#include "graph_benchmark_new.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/cache.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/env.h"

DEFINE_bool(is_directed, true, "is directed graph");
DEFINE_bool(enable_bloom_filter, true, "enable bloom filter");

void setup_bloom_filter(rocksdb::Options& options) {
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(5, false));
}

int main(int argc, char* argv[]) {
  int edge_update_policy = EDGE_UPDATE_ADAPTIVE;
  int encoding_type = ENCODING_TYPE_NONE;
  bool reinit = true;
  rocksdb::Options options;

  for (int i = 0; i < argc; i++)
  {
    std::string arg = argv[i];
    if (arg == "--update_policy"){
        edge_update_policy = atoi(argv[i + 1]);
    }else if(arg == "--direct_io"){
      options.use_direct_io_for_flush_and_compaction = true;
      options.use_direct_reads = true;
    }else if(arg == "--no_reinit"){
        reinit = false;
    }else if(arg == "--encode"){
        encoding_type = atoi(argv[i + 1]);
    }
  }
  
  if (FLAGS_enable_bloom_filter) {
    setup_bloom_filter(options);
  }
  options.level_compaction_dynamic_level_bytes = false;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.write_buffer_size = 4 * 1024 * 1024;
  options.max_bytes_for_level_base = options.write_buffer_size * options.max_bytes_for_level_multiplier;
  options.statistics = rocksdb::CreateDBStatistics();
  std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(8 * 1024 * 1024);
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  rocksdb::GraphBenchmarkTool tool(options, FLAGS_is_directed, edge_update_policy, encoding_type, reinit);
  // int load_n = 400000;
  // int load_m = 10000000;
  int load_n = 20000;
  int load_m = 200000;
  double update_ratio = 0.5;
  tool.SetRatio(update_ratio, 1 - update_ratio);
  // tool.TradeOffTest(load_n, &options);
  // return 0;
  auto load_start = std::chrono::steady_clock::now();
  tool.LoadRandomGraph(load_n, load_m);
  //tool.LoadPowerLawGraphNew(load_n, 2);
  //tool.DeleteTest();
  //tool.TinyExample();
      
  auto load_end = std::chrono::steady_clock::now();
  std::cout << "put latency: "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(load_end -
                                                                load_start)
                   .count() / (double) load_m
            << std::endl;

  auto exec_start = std::chrono::steady_clock::now();
  return 0;
  int get_n = 100;
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

  // tool.RunBenchmark(load_n, update_ratio, load_n);

  // tool.GetRocksGraphStats(stat);
  // std::cout << stat << std::endl;
  // std::cout << "statistics: " << options.statistics->ToString() << std::endl;

  // tool.printLSM();
}