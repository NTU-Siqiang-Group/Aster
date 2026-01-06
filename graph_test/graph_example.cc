#include <gflags/gflags.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

#include "graph_benchmark_new.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

DEFINE_bool(is_directed, true, "Use a directed graph");
DEFINE_bool(enable_bloom_filter, true, "Enable bloom filter");
DEFINE_bool(direct_io, false, "Enable direct IO for flush/compaction and reads");
DEFINE_bool(reinit, true, "Destroy existing DB before running");
DEFINE_bool(run_lookups, false, "Run random lookups after load");
DEFINE_bool(test_sketch, false, "Evaluate accuracy of degree sketches");
DEFINE_double(update_ratio, 0.5, "Update ratio for adaptive policy");
DEFINE_double(powerlaw_alpha, 2.0, "Alpha for power-law generator");
DEFINE_int32(update_policy, EDGE_UPDATE_ADAPTIVE, "Edge update policy");
DEFINE_int32(encoding_type, ENCODING_TYPE_NONE, "Edge encoding type");
DEFINE_int32(load_vertices, 20000, "Number of vertices to load");
DEFINE_int32(load_edges, 200000, "Number of edges to load (random mode)");
DEFINE_int32(lookup_count, 100, "Number of random lookups to run");
DEFINE_string(load_mode, "tiny",
              "Load mode: tiny | random | powerlaw");

namespace {
void SetupBloomFilter(rocksdb::Options& options) {
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(5, false));
}

bool IsLoadMode(const std::string& mode) {
  return mode == "tiny" || mode == "random" || mode == "powerlaw";
}
}  // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (!IsLoadMode(FLAGS_load_mode)) {
    std::cerr << "Unknown load mode: " << FLAGS_load_mode
              << ". Use tiny, random, or powerlaw." << std::endl;
    return 1;
  }

  rocksdb::Options options;
  if (FLAGS_enable_bloom_filter) {
    SetupBloomFilter(options);
  }
  if (FLAGS_direct_io) {
    options.use_direct_io_for_flush_and_compaction = true;
    options.use_direct_reads = true;
  }

  options.level_compaction_dynamic_level_bytes = false;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.write_buffer_size = 4 * 1024 * 1024;
  options.max_bytes_for_level_base =
      options.write_buffer_size * options.max_bytes_for_level_multiplier;
  std::shared_ptr<rocksdb::Cache> cache =
      rocksdb::NewLRUCache(8 * 1024 * 1024);
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  rocksdb::GraphBenchmarkTool tool(options, FLAGS_is_directed,
                                   FLAGS_update_policy, FLAGS_encoding_type,
                                   FLAGS_reinit);
  tool.SetRatio(FLAGS_update_ratio, 1 - FLAGS_update_ratio);

  if (FLAGS_load_mode == "tiny") {
    tool.TinyExample();
    return 0;
  }

  const auto load_start = std::chrono::steady_clock::now();
  if (FLAGS_load_mode == "random") {
    tool.LoadRandomGraph(FLAGS_load_vertices, FLAGS_load_edges);
  } else if (FLAGS_load_mode == "powerlaw") {
    tool.LoadPowerLawGraphNew(FLAGS_load_vertices, FLAGS_powerlaw_alpha);
  }
  const auto load_end = std::chrono::steady_clock::now();

  if (FLAGS_load_mode == "random" && FLAGS_load_edges > 0) {
    const auto load_latency =
        std::chrono::duration_cast<std::chrono::nanoseconds>(load_end -
                                                            load_start)
            .count() /
        static_cast<double>(FLAGS_load_edges);
    std::cout << "put latency: " << load_latency << std::endl;
  }

  if(FLAGS_test_sketch){
    tool.CompareDegreeFilterAccuracy(FLAGS_load_vertices, 10000);
  }

  if (!FLAGS_run_lookups) {
    return 0;
  }

  const auto exec_start = std::chrono::steady_clock::now();
  tool.RandomLookups(FLAGS_load_vertices, FLAGS_lookup_count);
  const auto exec_end = std::chrono::steady_clock::now();
  if (FLAGS_lookup_count > 0) {
    const auto lookup_latency =
        std::chrono::duration_cast<std::chrono::nanoseconds>(exec_end -
                                                            exec_start)
            .count() /
        static_cast<double>(FLAGS_lookup_count);
    std::cout << "get latency: " << lookup_latency << std::endl;
  }

  return 0;
}
