#pragma once
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
using namespace std::chrono;

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/graph.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

int generatePowerLawDegree(double alpha, int minDegree, int maxDegree,
                           std::mt19937& gen) {
  std::uniform_real_distribution<> dis(0.0, 1.0);
  double x = dis(gen);
  double denominator =
      std::pow(minDegree, 1.0 - alpha) - std::pow(maxDegree, 1.0 - alpha);
  double numerator = x * denominator + std::pow(maxDegree, 1.0 - alpha);
  return std::pow(numerator, 1.0 / (1.0 - alpha));
}

RocksGraph* CreateRocksGraph(Options& options, int policy,
                             int encoding = ENCODING_TYPE_NONE,
                             bool reinit = true) {
  return new RocksGraph(options, policy, encoding, reinit);
}

struct Timer {
  Timer() { start_ = std::chrono::steady_clock::now(); }
  std::chrono::steady_clock::time_point start_;
  double Elapsed() {
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start_)
        .count();
  }
};
class GraphBenchProfiler {
 public:
  GraphBenchProfiler() {}
  void AddStat(const std::string& name, double value) {
    if (stats_.find(name) == stats_.end()) {
      stats_[name] = std::vector<double>();
    }
    stats_[name].push_back(value);
  }
  std::string ToString() const {
    std::stringstream ss;
    for (auto& key : stats_) {
      auto sum = get_sum(key.second);
      auto std = get_std(key.second);
      auto mean = sum / key.second.size();
      auto _50_percentile = mean;
      auto _95_percentile = mean + 1.645 * std;
      auto _99_percentile = mean + 2.326 * std;
      ss << key.first << ".micros  P50: " << _50_percentile
         << " P95: " << _95_percentile << " P99: " << _99_percentile
         << " SUM: " << sum << " COUNT: " << key.second.size() << std::endl;
    }
    return ss.str();
  }

 private:
  double get_sum(const std::vector<double>& values) const {
    double sum = 0;
    for (auto v : values) {
      sum += v;
    }
    return sum;
  }
  double get_std(const std::vector<double>& values) const {
    double mean = get_sum(values) / values.size();
    double sum = 0;
    for (auto v : values) {
      sum += (v - mean) * (v - mean);
    }
    return sqrt(sum / values.size());
  }
  std::unordered_map<std::string, std::vector<double>> stats_;
};
class GraphBenchmarkTool {
 public:
  class OpGenerator {
   public:
    OpGenerator(const std::string& workload_file) : file_(workload_file) {
      fin_.open(file_);
    }
    bool Valid() { return !fin_.eof(); }
    void Next(std::string& op, node_id_t& from, node_id_t& to) {
      std::stringstream ss;
      std::string line;
      std::getline(fin_, line);
      ss << line;
      ss >> op >> from >> to;
    }
    ~OpGenerator() { fin_.close(); }

   private:
    std::string file_;
    std::ifstream fin_;
  };

  GraphBenchmarkTool(Options& options, bool is_directed, int policy,
                     int encoding, bool reinit)
      : is_directed_(is_directed),
        policy_(policy),
        encoding_(encoding),
        reinit_(reinit) {
    graph_ = CreateRocksGraph(options, policy_, encoding_, reinit_);
  }

  void LoadGraph(const std::string& graph_file) {
    std::ifstream fin(graph_file);
    std::string line;
    Status s;
    while (std::getline(fin, line)) {
      std::stringstream ss(line);
      node_id_t from, to;
      ss >> from >> to;
      if (is_directed_) {
        s = graph_->AddEdge(from, to);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
      } else {
        s = graph_->AddEdge(from, to);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
        s = graph_->AddEdge(to, from);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
      }
    }
  }

  void InitNodes(node_id_t n) {
    Status s;
    if (policy_ != EDGE_UPDATE_EAGER) {
      for (int i = 0; i < n; i++) {
        s = graph_->AddVertex(i);
        if (!s.ok()) {
          std::cout << "add node error: " << s.ToString() << std::endl;
          exit(0);
        }
      }
    } else {
      graph_->n = n;
    }
  }

  void LoadRandomGraph(node_id_t n, node_id_t m) {
    Status s;
    // srand(time(NULL));
    if (reinit_) InitNodes(n);
    for (int i = 0; i < m; i++) {
      if (i % (m / 100) == 0) {
        printf("\r");
        printf("%f", (i * 100) / (double)m);
        fflush(stdout);
      }
      node_id_t from, to;
      from = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
      from = from % n;
      to = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
      to = to % n;
      if (is_directed_) {
        s = graph_->AddEdge(from, to);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
      } else {
        s = graph_->AddEdge(from, to);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
        s = graph_->AddEdge(to, from);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
      }
    }
    printf("\n");
    return;
  }

  void LoadPowerLawGraph(node_id_t n, double alpha, int min_degree = 4,
                         int max_degree = 1024, int density = 1) {
    std::random_device rd;
    std::mt19937 gen(rd());
    int m_edge_count = 0;
    InitNodes(n);
    Status s;
    for (int i = 0; i < n; i++) {
      if (i % (n / 20) == 0) {
        std::cout << (i * 100) / (double)n << "\t%" << std::endl;
      }
      int degree =
          generatePowerLawDegree(alpha, min_degree, max_degree, gen) * density;
      // printf("degree:%d\n", degree);
      m_edge_count += degree;
      for (int j = 0; j < degree; j++) {
        node_id_t from, to;
        from = i;
        to = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
        to = to % n;
        s = graph_->AddEdge(from, to);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
      }
    }
    printf("M = %d when alpha = %f\n", m_edge_count, alpha);
    return;
  }

  void LoadPowerLawGraphNew(node_id_t n, double alpha, int min_degree = 4,
                            int max_degree = 1024, int density = 1) {
    std::random_device rd;
    std::mt19937 gen(rd());
    int m_edge_count = 0;
    InitNodes(n);
    Status s;
    std::vector<int> degree_array(n);
    node_id_t from, to;
    for (int i = 0; i < n; i++) {
      degree_array[i] =
          generatePowerLawDegree(alpha, min_degree, max_degree, gen) * density;
    }
    for (int t = max_degree; t > 0; t--) {
      if (t % (max_degree / 20) == 0) {
        std::cout << (t * 100) / (double)max_degree << "\t%" << std::endl;
      }
      for (int i = 0; i < n; i++) {
        if (degree_array[i] >= t) {
          m_edge_count++;
          from = i;
          to = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
          to = to % n;
          s = graph_->AddEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        }
      }
    }
    printf("M = %d when alpha = %f\n", m_edge_count, alpha);
    return;
  }

  void RunBenchmark(int operation_num, double update_ratio, node_id_t n) {
    Status s;
    double read_time_spent = 0, write_time_spent = 0;
    struct timespec t1, t2;
    for (int i = 0; i < operation_num; i++) {
      if (rand() / (double)RAND_MAX < update_ratio) {
        node_id_t from, to;
        from = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
        from = from % n;
        to = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
        to = to % n;
        clock_gettime(CLOCK_MONOTONIC, &t1);
        s = graph_->AddEdge(from, to);
        clock_gettime(CLOCK_MONOTONIC, &t2);
        write_time_spent += (t2.tv_sec - t1.tv_sec) +
                            (double)(t2.tv_nsec - t1.tv_nsec) / 1000000000;
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
      } else {
        node_id_t from;
        from = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
        from = from % n;
        Edges edges{.num_edges_out = 0, .num_edges_in = 0};
        clock_gettime(CLOCK_MONOTONIC, &t1);
        s = graph_->GetAllEdges(from, &edges);
        clock_gettime(CLOCK_MONOTONIC, &t2);
        read_time_spent += (t2.tv_sec - t1.tv_sec) +
                           (double)(t2.tv_nsec - t1.tv_nsec) / 1000000000;
        free_edges(&edges);
        // if (!s.ok()) {
        //   std::cout << "add error: " << s.ToString() << std::endl;
        //   exit(0);
        // }
      }
    }
    std::cout << "\nWrite Time: " << write_time_spent
              << "\nRead Time: " << read_time_spent
              << "\nTotal Time: " << write_time_spent + read_time_spent
              << std::endl;
  }

  void TradeOffTest(node_id_t n, Options* opt_handle, int section_num = 10,
                    int section_gap = 4) {
    Status sta;
    InitNodes(n);
    for (int s = 0; s < section_num; s++) {
      std::cout << "sec:" << s << std::endl;
      for (int j = 1; j <= section_gap; j++) {
        for (node_id_t from = 0; from < n; from++) {
          if (from >= s * (n / section_num)) {
            node_id_t to = (from + j + section_gap * s) % n;
            // printf("from = %lld\n", from);
            // printf("to = %lld\n", to);
            sta = graph_->AddEdge(from, to);
            if (!sta.ok()) {
              std::cout << "add error: " << sta.ToString() << std::endl;
            }
          }
        }
      }
    }

    for (int s = 0; s < section_num; s++) {
      // opt_handle->statistics = rocksdb::CreateDBStatistics();
      // opt_handle->statistics.get()->set_stats_level(kExceptDetailedTimers);
      double read_time_spent = 0, write_time_spent = 0;
      struct timespec t1, t2;
      for (int i = 0; i < (n / section_num) * 2; i++) {
        {
          node_id_t from;
          from = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
          from = s * (n / section_num) + (from % (n / section_num));
          Edges edges;
          clock_gettime(CLOCK_MONOTONIC, &t1);
          sta = graph_->GetAllEdges(from, &edges);
          clock_gettime(CLOCK_MONOTONIC, &t2);
          read_time_spent += (t2.tv_sec - t1.tv_sec) +
                             (double)(t2.tv_nsec - t1.tv_nsec) / 1000000000;
          if (!sta.ok()) {
            std::cout << "add error: " << sta.ToString() << std::endl;
            exit(0);
          }
        }
        {
          node_id_t from, to;
          from = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
          from = s * (n / section_num) + (from % (n / section_num));
          to = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
          // to = s * (n / section_num) + (from % (n / section_num));
          to = from + 1;
          clock_gettime(CLOCK_MONOTONIC, &t1);
          sta = graph_->AddEdge(from, to);
          clock_gettime(CLOCK_MONOTONIC, &t2);
          write_time_spent += (t2.tv_sec - t1.tv_sec) +
                              (double)(t2.tv_nsec - t1.tv_nsec) / 1000000000;
          if (!sta.ok()) {
            std::cout << "add error: " << sta.ToString() << std::endl;
            exit(0);
          }
        }
      }
      std::cout << "\nWrite Time: " << write_time_spent
                << "\nRead Time: " << read_time_spent
                << "\nTotal Time: " << write_time_spent + read_time_spent
                << std::endl;
      std::string stat;
      GetRocksGraphStats(stat);
      std::cout << stat << std::endl;
      std::cout << "statistics: " << opt_handle->statistics->ToString()
                << std::endl;
    }
    printLSM();
  }

  void RunDegreeFilterBenchmark() {
    graph_->~RocksGraph();
    rocksdb::Options options;
    auto table_options =
        options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
    table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(5, false));
    options.level_compaction_dynamic_level_bytes = false;
    options.create_if_missing = true;
    options.statistics = rocksdb::CreateDBStatistics();
    options.write_buffer_size = 4 * 1024 * 1024;
    for (double alpha = 2.5; alpha <= 4; alpha = alpha + 0.5) {
      graph_ = CreateRocksGraph(options, false);
      LoadPowerLawGraph(40000, alpha);
      CompareDegreeFilterAccuracy(40000, 40000);
      graph_->~RocksGraph();
    }
  }

  void RandomLookups(node_id_t n, node_id_t m) {
    Status s;
    for (int i = 0; i < m; i++) {
      // if (i % (m / 100) == 0) {
      //  printf("\r");
      // printf("%.1f\t", (i * 100) / (double)m);
      //  fflush(stdout);
      //}
      node_id_t from;
      from = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
      from = from % n;
      Edges edges;
      s = graph_->GetAllEdges(from, &edges);
      if (!s.ok()) {
        std::cout << "get error: " << s.ToString() << std::endl;
        exit(0);
      }
      std::cout << from << " ||\t";
      for (node_id_t i = 0; i < edges.num_edges_out; i++) {
        std::cout << edges.nxts_out[i].nxt << "\t ";
      }
      std::cout << " ||\t";
      for (node_id_t i = 0; i < edges.num_edges_in; i++) {
        std::cout << edges.nxts_in[i].nxt << "\t ";
      }
      std::cout << " ||\t" << edges.num_edges_out + edges.num_edges_in;
      std::cout << " ||\t" << graph_->GetDegreeApproximate(from);
      // std::cout << " ||\t" << graph_->GetInDegreeApproximate(from);
      std::cout << std::endl;
    }
    return;
  }

  void DeleteTest(node_id_t n = 50000, node_id_t d = 10) {
    InitNodes(n);
    Status s;
    for (int i = 1; i <= d; i++) {
      printf("\r");
      printf("%f", (i * 100) / (double)d);
      fflush(stdout);
      for (int j = 0; j < n; j++) {
        node_id_t from, to;
        from = j;
        to = j + i;
        to = to % n;
        if (is_directed_) {
          s = graph_->AddEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        } else {
          s = graph_->AddEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
          s = graph_->AddEdge(to, from);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        }
      }
    }
    for (int i = 1; i <= d; i++) {
      printf("\r");
      printf("%f", (i * 100) / (double)d);
      fflush(stdout);
      for (int j = 0; j < n; j++) {
        node_id_t from, to;
        from = j;
        to = j + i + d;
        to = to % n;
        if (is_directed_) {
          s = graph_->AddEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        } else {
          s = graph_->AddEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
          s = graph_->AddEdge(to, from);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        }
        from = j;
        to = j - i + d / 2;
        to = to % n;
        if (is_directed_) {
          s = graph_->DeleteEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        } else {
          s = graph_->DeleteEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
          s = graph_->DeleteEdge(to, from);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        }
      }
    }
    printf("\n");
    return;
  }

  void PropertyTest(node_id_t n = 10000, node_id_t d = 10) {
    InitNodes(n);
    Status s;
    for (int i = 1; i <= d; i++) {
      printf("\r");
      printf("%f", (i * 100) / (double)d);
      fflush(stdout);
      for (int j = 0; j < n; j++) {
        node_id_t from, to;
        from = j;
        to = j + i;
        to = to % n;
        if (is_directed_) {
          s = graph_->AddEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        } else {
          s = graph_->AddEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
          s = graph_->AddEdge(to, from);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        }
      }
    }

    for (int i = 1; i <= d; i++) {
      printf("\r");
      printf("%f", (i * 100) / (double)d);
      fflush(stdout);
      for (int j = 0; j < n; j++) {
        node_id_t from, to;
        from = j;
        to = j + i;
        to = to % n;
        Property prop{.name = std::to_string(from), .value=std::to_string(to)};
        s = graph_->AddEdgeProperty(from, to, prop);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
      }
    }

    for (int i = 1; i <= d; i++) {
      printf("\r");
      printf("%f", (i * 100) / (double)d);
      fflush(stdout);
      for (int j = 0; j < n; j++) {
        node_id_t from, to;
        from = j;
        to = j + i;
        to = to % n;
        Property prop{.name = "a", .value="b"};
        s = graph_->AddEdgeProperty(from, to, prop);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
      }
    }

    for (int i = 1; i <= d; i++) {
      for (int j = 0; j < n; j += 100) {
        std::vector<Property> props;
        node_id_t from, to;
        from = j;
        to = j + i;
        to = to % n;
        s = graph_->GetEdgeProperty(from, to, props);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
        std::cout << from << "-->" << to << " || ";
        for (Property prop : props) {
          std::cout << prop.name << ": " << prop.value << " || ";
        }
        std::cout << std::endl;
      }
    }
    printf("\n");
    return;
  }

  void VertexPropertyTest(node_id_t n = 10000, node_id_t d = 10) {
    InitNodes(n);
    Status s;
    for (int i = 1; i <= d; i++) {
      printf("\r");
      printf("%f", (i * 100) / (double)d);
      fflush(stdout);
      for (int j = 0; j < n; j++) {
        node_id_t from, to;
        from = j;
        to = j + i;
        to = to % n;
        if (is_directed_) {
          s = graph_->AddEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        } else {
          s = graph_->AddEdge(from, to);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
          s = graph_->AddEdge(to, from);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
        }
      }
    }

    // for (int i = 1; i <= d; i++) {
    //   printf("\r");
    //   printf("%f", (i * 100) / (double)d);
    //   fflush(stdout);
    //   for (int j = 0; j < n; j++) {
    //     node_id_t from, to;
    //     from = j;
    //     to = j + i;
    //     to = to % n;
    //     Property prop{.name = std::to_string(from), .value=std::to_string(to)};
    //     s = graph_->AddVertexProperty(to, prop);
    //     if (!s.ok()) {
    //       std::cout << "add error: " << s.ToString() << std::endl;
    //       exit(0);
    //     }
    //   }
    // }

    for (int i = 1; i <= d; i++) {
      printf("\r");
      printf("%f", (i * 100) / (double)d);
      fflush(stdout);
      for (int j = 0; j < n; j++) {
        node_id_t from, to;
        from = j;
        to = j + i;
        to = to % n;
        Property prop{.name = "a", .value=std::to_string(to)};
        s = graph_->AddVertexProperty(from, prop);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
      }
    }

    for (int i = 1; i <= d; i++) {
      for (int j = 0; j < n; j += 100) {
        node_id_t from, to;
        from = j;
        to = j + i;
        to = to % n;
        Property prop{.name = "a", .value=std::to_string(to)};
        std::vector<node_id_t> vertices;
        vertices = graph_->GetVerticesWithProperty(prop);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
        std::cout << from << "-->" << to << " || ";
        for (node_id_t v : vertices) {
          std::cout << "vertices" << ": " << v << " || ";
        }
        std::cout << std::endl;
      }
    }
    printf("\n");
    return;
  }

  void TinyExample(){
    node_id_t vertex1 = 1;
    node_id_t vertex2 = 2;
    node_id_t vertex3 = 3;

    if (!graph_->AddVertex(vertex1).ok() || !graph_->AddVertex(vertex2).ok() || !graph_->AddVertex(vertex3).ok()) {
        std::cerr << "Error adding vertices" << std::endl;
        return;
    }

    if (!graph_->AddEdge(vertex1, vertex2).ok() || !graph_->AddEdge(vertex1, vertex3).ok()) {
        std::cerr << "Error adding edges" << std::endl;
        return;
    }

    Edges edges;
    if (graph_->GetAllEdges(vertex1, &edges).ok()) {
        std::cout << "Edges from vertex " << vertex1 << ":\n";
        for (uint32_t i = 0; i < edges.num_edges_out; ++i) {
            std::cout << " -> " << edges.nxts_out[i].nxt << std::endl;
        }
        free_edges(&edges);
    } else {
        std::cerr << "Error retrieving edges from vertex " << vertex1 << std::endl;
    }

    if (graph_->DeleteEdge(vertex1, vertex2).ok()) {
        std::cout << "Edge from vertex " << vertex1 << " to vertex " << vertex2 << " deleted" << std::endl;
    } else {
        std::cerr << "Error deleting edge from vertex " << vertex1 << " to vertex " << vertex2 << std::endl;
    }

    if (graph_->GetAllEdges(vertex1, &edges).ok()) {
        std::cout << "Edges from vertex " << vertex1 << ":\n";
        for (uint32_t i = 0; i < edges.num_edges_out; ++i) {
            std::cout << " -> " << edges.nxts_out[i].nxt << std::endl;
        }
        free_edges(&edges);
    } else {
        std::cerr << "Error retrieving edges from vertex " << vertex1 << std::endl;
    }

  }

  struct EdgeOperation {
    enum Type { kAdd, kDelete };
    Type type;
    node_id_t from;
    node_id_t to;
  };

  struct EdgePairHash {
    size_t operator()(const std::pair<node_id_t, node_id_t>& pair) const {
      return std::hash<node_id_t>()(pair.first) ^
             (std::hash<node_id_t>()(pair.second) << 1);
    }
  };

  void EdgeInterfaceTest(node_id_t n, node_id_t m, bool mix_delete) {
    if (n <= 0 || m <= 0) {
      std::cout << "EdgeInterfaceTest skipped: invalid sizes." << std::endl;
      return;
    }
    InitNodes(n);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<node_id_t> dist(0, n - 1);

    std::vector<std::pair<node_id_t, node_id_t>> edges;
    edges.reserve(m);
    std::unordered_set<std::pair<node_id_t, node_id_t>, EdgePairHash>
        unique_edges;
    while (edges.size() < static_cast<size_t>(m)) {
      node_id_t from = dist(rng);
      node_id_t to = dist(rng);
      std::pair<node_id_t, node_id_t> edge(from, to);
      if (unique_edges.insert(edge).second) {
        edges.push_back(edge);
      }
    }

    std::vector<EdgeOperation> ops;
    ops.reserve(edges.size() + edges.size() / 5);
    for (const auto& edge : edges) {
      ops.push_back({EdgeOperation::kAdd, edge.first, edge.second});
    }

    if (mix_delete) {
      std::vector<size_t> indices(edges.size());
      for (size_t i = 0; i < edges.size(); ++i) {
        indices[i] = i;
      }
      std::shuffle(indices.begin(), indices.end(), rng);
      const size_t delete_count = edges.size() / 5;
      for (size_t i = 0; i < delete_count; ++i) {
        const auto& edge = edges[indices[i]];
        ops.push_back({EdgeOperation::kDelete, edge.first, edge.second});
      }
    }

    std::shuffle(ops.begin(), ops.end(), rng);

    std::unordered_map<node_id_t, std::unordered_set<node_id_t>>
        expected_out;
    std::unordered_map<node_id_t, std::unordered_set<node_id_t>> expected_in;

    auto apply_add = [&](node_id_t from, node_id_t to) {
      expected_out[from].insert(to);
      expected_in[to].insert(from);
    };
    auto apply_delete = [&](node_id_t from, node_id_t to) {
      auto out_it = expected_out.find(from);
      if (out_it != expected_out.end()) {
        out_it->second.erase(to);
      }
      auto in_it = expected_in.find(to);
      if (in_it != expected_in.end()) {
        in_it->second.erase(from);
      }
    };

    for (const auto& op : ops) {
      Status s;
      if (op.type == EdgeOperation::kAdd) {
        s = graph_->AddEdge(op.from, op.to);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
        apply_add(op.from, op.to);
        if (!is_directed_) {
          s = graph_->AddEdge(op.to, op.from);
          if (!s.ok()) {
            std::cout << "add error: " << s.ToString() << std::endl;
            exit(0);
          }
          apply_add(op.to, op.from);
        }
      } else {
        s = graph_->DeleteEdge(op.from, op.to);
        if (!s.ok()) {
          std::cout << "delete error: " << s.ToString() << std::endl;
          exit(0);
        }
        apply_delete(op.from, op.to);
        if (!is_directed_) {
          s = graph_->DeleteEdge(op.to, op.from);
          if (!s.ok()) {
            std::cout << "delete error: " << s.ToString() << std::endl;
            exit(0);
          }
          apply_delete(op.to, op.from);
        }
      }
    }

    size_t mismatch_nodes = 0;
    size_t mismatch_edges = 0;
    for (node_id_t node = 0; node < n; ++node) {
      Edges edges_read;
      Status s = graph_->GetAllEdges(node, &edges_read);
      std::unordered_set<node_id_t> got_out;
      std::unordered_set<node_id_t> got_in;
      if (s.ok()) {
        for (uint32_t i = 0; i < edges_read.num_edges_out; ++i) {
          got_out.insert(edges_read.nxts_out[i].nxt);
        }
        for (uint32_t i = 0; i < edges_read.num_edges_in; ++i) {
          got_in.insert(edges_read.nxts_in[i].nxt);
        }
        free_edges(&edges_read);
      } else if (!s.IsNotFound()) {
        std::cout << "get error: " << s.ToString() << std::endl;
        exit(0);
      }

      const auto expected_out_it = expected_out.find(node);
      const auto expected_in_it = expected_in.find(node);
      const std::unordered_set<node_id_t> empty_set;
      const auto& expected_out_set =
          expected_out_it == expected_out.end() ? empty_set
                                                : expected_out_it->second;
      const auto& expected_in_set =
          expected_in_it == expected_in.end() ? empty_set
                                              : expected_in_it->second;

      bool match = true;
      if (got_out.size() != expected_out_set.size() ||
          got_in.size() != expected_in_set.size()) {
        match = false;
      } else {
        for (const auto& neighbor : expected_out_set) {
          if (got_out.find(neighbor) == got_out.end()) {
            match = false;
            break;
          }
        }
        if (match) {
          for (const auto& neighbor : expected_in_set) {
            if (got_in.find(neighbor) == got_in.end()) {
              match = false;
              break;
            }
          }
        }
      }
      if (!match) {
        mismatch_nodes++;
        mismatch_edges += std::max(got_out.size(), expected_out_set.size()) +
                          std::max(got_in.size(), expected_in_set.size());
      }
    }

    std::cout << "EdgeInterfaceTest result: nodes=" << n
              << " edges=" << m << " mix_delete=" << (mix_delete ? "true" : "false")
              << " mismatched_nodes=" << mismatch_nodes
              << " mismatch_edges=" << mismatch_edges << std::endl;
    if (mismatch_nodes == 0) {
      std::cout << "EdgeInterfaceTest: PASS" << std::endl;
    } else {
      std::cout << "EdgeInterfaceTest: FAIL" << std::endl;
    }
  }

  void CompareDegreeFilterAccuracy(node_id_t n, node_id_t m) {
    Status s;
    double cms_relative_error = 0;
    double mor_relative_error = 0;
    double cms_absolute_error = 0;
    double mor_absolute_error = 0;
    // std::cout << "Count Min Sketch Size: "
    //           << graph_->GetDegreeFilterSize(FILTER_TYPE_CMS) << " Bytes."
    //           << std::endl;
    std::cout << "Morris Counter Size: "
              << graph_->GetDegreeFilterSize(FILTER_TYPE_MORRIS) << " Bytes."
              << std::endl;
    for (int i = 0; i < m; i++) {
      if (i % (m / 100) == 0) {
        printf("\r");
        printf("%f", (i * 100) / (double)m);
        fflush(stdout);
      }
      node_id_t from, to;
      from = (static_cast<node_id_t>(rand()) << (sizeof(int) * 8)) | rand();
      from = from % n;
      Edges edges;
      s = graph_->GetAllEdges(from, &edges);
      if (!s.ok()) {
        std::cout << "get error: " << s.ToString() << std::endl;
        exit(0);
      }
      int real_degree = edges.num_edges_out + edges.num_edges_in;
      // cms_absolute_error += abs(
      //     real_degree - graph_->GetDegreeApproximate(from, FILTER_TYPE_CMS));
      // cms_relative_error += abs(real_degree - graph_->GetDegreeApproximate(
      //                                             from, FILTER_TYPE_CMS)) /
      //                       (double)real_degree;
      mor_absolute_error += abs(real_degree - graph_->GetDegreeApproximate(
                                                  from));
      if(real_degree!=0){
        mor_relative_error += abs(real_degree - graph_->GetDegreeApproximate(
                                                    from))
                                                    /
                              (double)real_degree;
      }
      // std::cout << from << " ||\t";
      // for (node_id_t i = 0; i < edges.num_edges_out; i++) {
      //   std::cout << edges.nxts_out[i].nxt << "\t";
      // }
    }
    //cms_relative_error = cms_relative_error / m;
    mor_relative_error = mor_relative_error / m;
    //cms_absolute_error = cms_absolute_error / m;
    mor_absolute_error = mor_absolute_error / m;

    // std::cout << "\nCount Min Sketch Relative Error: "
    //           << cms_relative_error * 100 << "%." << std::endl;
    std::cout << "\nMorris Counter Relative Error: " << mor_relative_error * 100
              << "%." << std::endl;
    // std::cout << "Count Min Sketch Absolute Error: " << cms_absolute_error
    //           << "." << std::endl;
    std::cout << "Morris Counter Absolute Error: " << mor_absolute_error << "."
              << std::endl;
    return;
  }

  void Execute(const std::string& workload_file) {
    OpGenerator op_generator(workload_file);
    std::string op;
    node_id_t from, to;
    Status s;
    while (op_generator.Valid()) {
      op_generator.Next(op, from, to);
      if (op == "add") {
        Timer t;
        s = graph_->AddEdge(from, to);
        if (!s.ok()) {
          std::cout << "add error: " << s.ToString() << std::endl;
          exit(0);
        }
        profiler_.AddStat("add_edges", t.Elapsed());
      } else if (op == "get") {
        Timer t;
        Edges edges;
        s = graph_->GetAllEdges(from, &edges);
        free_edges(&edges);
        if (!s.ok()) {
          std::cout << "get error: " << s.ToString() << std::endl;
          exit(0);
        }
        profiler_.AddStat("get_edges", t.Elapsed());
      } else if (op == "walk") {
        Timer t;
        s = graph_->SimpleWalk(from);
        if (!s.ok()) {
          std::cout << "walk error: " << s.ToString() << std::endl;
          exit(0);
        }
        profiler_.AddStat("walk", t.Elapsed());
      }
    }
  }
  ~GraphBenchmarkTool() { delete graph_; }
  void GetRocksGraphStats(std::string& stat) {
    graph_->GetRocksDBStats(stat);
    stat += "\n" + profiler_.ToString();
  }

  void printLSM(int colume = 0) { graph_->printLSM(colume); }

  void SetRatio(double update_ratio, double lookup_ratio) {
    graph_->SetRatio(update_ratio, lookup_ratio);
  }

 private:
  RocksGraph* graph_;
  GraphBenchProfiler profiler_;
  bool is_directed_;
  int policy_;
  int encoding_;
  bool reinit_;
};

}  // namespace ROCKSDB_NAMESPACE
