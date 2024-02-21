#pragma once
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/graph.h"

namespace ROCKSDB_NAMESPACE {

RocksGraph* CreateRocksGraph(Options& options, bool is_lazy = true) {
  return new RocksGraph(options, is_lazy);
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
  GraphBenchmarkTool(Options& options, bool is_directed, bool is_lazy)
      : is_directed_(is_directed), is_lazy_(is_lazy) {
    graph_ = CreateRocksGraph(options, is_lazy);
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
    for (int i = 0; i < n; i++) {
      s = graph_->AddVertex(i);
      if (!s.ok()) {
        std::cout << "add node error: " << s.ToString() << std::endl;
        exit(0);
      }
    }
  }

  void LoadRandomGraph(node_id_t n, node_id_t m) {
    Status s;
    InitNodes(n);
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
      // std::cout << "From: " << from << "\t" << "To: " << to << std::endl;
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

  void RandomLookups(node_id_t n, node_id_t m) {
    Status s;
    for (int i = 0; i < m; i++) {
      if (i % (m / 100) == 0) {
        //printf("\r");
        printf("%f", (i * 100) / (double)m);
        //fflush(stdout);
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
      std::cout << from << " ||\t";
      for (node_id_t i = 0; i < edges.num_edges_out; i++) {
        std::cout << edges.nxts_out[i].nxt << "\t";
      }
      std::cout << std::endl;
    }
    return;
  }

  void CompareDegreeFilterAccuracy(node_id_t n, node_id_t m) {
    Status s;
    double cms_relative_error = 0;
    double mor_relative_error = 0;
    std::cout<<"Count Min Sketch Size: "<< graph_->GetDegreeFilterSize(FILTER_TYPE_CMS) <<  " Bytes." << std::endl;
    std::cout<<"Morris Counter Size: "<< graph_->GetDegreeFilterSize(FILTER_TYPE_MORRIS) <<  " Bytes." << std::endl;
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
      int real_degree = edges.num_edges_out;
      cms_relative_error += abs(real_degree - graph_->GetOutDegreeApproximate(from, FILTER_TYPE_CMS)) / (double)real_degree;
      mor_relative_error += abs(real_degree - graph_->GetOutDegreeApproximate(from, FILTER_TYPE_MORRIS)) / (double)real_degree;
      // std::cout << from << " ||\t";
      // for (node_id_t i = 0; i < edges.num_edges_out; i++) {
      //   std::cout << edges.nxts_out[i].nxt << "\t";
      // }
    }
    cms_relative_error = cms_relative_error / m;
    mor_relative_error = mor_relative_error / m;

    std::cout<<"\nCount Min Sketch Relative Error: "<< cms_relative_error * 100 <<  "%." << std::endl;
    std::cout<<"Morris Counter Relative Error: "<< mor_relative_error * 100 <<  "%." << std::endl;
    return;
  }

  void LoadRandomPowerGraph(node_id_t n, node_id_t m) { return; }

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

  void printLSM() { graph_->printLSM(); }

 private:
  RocksGraph* graph_;
  GraphBenchProfiler profiler_;
  bool is_directed_;
  bool is_lazy_;
};

}  // namespace ROCKSDB_NAMESPACE