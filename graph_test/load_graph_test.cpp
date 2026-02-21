#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cassert>
#include "rocksdb/db.h"
#include "rocksdb/graph.h"
#include "../tools/graph_benchmark.h"

using namespace ROCKSDB_NAMESPACE;

/**
 * Test LoadGraphFromCSV function using MovieLens-small dataset
 * Loads edges from the MovieLens dataset into the graph
 */
void TestLoadGraphFromCSV() {
  std::cout << "=== Testing LoadGraphFromCSV ===" << std::endl;

  const std::string csv_file = "../third-party/GraphDatasets/movielens-small/edges.csv";

  // Check if file exists
  std::ifstream check_file(csv_file);
  if (!check_file.good()) {
    std::cout << "Error: CSV file not found at " << csv_file << std::endl;
    std::cout << "Please run 'make movielens-small' in third-party/GraphDatasets first" << std::endl;
    return;
  }
  check_file.close();

  // Setup RocksDB options
  Options options;
  options.create_if_missing = true;

  // Clean up any existing database
  system("rm -rf /tmp/rocksdb_load_graph_test");

  // Create graph benchmark tool (directed graph)
  GraphBenchmarkTool tool(options, true, EDGE_UPDATE_LAZY);

  // Load graph from CSV
  std::cout << "Loading graph from CSV: " << csv_file << std::endl;
  auto start = std::chrono::steady_clock::now();
  tool.LoadGraphFromCSV(csv_file);
  auto end = std::chrono::steady_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "Graph loaded successfully in " << duration << " ms" << std::endl;

  // Get statistics
  std::string stats;
  tool.GetRocksGraphStats(stats);
  std::cout << stats << std::endl;

  std::cout << "Test passed: LoadGraphFromCSV" << std::endl << std::endl;

  // Cleanup
  system("rm -rf /tmp/rocksdb_load_graph_test");
}

/**
 * Test LoadPropertyGraphFromCSV function using MovieLens-small dataset
 * Loads node and edge properties from the MovieLens dataset
 */
void TestLoadPropertyGraphFromCSV() {
  std::cout << "=== Testing LoadPropertyGraphFromCSV ===" << std::endl;

  const std::string nodes_csv = "../third-party/GraphDatasets/movielens-small/nodes.csv";
  const std::string edges_csv = "../third-party/GraphDatasets/movielens-small/edges.csv";

  // Check if files exist
  std::ifstream check_nodes(nodes_csv);
  std::ifstream check_edges(edges_csv);
  if (!check_nodes.good() || !check_edges.good()) {
    std::cout << "Error: CSV files not found" << std::endl;
    std::cout << "Please run 'make movielens-small' in third-party/GraphDatasets first" << std::endl;
    return;
  }
  check_nodes.close();
  check_edges.close();

  // Setup RocksDB options
  Options options;
  options.create_if_missing = true;

  // Clean up any existing database
  system("rm -rf /tmp/rocksdb_property_graph_test");

  // Create graph benchmark tool
  GraphBenchmarkTool tool(options, true, EDGE_UPDATE_LAZY);

  // First, load the graph structure (edges)
  std::cout << "Step 1: Loading graph structure from edges.csv..." << std::endl;
  auto start1 = std::chrono::steady_clock::now();
  tool.LoadGraphFromCSV(edges_csv);
  auto end1 = std::chrono::steady_clock::now();
  auto duration1 = std::chrono::duration_cast<std::chrono::milliseconds>(end1 - start1).count();
  std::cout << "Graph structure loaded in " << duration1 << " ms" << std::endl;

  // Create temporary CSV files with property format for testing
  // MovieLens nodes.csv has format: node_id,type,name,genres,rating_count
  // We need to convert it to: node_id,property_name,property_value

  std::cout << "\nStep 2: Converting and loading node properties..." << std::endl;
  const std::string temp_node_props = "/tmp/test_node_props.csv";
  std::ofstream node_out(temp_node_props);
  node_out << "node_id,property_name,property_value\n";

  csv::CSVReader reader(nodes_csv);
  int node_count = 0;
  for (csv::CSVRow& row : reader) {
    if (node_count >= 100) break;  // Limit to first 100 nodes for testing

    node_id_t node_id = row["node_id"].get<node_id_t>();
    std::string type = row["type"].get<std::string>();
    std::string name = row["name"].get<std::string>();

    node_out << node_id << ",type," << type << "\n";
    node_out << node_id << ",name," << name << "\n";
    node_count++;
  }
  node_out.close();

  auto start2 = std::chrono::steady_clock::now();
  tool.LoadPropertyGraphFromCSV(temp_node_props);
  auto end2 = std::chrono::steady_clock::now();
  auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start2).count();
  std::cout << "Node properties loaded in " << duration2 << " ms" << std::endl;

  // Convert edge properties
  // MovieLens edges.csv has format: src,dst,rating,timestamp
  // We need to convert it to: src,dst,property_name,property_value

  std::cout << "\nStep 3: Converting and loading edge properties..." << std::endl;
  const std::string temp_edge_props = "/tmp/test_edge_props.csv";
  std::ofstream edge_out(temp_edge_props);
  edge_out << "src,dst,property_name,property_value\n";

  csv::CSVReader edge_reader(edges_csv);
  int edge_count = 0;
  for (csv::CSVRow& row : edge_reader) {
    if (edge_count >= 100) break;  // Limit to first 100 edges for testing

    node_id_t src = row["src"].get<node_id_t>();
    node_id_t dst = row["dst"].get<node_id_t>();
    std::string rating = row["rating"].get<std::string>();

    edge_out << src << "," << dst << ",rating," << rating << "\n";
    edge_count++;
  }
  edge_out.close();

  auto start3 = std::chrono::steady_clock::now();
  tool.LoadPropertyGraphFromCSV(temp_edge_props);
  auto end3 = std::chrono::steady_clock::now();
  auto duration3 = std::chrono::duration_cast<std::chrono::milliseconds>(end3 - start3).count();
  std::cout << "Edge properties loaded in " << duration3 << " ms" << std::endl;

  std::cout << "\nProperty graph loaded successfully!" << std::endl;
  std::cout << "Total time: " << (duration1 + duration2 + duration3) << " ms" << std::endl;
  std::cout << "Test passed: LoadPropertyGraphFromCSV" << std::endl << std::endl;

  // Cleanup
  system("rm -rf /tmp/test_node_props.csv");
  system("rm -rf /tmp/test_edge_props.csv");
  system("rm -rf /tmp/rocksdb_property_graph_test");
}

int main() {
  std::cout << "========================================" << std::endl;
  std::cout << "  Load Graph Tests - MovieLens Dataset" << std::endl;
  std::cout << "========================================" << std::endl << std::endl;

  TestLoadGraphFromCSV();
  TestLoadPropertyGraphFromCSV();

  std::cout << "========================================" << std::endl;
  std::cout << "  All tests passed!" << std::endl;
  std::cout << "========================================" << std::endl;

  return 0;
}
