#pragma once
#include <boost/functional/hash.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/variate_generator.hpp>
#include <deque>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <vector>

namespace ROCKSDB_NAMESPACE {
using vertex_id_t = int64_t;

template <typename T>
size_t calculateMemoryUsage(const std::vector<T>& vec) {
    size_t size = vec.capacity() * sizeof(T); 
    return size;
}

template <typename T>
size_t calculateMemoryUsage(const std::vector<std::vector<T>>& vec) {
    size_t size = vec.capacity() * sizeof(std::vector<T>); 
    for (const auto& subVec : vec) {
        size += calculateMemoryUsage(subVec); 
    }
    return size;
}

class CountMinSketch {
 public:
  CountMinSketch() {}

  CountMinSketch(double delta,
                 double epsilon)  // Initializes the data structures
  {
    table_width = ceil(exp(1) / epsilon);
    table_height = ceil(log(1 / delta));
    // std::cout << "table_width: " << table_width << "\t table_height: "
    //           << table_height << std::endl;
    table.resize(table_height);
    for (size_t i = 0; i < table.size(); i++) {
      table.at(i).resize(table_width, 0);
    }
  }

  ~CountMinSketch()  // Cleans up remaining data structures
  {
    ClearAll();
  }

  void ClearAll()  // Clean Up Memory Structures
  {
    return;
  }

  void UpdateSketch(vertex_id_t v) {
    std::vector<vertex_id_t> hashes = HashVertex(v);
    for (size_t i = 0; i < hashes.size(); i++) {
      // printf("%s=>H%d:%d\n",word.c_str(),i,hashes.at(i));
      table[i][hashes.at(i)] += 1;
    }
  }

  std::vector<vertex_id_t> HashVertex(vertex_id_t v) {
    boost::hash<vertex_id_t> vertex_hash;
    boost::mt19937 randGen(static_cast<unsigned int>(vertex_hash(v)));
    boost::uniform_int<vertex_id_t> numrange(0, table_width - 1);
    boost::variate_generator<boost::mt19937&, boost::uniform_int<vertex_id_t>>
        GetRand(randGen, numrange);
    std::vector<vertex_id_t> hashes;
    for (vertex_id_t i = 0; i < table_height; i++) {
      hashes.push_back(GetRand());
    }
    return hashes;
  }

  int GetVertexCount(vertex_id_t v) {
    std::vector<vertex_id_t> hashes = HashVertex(v);
    int mincount = std::numeric_limits<int>::max();
    for (size_t i = 0; i < hashes.size(); i++) {
      if (mincount > table[i][hashes.at(i)]) {
        mincount = table[i][hashes.at(i)];
      }
    }
    return mincount;
  }

  size_t CalcMemoryUsage(){
    return calculateMemoryUsage(table);
  }

 protected:
  std::vector<std::vector<int>> table;
  vertex_id_t table_height, table_width;
};

class MorrisCounter {
 public:
  std::vector<unsigned char> counters;
  int exponent_bits = 3;
  int mantissa_bits = 5;
  std::random_device rd;
  std::mt19937 rand_gen;

  MorrisCounter(vertex_id_t n) : rand_gen(rd()) { counters.resize(n, 0); }

  MorrisCounter() : rand_gen(rd()) { counters.resize(1); }

  ~MorrisCounter() {}

  void AddCounter(vertex_id_t v) {
    while (static_cast<size_t>(v) > counters.size()) {
      vertex_id_t new_size = counters.size() * 2;
      counters.resize(new_size, 0);
    }
    if(counters[v] == UCHAR_MAX) return;
    int exponent = ExtractExponent(counters[v]);
    std::uniform_int_distribution<> dist(1, pow(2, exponent));
    if (dist(rand_gen) == 1) {
      counters[v]++;
    }
  }

  void DecayCounter(vertex_id_t v) {
    if(static_cast<size_t>(v) > counters.size()) return;
    int exponent = ExtractExponent(counters[v]);
    std::uniform_int_distribution<> dist(1, pow(2, exponent));
    if (dist(rand_gen) == 1 && counters[v]!= 0) {
      counters[v]--;
    }
  }

  inline int ExtractExponent(unsigned char counter) {
    unsigned char mask = static_cast<unsigned char>((1 << exponent_bits) - 1)
                         << (8 - exponent_bits);
    return (counter & mask) >> (8 - exponent_bits);
  }

  inline int ExtractMantissa(unsigned char counter) {
    unsigned char mask = static_cast<unsigned char>(1 << mantissa_bits) - 1;
    ;
    return (counter & mask);
  }

  int GetVertexCount(vertex_id_t v) {
    if (static_cast<size_t>(v) > counters.size()) {
      return 0;
    }
    if(counters[v] == UCHAR_MAX) return INT_MAX;
    int exponent = ExtractExponent(counters[v]);
    int mantissa = ExtractMantissa(counters[v]);
    return (pow(2, exponent) - 1) * pow(2, mantissa_bits) +
           pow(2, exponent) * mantissa;
  }

  size_t CalcMemoryUsage(){
    return calculateMemoryUsage(counters);
  }
};

}  // namespace ROCKSDB_NAMESPACE
