add_executable(graph_example
  graph_example.cc)
target_link_libraries(graph_example
  ${ROCKSDB_LIB})
