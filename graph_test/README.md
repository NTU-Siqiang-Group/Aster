1. Compile RocksDB first by executing `make static_lib` in parent dir
2. Compile graph tests: `cd graph_test/; make all`
3. Run the graph example: `./graph_example --load_mode=tiny`
