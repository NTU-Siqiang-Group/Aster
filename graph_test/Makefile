include ../make_config.mk

CXXFLAGS += -Wno-error=shadow -Wno-shadow
PLATFORM_CXXFLAGS += -Wno-error=shadow -Wno-shadow

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	# CXXFLAGS += -fno-rtti
endif

.PHONY: clean librocksdb

all: graph_example load_graph_test

graph_example: librocksdb graph_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -I../include -O2 -std=c++17 $(PLATFORM_CXXFLAGS) -o$@ ../librocksdb.a $(PLATFORM_LDFLAGS) $(EXEC_LDFLAGS)

load_graph_test: librocksdb load_graph_test.cpp
	$(CXX) $(CXXFLAGS) $@.cpp -I../include -O2 -std=c++17 $(PLATFORM_CXXFLAGS) -o$@ ../librocksdb.a $(PLATFORM_LDFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./graph_example ./load_graph_test

librocksdb:
	cd .. && $(MAKE) static_lib
