// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::DB methods from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "include/org_rocksdb_RocksGraph.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/graph.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/types.h"
#include "rocksdb/version.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

#ifdef min
#undef min
#endif

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    Reinitialize
 * Signature: (JI)J
 */
jlong Java_org_rocksdb_RocksGraph_Reinitialize__JI(JNIEnv*, jclass,
                                                   jlong jopt_handle,
                                                   jint update_policy) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle);
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      new ROCKSDB_NAMESPACE::RocksGraph(*opt, static_cast<int>(update_policy));
  return GET_CPLUSPLUS_POINTER(graph_db);
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    Reinitialize
 * Signature: (JII)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_RocksGraph_Reinitialize__JII(
    JNIEnv*, jclass, jlong jopt_handle, jint update_policy, jint encoding) {
  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(jopt_handle);
  ROCKSDB_NAMESPACE::RocksGraph* graph_db = new ROCKSDB_NAMESPACE::RocksGraph(
      *opt, static_cast<int>(update_policy), static_cast<int>(encoding));
  return GET_CPLUSPLUS_POINTER(graph_db);
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    AddVertex
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksGraph_AddVertex(JNIEnv* env,
                                                             jobject,
                                                             jlong jdb_handle,
                                                             jlong id) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  ROCKSDB_NAMESPACE::Status s;
  s = graph_db->AddVertex(static_cast<ROCKSDB_NAMESPACE::node_id_t>(id));
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    AddEdge
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksGraph_AddEdge(JNIEnv* env, jobject,
                                                           jlong jdb_handle,
                                                           jlong source,
                                                           jlong target) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  ROCKSDB_NAMESPACE::Status s;
  s = graph_db->AddEdge(static_cast<ROCKSDB_NAMESPACE::node_id_t>(source),
                        static_cast<ROCKSDB_NAMESPACE::node_id_t>(target));
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    DeleteEdge
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksGraph_DeleteEdge(
    JNIEnv* env, jobject, jlong jdb_handle, jlong source, jlong target) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  ROCKSDB_NAMESPACE::Status s;
  s = graph_db->DeleteEdge(static_cast<ROCKSDB_NAMESPACE::node_id_t>(source),
                           static_cast<ROCKSDB_NAMESPACE::node_id_t>(target));
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    CountVertex
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_RocksGraph_CountVertex(JNIEnv*, jobject, jlong jdb_handle) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  return static_cast<jlong>(graph_db->CountVertex());
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    CountEdge
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_org_rocksdb_RocksGraph_CountEdge(JNIEnv*, jobject, jlong jdb_handle) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  return static_cast<jlong>(graph_db->CountEdge());
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    GetOutNeighbours
 * Signature: (JJ)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_rocksdb_RocksGraph_GetOutNeighbours(
    JNIEnv* env, jobject, jlong jdb_handle, jlong id) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);

  ROCKSDB_NAMESPACE::Edges edges;
  ROCKSDB_NAMESPACE::Status s = graph_db->GetAllEdges(
      static_cast<ROCKSDB_NAMESPACE::node_id_t>(id), &edges);
  if (!s.ok()) {
    std::cout << "get error: " << s.ToString() << std::endl;
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    exit(0);
  }
  const jsize resultsLen = static_cast<jsize>(edges.num_edges_out);
  std::unique_ptr<jlong[]> results =
      std::unique_ptr<jlong[]>(new jlong[resultsLen]);
  for (ROCKSDB_NAMESPACE::node_id_t i = 0; i < edges.num_edges_out; i++) {
    results[i] = static_cast<jlong>(edges.nxts_out[i].nxt);
  }
  jlongArray jresults = env->NewLongArray(resultsLen);
  env->SetLongArrayRegion(jresults, 0, resultsLen, results.get());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    GetInNeighbours
 * Signature: (JJ)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_rocksdb_RocksGraph_GetInNeighbours(
    JNIEnv* env, jobject, jlong jdb_handle, jlong id) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  ROCKSDB_NAMESPACE::Edges edges;
  ROCKSDB_NAMESPACE::Status s = graph_db->GetAllEdges(
      static_cast<ROCKSDB_NAMESPACE::node_id_t>(id), &edges);
  if (!s.ok()) {
    std::cout << "get error: " << s.ToString() << std::endl;
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    exit(0);
  }
  const jsize resultsLen = static_cast<jsize>(edges.num_edges_in);
  std::unique_ptr<jlong[]> results =
      std::unique_ptr<jlong[]>(new jlong[resultsLen]);
  for (ROCKSDB_NAMESPACE::node_id_t i = 0; i < edges.num_edges_in; i++) {
    results[i] = static_cast<jlong>(edges.nxts_in[i].nxt);
  }
  jlongArray jresults = env->NewLongArray(resultsLen);
  env->SetLongArrayRegion(jresults, 0, resultsLen, results.get());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    GetAllNeighbours
 * Signature: (JJ)[[J
 */
// JNIEXPORT jobjectArray JNICALL Java_org_rocksdb_RocksGraph_GetAllNeighbours(
//     JNIEnv* env, jobject, jlong jdb_handle, jlong id) {}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    InDegree
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_rocksdb_RocksGraph_InDegree(JNIEnv*, jobject,
                                                            jlong jdb_handle,
                                                            jlong id) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  ROCKSDB_NAMESPACE::Edges edges;
  // ROCKSDB_NAMESPACE::node_id_t vertex_id = static_cast<node_id_t> id;
  ROCKSDB_NAMESPACE::Status s = graph_db->GetAllEdges(
      static_cast<ROCKSDB_NAMESPACE::node_id_t>(id), &edges);
  return static_cast<jint>(edges.num_edges_in);
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    OutDegree
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_rocksdb_RocksGraph_OutDegree(JNIEnv*, jobject,
                                                             jlong jdb_handle,
                                                             jlong id) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  ROCKSDB_NAMESPACE::Edges edges;
  ROCKSDB_NAMESPACE::Status s = graph_db->GetAllEdges(
      static_cast<ROCKSDB_NAMESPACE::node_id_t>(id), &edges);
  return static_cast<jint>(edges.num_edges_out);
}

// /*
//  * Class:     org_rocksdb_RocksGraph
//  * Method:    InDegreeFast
//  * Signature: (JJ)I
//  */
// JNIEXPORT jint JNICALL Java_org_rocksdb_RocksGraph_InDegreeFast(
//     JNIEnv, jobject, jlong jdb_handle, jlong id) {
//   ROCKSDB_NAMESPACE::RocksGraph* graph_db =
//       reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
//   return static_cast<jint>(graph_db->GetInDegreeApproximate(
//       static_cast<ROCKSDB_NAMESPACE::node_id_t>(id)));
// }

// /*
//  * Class:     org_rocksdb_RocksGraph
//  * Method:    OutDegreeFast
//  * Signature: (JJ)I
//  */
// JNIEXPORT jint JNICALL Java_org_rocksdb_RocksGraph_OutDegreeFast(
//     JNIEnv, jobject, jlong jdb_handle, jlong id) {
//   ROCKSDB_NAMESPACE::RocksGraph* graph_db =
//       reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
//   return static_cast<jint>(graph_db->GetOutDegreeApproximate(
//       static_cast<ROCKSDB_NAMESPACE::node_id_t>(id)));
// }

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    Terminate
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksGraph_Terminate(JNIEnv, jclass,
                                                             jlong jdb_handle) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  graph_db->~RocksGraph();
}

JNIEXPORT void JNICALL Java_org_rocksdb_RocksGraph_SetWorkload(
    JNIEnv*, jobject, jlong jdb_handle, jdouble jlookup_ratio) {
  double lookup_ratio = static_cast<double>(jlookup_ratio);
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
  graph_db->SetRatio(1 - lookup_ratio, lookup_ratio);
}

/*
 * Class:     org_rocksdb_RocksGraph
 * Method:    disposeInternal
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_rocksdb_RocksGraph_disposeInternal(JNIEnv, jobject, jlong jdb_handle) {
  ROCKSDB_NAMESPACE::RocksGraph* graph_db =
      reinterpret_cast<ROCKSDB_NAMESPACE::RocksGraph*>(jdb_handle);
      graph_db->~RocksGraph();
}