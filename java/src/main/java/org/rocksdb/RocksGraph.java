// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.rocksdb.util.Environment;

/**
 * A RocksDB is a persistent ordered map from keys to values.  It is safe for
 * concurrent access from multiple threads without any external synchronization.
 * All methods of this class could potentially throw RocksDBException, which
 * indicates sth wrong at the RocksDB library side and the call failed.
 */
public class RocksGraph extends RocksObject {
  public static final byte[] DEFAULT_COLUMN_FAMILY = "default".getBytes(UTF_8);
  public static final int NOT_FOUND = -1;

  private enum LibraryState {
    NOT_LOADED,
    LOADING,
    LOADED
  }

  private static final AtomicReference<LibraryState> libraryLoaded =
      new AtomicReference<>(LibraryState.NOT_LOADED);

  static {
    RocksDB.loadLibrary();
  }

  static final String PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD =
      "Performance optimization for a very specific workload";

  private final List<ColumnFamilyHandle> ownedColumnFamilyHandles = new ArrayList<>();
 
  /**
   * Private constructor.
   *
   * @param nativeHandle The native handle of the C++ RocksDB object
   */
  protected RocksGraph(final long nativeHandle) {
    super(nativeHandle);
  }

  private static native long Reinitialize(final long optionsHandle, final int policy)
      throws RocksDBException;
  private native void AddEdge(final long handle, final long source_id,
      final long target_id) throws RocksDBException;
  private native void DeleteEdge(final long handle, final long source_id,
      final long target_id) throws RocksDBException;
  private native long[] GetOutNeighbours(final long handle, final long id) throws RocksDBException;
  private native long[] GetInNeighbours(final long handle, final long id) throws RocksDBException;
  private native long[][] GetAllNeighbours(final long handle, final long id) throws RocksDBException;
  private native int InDegree(final long handle, final long id) throws RocksDBException;
  private native int OutDegree(final long handle, final long id) throws RocksDBException;
  private native int InDegreeFast(final long handle, final long id) throws RocksDBException;
  private native int OutDegreeFast(final long handle, final long id) throws RocksDBException;
  private static native void Terminate(final long handle) throws RocksDBException;
  @Override protected native void disposeInternal(final long handle);
}
