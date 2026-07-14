/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_FRONTIER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Best-effort, read-only background prefetch of trie-node paths (Design-5 migration part 2a). For a
 * look-ahead block's {@link TrieLog}, enumerates the trie-node keys the migrator's {@code
 * persist()} walk will read and issues one bounded background {@code multiGet} against {@code
 * TRIE_BRANCH_FRONTIER} then {@code TRIE_BRANCH_STORAGE}, discarding the results to warm the
 * RocksDB block cache. Never touches the migrator's in-memory caches, so it is safe to run
 * off-thread.
 */
public final class MigrationPrefetcher implements Closeable {

  private final SegmentedKeyValueStorage storage;
  private final Executor executor;
  private final Semaphore inFlight;
  private final int maxDepth;
  private volatile boolean closed = false;

  /**
   * Counts prefetch tasks actually submitted to {@link #executor} (i.e. after a successful {@link
   * #inFlight} acquire). Exists so tests can assert that prefetch genuinely ran end-to-end rather
   * than silently no-op'ing; production code does not read this value.
   */
  private final AtomicLong submittedTaskCount = new AtomicLong();

  /**
   * Creates a MigrationPrefetcher.
   *
   * @param storage the composed (real) storage to issue read-only warming {@code multiGet}s against
   * @param executor executor used to run the bounded background prefetch tasks
   * @param maxInFlight maximum number of concurrently in-flight prefetch tasks
   * @param maxDepth maximum trie-node location depth to prefetch, per {@link
   *     TrieNodePathEnumerator#trieNodePrefetchKeys(TrieLog, int)}
   */
  public MigrationPrefetcher(
      final SegmentedKeyValueStorage storage,
      final Executor executor,
      final int maxInFlight,
      final int maxDepth) {
    this.storage = storage;
    this.executor = executor;
    this.inFlight = new Semaphore(maxInFlight);
    this.maxDepth = maxDepth;
  }

  /**
   * Enumerates the trie-node keys touched by {@code trieLog} and submits at most one bounded
   * background task that warms the RocksDB block cache for them. Non-blocking: silently skips
   * (no-op) when closed or when the in-flight bound is saturated.
   *
   * @param trieLog the look-ahead block's trie log
   */
  public void prefetchTrieNodes(final TrieLog trieLog) {
    if (closed) {
      return;
    }
    final List<byte[]> keys = TrieNodePathEnumerator.trieNodePrefetchKeys(trieLog, maxDepth);
    if (keys.isEmpty() || !inFlight.tryAcquire()) {
      return;
    }
    try {
      executor.execute(
          () -> {
            try {
              storage.multiGet(TRIE_BRANCH_FRONTIER, keys);
              storage.multiGet(TRIE_BRANCH_STORAGE, keys);
            } catch (final RuntimeException ignored) {
              // Prefetch is best-effort; a failed warm read must never affect migration.
            } finally {
              inFlight.release();
            }
          });
      submittedTaskCount.incrementAndGet();
    } catch (final RuntimeException rejected) {
      // Executor rejected the task (e.g. shutting down) - release and move on.
      inFlight.release();
    }
  }

  /**
   * Returns the number of prefetch tasks actually submitted to the executor so far.
   *
   * @return the count of submitted (not necessarily completed) prefetch tasks
   */
  public long submittedTaskCount() {
    return submittedTaskCount.get();
  }

  /** Stops accepting new prefetch tasks. Already-submitted tasks run to completion. */
  @Override
  public void close() {
    closed = true;
  }
}
