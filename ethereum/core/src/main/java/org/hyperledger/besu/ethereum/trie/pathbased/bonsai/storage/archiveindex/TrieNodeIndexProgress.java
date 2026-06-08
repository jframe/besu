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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.ByteBuffer;

/**
 * Tracks the contiguous window of blocks that have been trie-node-indexed, so the read path never
 * serves a proof from outside the indexed window.
 *
 * <p>Coverage is defined as a closed window [{@link #indexStartBlock()}, {@link
 * #lastIndexedBlock()}]. {@link #covers(long)} returns {@code true} iff the requested block falls
 * within that window.
 *
 * <p>Persistence is wired to {@code TRIE_BRANCH_STORAGE} via {@link #load(SegmentedKeyValueStorage,
 * long)} and {@link #save(SegmentedKeyValueStorageTransaction)}. The serialisation format is fixed
 * 16 bytes (two big-endian longs: {@code lastIndexedBlock}, {@code indexStartBlock}).
 *
 * <p><strong>Thread-safety:</strong> This class is <em>not</em> thread-safe. Callers sharing an
 * instance between a writer thread (block import) and reader threads (proof path) must provide
 * external synchronisation.
 */
public class TrieNodeIndexProgress {

  /** Sentinel value for {@link #lastIndexedBlock()} when no block has been indexed yet. */
  public static final long UNSET_LAST_INDEXED = -1L;

  /** Sentinel value for {@link #indexStartBlock()} when no backfill has started yet. */
  public static final long UNSET_INDEX_START = Long.MAX_VALUE;

  /**
   * Key used to persist this record in {@code TRIE_BRANCH_STORAGE}. Co-located with {@code
   * WORLD_BLOCK_NUMBER_KEY} so that all metadata for the world-state live in the same column
   * family. The raw value is the UTF-8 encoding of {@code "trieNodeIndexProgress"}.
   */
  static final byte[] TRIE_NODE_INDEX_PROGRESS_KEY =
      "trieNodeIndexProgress".getBytes(java.nio.charset.StandardCharsets.UTF_8);

  private final long rangeSize;

  /**
   * The highest block number that has been forwarded-indexed. Starts at {@link #UNSET_LAST_INDEXED}
   * (-1). Monotonically non-decreasing: {@link #setLastIndexedBlock(long)} is a no-op when the
   * supplied value is less than the current value.
   */
  private long lastIndexedBlock;

  /**
   * The lowest block number from which backfill indexing has started. Starts at {@link
   * #UNSET_INDEX_START} (Long.MAX_VALUE). Monotonically non-increasing: {@link
   * #setIndexStartBlock(long)} is a no-op when the supplied value is greater than the current
   * value.
   */
  private long indexStartBlock;

  /**
   * Constructs a new, empty progress record.
   *
   * @param rangeSize the number of blocks in each indexing range (typically 1,000,000)
   */
  public TrieNodeIndexProgress(final long rangeSize) {
    if (rangeSize <= 0) {
      throw new IllegalArgumentException("rangeSize must be positive, got: " + rangeSize);
    }
    this.rangeSize = rangeSize;
    this.lastIndexedBlock = UNSET_LAST_INDEXED;
    this.indexStartBlock = UNSET_INDEX_START;
  }

  private TrieNodeIndexProgress(
      final long rangeSize, final long lastIndexedBlock, final long indexStartBlock) {
    this.rangeSize = rangeSize;
    this.lastIndexedBlock = lastIndexedBlock;
    this.indexStartBlock = indexStartBlock;
  }

  // ---------------------------------------------------------------------------
  // rangeSize accessor
  // ---------------------------------------------------------------------------

  /**
   * Returns the number of blocks in each indexing range.
   *
   * @return the range size supplied at construction time
   */
  public long rangeSize() {
    return rangeSize;
  }

  // ---------------------------------------------------------------------------
  // Coverage gate
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} iff {@code block} falls within the indexed window [{@link
   * #indexStartBlock()}, {@link #lastIndexedBlock()}].
   *
   * <p>Handles both live-import (forward) and migrator (backfill) paths.
   *
   * @param block absolute block number
   * @return whether the block is within the indexed window
   */
  public boolean covers(final long block) {
    if (block < 0) return false;
    if (indexStartBlock == UNSET_INDEX_START) return false;
    return block >= indexStartBlock && block <= lastIndexedBlock;
  }

  // ---------------------------------------------------------------------------
  // lastIndexedBlock — monotonic UP
  // ---------------------------------------------------------------------------

  /**
   * Returns the highest block number that has been forwarded-indexed, or {@link
   * #UNSET_LAST_INDEXED} (-1) if none.
   */
  public long lastIndexedBlock() {
    return lastIndexedBlock;
  }

  /**
   * Advances {@code lastIndexedBlock} to {@code n}. No-op if {@code n} is less than the current
   * value (monotonically non-decreasing).
   *
   * @param n candidate new last-indexed block number
   */
  public void setLastIndexedBlock(final long n) {
    if (n > lastIndexedBlock) {
      lastIndexedBlock = n;
    }
  }

  // ---------------------------------------------------------------------------
  // indexStartBlock — monotonic DOWN
  // ---------------------------------------------------------------------------

  /**
   * Returns the lowest block from which backfill indexing has started, or {@link
   * #UNSET_INDEX_START} (Long.MAX_VALUE) if no backfill has started.
   */
  public long indexStartBlock() {
    return indexStartBlock;
  }

  /**
   * Extends the backfill start downward to {@code n}. No-op if {@code n} is greater than the
   * current value (monotonically non-increasing).
   *
   * @param n candidate new index-start block number
   */
  public void setIndexStartBlock(final long n) {
    if (n < indexStartBlock) {
      indexStartBlock = n;
    }
  }

  // ---------------------------------------------------------------------------
  // Serialisation stubs
  // ---------------------------------------------------------------------------

  /**
   * Serialises this progress record to a fixed-width byte array suitable for storage.
   *
   * <p>Format (big-endian longs):
   *
   * <pre>
   *   [8 bytes] lastIndexedBlock
   *   [8 bytes] indexStartBlock
   * </pre>
   *
   * @return serialised bytes (always exactly 16 bytes)
   */
  public byte[] toBytes() {
    final ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(lastIndexedBlock);
    buf.putLong(indexStartBlock);
    return buf.array();
  }

  /**
   * Deserialises a progress record previously written by {@link #toBytes()}.
   *
   * <p>The caller MUST supply the same {@code rangeSize} that was in effect at write time: {@code
   * rangeSize} is not stored in the wire format.
   *
   * @param rangeSize the range size to associate with the restored record (must match write time)
   * @param bytes bytes produced by {@link #toBytes()} (must be exactly 16 bytes)
   * @return restored {@link TrieNodeIndexProgress}
   */
  public static TrieNodeIndexProgress fromBytes(final long rangeSize, final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final long last = buf.getLong();
    final long start = buf.getLong();
    return new TrieNodeIndexProgress(rangeSize, last, start);
  }

  // ---------------------------------------------------------------------------
  // Storage I/O
  // ---------------------------------------------------------------------------

  /**
   * Loads a {@link TrieNodeIndexProgress} from {@code TRIE_BRANCH_STORAGE} in the given storage.
   *
   * <p>If no progress record is found (e.g. on first startup), returns a fresh, empty instance.
   *
   * @param storage the segmented key-value storage to read from
   * @param rangeSize the number of blocks per index range (must match the value used at write time)
   * @return the restored progress record, or a new empty one if absent
   */
  public static TrieNodeIndexProgress load(
      final SegmentedKeyValueStorage storage, final long rangeSize) {
    return storage
        .get(TRIE_BRANCH_STORAGE, TRIE_NODE_INDEX_PROGRESS_KEY)
        .map(bytes -> fromBytes(rangeSize, bytes))
        .orElseGet(() -> new TrieNodeIndexProgress(rangeSize));
  }

  /**
   * Persists this progress record to {@code TRIE_BRANCH_STORAGE} in the given transaction.
   *
   * <p>The write is appended to {@code tx} and committed by the caller along with any other pending
   * writes for the block.
   *
   * @param tx the transaction on which to write the progress bytes
   */
  public void save(final SegmentedKeyValueStorageTransaction tx) {
    tx.put(TRIE_BRANCH_STORAGE, TRIE_NODE_INDEX_PROGRESS_KEY, toBytes());
  }
}
