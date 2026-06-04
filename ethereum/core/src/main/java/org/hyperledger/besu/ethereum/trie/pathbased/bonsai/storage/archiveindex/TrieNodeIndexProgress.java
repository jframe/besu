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

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Tracks which 1,000,000-block ranges have been fully trie-node-indexed, so the read path (Stage 4)
 * never serves a proof from a partially-indexed range.
 *
 * <p>The completeness bitmap is keyed by {@code rangeId = block / rangeSize}. Range 0 covers blocks
 * [0, rangeSize), range 1 covers blocks [rangeSize, 2*rangeSize), etc.
 *
 * <p>Persistence wiring is deferred to Stage 3/5. Use {@link #toBytes()} / {@link #fromBytes(long,
 * byte[])} for serialisation stubs.
 *
 * <p><strong>Thread-safety:</strong> This class is <em>not</em> thread-safe. Callers sharing an
 * instance between a writer thread (block import, Stage 3) and reader threads (proof path, Stage 4)
 * must provide external synchronisation or swap in thread-safe fields before Stage 3 integration.
 *
 * <p>TODO(Stage 3/5): wire {@link #toBytes()} / {@link #fromBytes(long, byte[])} to the
 * TRIE_BRANCH_STORAGE metadata CF (the same column family used by
 * PathBasedWorldStateKeyValueStorage for WORLD_BLOCK_NUMBER_KEY / WORLD_ROOT_HASH_KEY).
 */
public class TrieNodeIndexProgress {

  /** Sentinel value for {@link #lastIndexedBlock()} when no block has been indexed yet. */
  public static final long UNSET_LAST_INDEXED = -1L;

  /** Sentinel value for {@link #indexStartBlock()} when no backfill has started yet. */
  public static final long UNSET_INDEX_START = Long.MAX_VALUE;

  private final long rangeSize;

  /**
   * Completed-range bitmap. Bit {@code i} is set iff range {@code i} (blocks [{@code i *
   * rangeSize}, {@code (i+1) * rangeSize})) has been fully indexed. Realistic mainnet usage: ~22
   * bits (22 M blocks / 1 M rangeSize), so {@link BitSet} is compact and correct.
   */
  private final BitSet completedRanges;

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
    this.completedRanges = new BitSet();
    this.lastIndexedBlock = UNSET_LAST_INDEXED;
    this.indexStartBlock = UNSET_INDEX_START;
  }

  private TrieNodeIndexProgress(
      final long rangeSize,
      final BitSet completedRanges,
      final long lastIndexedBlock,
      final long indexStartBlock) {
    this.rangeSize = rangeSize;
    this.completedRanges = completedRanges;
    this.lastIndexedBlock = lastIndexedBlock;
    this.indexStartBlock = indexStartBlock;
  }

  // ---------------------------------------------------------------------------
  // Coverage gate
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} iff the range containing {@code block} (i.e. {@code block / rangeSize})
   * has been marked complete via {@link #markRangeComplete(long)}.
   *
   * @param block absolute block number
   * @return whether the block's range is fully indexed
   * @see #markRangeComplete(long)
   */
  public boolean covers(final long block) {
    if (block < 0) {
      return false;
    }
    long rangeId = block / rangeSize;
    if (rangeId > Integer.MAX_VALUE) {
      // A rangeId this large can never have been marked complete (markRangeComplete rejects it),
      // so return false rather than narrowing the cast to a wrong (possibly negative) int.
      return false;
    }
    return completedRanges.get((int) rangeId);
  }

  /**
   * Marks the given range as fully indexed.
   *
   * @param rangeId the range identifier, i.e. {@code block / rangeSize} — NOT a block number
   * @see #covers(long)
   */
  public void markRangeComplete(final long rangeId) {
    if (rangeId < 0 || rangeId > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("rangeId out of bounds: " + rangeId);
    }
    completedRanges.set((int) rangeId);
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
   * Serialises this progress record to a compact byte array suitable for storage in the
   * TRIE_BRANCH_STORAGE metadata column family.
   *
   * <p>Format (little-endian longs):
   *
   * <pre>
   *   [8 bytes] lastIndexedBlock
   *   [8 bytes] indexStartBlock
   *   [4 bytes] bitmap word count  (N)
   *   [N * 8 bytes] bitmap words (little-endian long[])
   * </pre>
   *
   * TODO(Stage 3/5): wire to TRIE_BRANCH_STORAGE metadata CF; add a format-version byte before
   * wiring to storage, and validate {@code bytes.length} before parsing in {@link #fromBytes(long,
   * byte[])}.
   *
   * @return serialised bytes
   */
  public byte[] toBytes() {
    long[] words = completedRanges.toLongArray();
    int byteCount = 8 + 8 + 4 + words.length * 8;
    ByteBuffer buf = ByteBuffer.allocate(byteCount);
    buf.putLong(lastIndexedBlock);
    buf.putLong(indexStartBlock);
    buf.putInt(words.length);
    for (long w : words) {
      buf.putLong(w);
    }
    return buf.array();
  }

  /**
   * Deserialises a progress record previously written by {@link #toBytes()}.
   *
   * <p>The caller MUST supply the same {@code rangeSize} that was in effect at write time: {@code
   * rangeSize} is not stored in the wire format, and a mismatch silently corrupts {@link
   * #covers(long)} results (a block would map to a different rangeId than when it was indexed).
   *
   * <p>TODO(Stage 3/5): wire to TRIE_BRANCH_STORAGE metadata CF; add a format-version byte before
   * wiring to storage, and validate {@code bytes.length} before parsing.
   *
   * @param rangeSize the range size to associate with the restored record (must match write time)
   * @param bytes bytes produced by {@link #toBytes()}
   * @return restored {@link TrieNodeIndexProgress}
   */
  public static TrieNodeIndexProgress fromBytes(final long rangeSize, final byte[] bytes) {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    long last = buf.getLong();
    long start = buf.getLong();
    int wordCount = buf.getInt();
    long[] words = new long[wordCount];
    for (int i = 0; i < wordCount; i++) {
      words[i] = buf.getLong();
    }
    BitSet bitmap = BitSet.valueOf(words);
    return new TrieNodeIndexProgress(rangeSize, bitmap, last, start);
  }
}
