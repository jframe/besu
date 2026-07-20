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

import java.util.EnumMap;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;

/**
 * Accumulates a composition breakdown of the {@code TRIE_NODE_HISTORY_ARCHIVE} column family by
 * decoding each entry's metadata byte (see {@link TrieNodeDiffCodec}) and its history key (see
 * {@link ArchiveNodeKey}).
 *
 * <p>The breakdown answers "what is consuming the history CF?" — specifically whether the bulk is
 * {@code FULL} creations, {@code FULL} checkpoints, always-{@code FULL} upper-trie nodes, or small
 * {@code DIFF}s — and how many bytes fall in the blob-eligible range ({@code value.length >=
 * minBlobSize}) versus staying inline in the SST. All byte counts are <em>logical/uncompressed</em>
 * sizes; the caller maps them to on-disk file sizes using the CF's measured compression ratio.
 *
 * <p>This class does no I/O; the caller feeds it every {@code (key, value)} pair via {@link
 * #record(byte[], byte[])}.
 */
public final class TrieNodeHistoryComposition {

  /** Width in bytes of the trailing big-endian block-number suffix on every history key. */
  private static final int BLOCK_SUFFIX_BYTES = 8;

  /** Width in bytes of the account-hash prefix on storage-trie natural keys. */
  private static final int ACCOUNT_HASH_BYTES = 32;

  /** Location-depth histogram cap (bytes). Locations deeper than this fold into the last bucket. */
  private static final int MAX_DEPTH = 64;

  /** RLP list arity of a branch node (16 children + value). */
  private static final int BRANCH_ARITY = 17;

  /** RLP list arity of a short node (path + value). */
  private static final int SHORT_ARITY = 2;

  /** The composition bucket an entry is attributed to. */
  public enum Category {
    CREATION_BRANCH,
    CREATION_SHORT,
    CREATION_UNKNOWN,
    CHECKPOINT_BRANCH,
    CHECKPOINT_SHORT,
    CHECKPOINT_UNKNOWN,
    UPPER_TRIE_BRANCH,
    UPPER_TRIE_SHORT,
    UPPER_TRIE_UNKNOWN,
    DIFF_BRANCH,
    DIFF_SHORT,
    HASH_REF_CREATION,
    HASH_REF_CHECKPOINT,
    DELETION
  }

  /** Mutable accumulator of counts and (logical) byte totals for a single {@link Category}. */
  public static final class Bucket {
    private long count;
    private long keyBytes;
    private long valueBytes;
    private long blobCount;
    private long blobValueBytes;

    /** {@return number of entries in this bucket} */
    public long count() {
      return count;
    }

    /** {@return total key bytes across entries in this bucket} */
    public long keyBytes() {
      return keyBytes;
    }

    /** {@return total (logical) value bytes across entries in this bucket} */
    public long valueBytes() {
      return valueBytes;
    }

    /** {@return number of entries whose value is blob-eligible (at least {@code minBlobSize})} */
    public long blobCount() {
      return blobCount;
    }

    /** {@return total (logical) value bytes of blob-eligible entries in this bucket} */
    public long blobValueBytes() {
      return blobValueBytes;
    }
  }

  private final int minBlobSize;
  private final int fullAboveDepth;
  private final Map<Category, Bucket> buckets = new EnumMap<>(Category.class);
  private final long[] depthHistogram = new long[MAX_DEPTH + 1];
  private long totalEntries;

  /**
   * @param minBlobSize the RocksDB {@code min_blob_size} threshold (100 for the archive CF); a
   *     value whose length is {@code >= minBlobSize} is stored in a blob file rather than inline
   * @param fullAboveDepth the {@code FULL_ABOVE_DEPTH} threshold (2 for the archive strategy); a
   *     node whose location is this many bytes or shallower is always stored FULL
   */
  public TrieNodeHistoryComposition(final int minBlobSize, final int fullAboveDepth) {
    this.minBlobSize = minBlobSize;
    this.fullAboveDepth = fullAboveDepth;
    for (final Category c : Category.values()) {
      buckets.put(c, new Bucket());
    }
  }

  /**
   * Records a single history-CF entry into the appropriate bucket and the depth histogram.
   *
   * @param key the raw history key ({@code naturalKey ‖ block(8B)})
   * @param value the raw entry value (metadata byte followed by the FULL/DIFF/tombstone body)
   */
  public void record(final byte[] key, final byte[] value) {
    final Category category = classify(key, value, fullAboveDepth);
    final Bucket b = buckets.get(category);
    b.count++;
    b.keyBytes += key.length;
    b.valueBytes += value.length;
    if (value.length >= minBlobSize) {
      b.blobCount++;
      b.blobValueBytes += value.length;
    }
    depthHistogram[Math.min(locationBytes(key.length), MAX_DEPTH)]++;
    totalEntries++;
  }

  /**
   * Adds another accumulator's totals into this one. Used to combine per-thread accumulators from a
   * parallel scan. Both must have been constructed with the same {@code minBlobSize} and {@code
   * fullAboveDepth}.
   *
   * @param other the accumulator to fold into this one
   */
  public void merge(final TrieNodeHistoryComposition other) {
    for (final Category c : Category.values()) {
      final Bucket dst = buckets.get(c);
      final Bucket src = other.buckets.get(c);
      dst.count += src.count;
      dst.keyBytes += src.keyBytes;
      dst.valueBytes += src.valueBytes;
      dst.blobCount += src.blobCount;
      dst.blobValueBytes += src.blobValueBytes;
    }
    for (int i = 0; i < depthHistogram.length; i++) {
      depthHistogram[i] += other.depthHistogram[i];
    }
    totalEntries += other.totalEntries;
  }

  /**
   * Returns the accumulator for a category.
   *
   * @param category the bucket to read
   * @return the accumulator for {@code category} (never {@code null})
   */
  public Bucket bucket(final Category category) {
    return buckets.get(category);
  }

  /** {@return total number of entries recorded} */
  public long totalEntries() {
    return totalEntries;
  }

  /**
   * Returns the location-depth histogram.
   *
   * @return histogram of entry counts indexed by location depth in bytes (the last index is the
   *     overflow bucket for deeper locations)
   */
  public long[] locationDepthHistogram() {
    return depthHistogram;
  }

  /**
   * Classifies a single history-CF entry from its key and value bytes.
   *
   * @param key the raw history key ({@code naturalKey ‖ block(8B)})
   * @param value the raw entry value (first byte is the {@link TrieNodeDiffCodec} metadata byte)
   * @param fullAboveDepth the always-FULL upper-trie depth threshold in bytes
   * @return the category the entry belongs to
   */
  public static Category classify(final byte[] key, final byte[] value, final int fullAboveDepth) {
    if (value.length == 0) {
      return Category.DELETION;
    }
    final byte md = value[0];
    final boolean full = (md & TrieNodeDiffCodec.ENTRY_FULL) != 0;
    final boolean deletion = (md & TrieNodeDiffCodec.DELETION) != 0;
    final boolean creation = (md & TrieNodeDiffCodec.CREATION) != 0;
    final boolean branchBit = (md & TrieNodeDiffCodec.NODE_IS_BRANCH) != 0;
    final boolean hashRef = (md & TrieNodeDiffCodec.HASH_REF) != 0;

    if (full && hashRef) {
      // Ref bodies are 32-byte hashes, not RLP — arity parsing does not apply.
      return creation ? Category.HASH_REF_CREATION : Category.HASH_REF_CHECKPOINT;
    }

    if (full) {
      final int arity = arity(value);
      if (creation) {
        return shaped(
            arity, Category.CREATION_BRANCH, Category.CREATION_SHORT, Category.CREATION_UNKNOWN);
      }
      if (locationBytes(key.length) <= fullAboveDepth) {
        return shaped(
            arity,
            Category.UPPER_TRIE_BRANCH,
            Category.UPPER_TRIE_SHORT,
            Category.UPPER_TRIE_UNKNOWN);
      }
      return shaped(
          arity,
          Category.CHECKPOINT_BRANCH,
          Category.CHECKPOINT_SHORT,
          Category.CHECKPOINT_UNKNOWN);
    }
    if (deletion) {
      return Category.DELETION;
    }
    return branchBit ? Category.DIFF_BRANCH : Category.DIFF_SHORT;
  }

  private static Category shaped(
      final int arity, final Category branch, final Category shortNode, final Category unknown) {
    if (arity == BRANCH_ARITY) {
      return branch;
    }
    if (arity == SHORT_ARITY) {
      return shortNode;
    }
    return unknown;
  }

  /**
   * Location depth in bytes: for account-trie nodes the natural key is the location; for storage
   * nodes it is {@code accountHash(32) ‖ location}, so the 32-byte prefix is subtracted.
   */
  private static int locationBytes(final int keyLength) {
    final int naturalKeyLen = Math.max(0, keyLength - BLOCK_SUFFIX_BYTES);
    return naturalKeyLen >= ACCOUNT_HASH_BYTES ? naturalKeyLen - ACCOUNT_HASH_BYTES : naturalKeyLen;
  }

  /**
   * Returns the RLP list arity of a FULL entry's node body, or {@code -1} if the body is not a
   * parseable RLP list.
   */
  private static int arity(final byte[] value) {
    if (value.length <= 1) {
      return -1;
    }
    try {
      final var in =
          org.hyperledger.besu.ethereum.rlp.RLP.input(Bytes.wrap(value, 1, value.length - 1));
      in.enterList();
      int n = 0;
      while (!in.isEndOfCurrentList()) {
        in.skipNext();
        n++;
      }
      in.leaveListLenient();
      return n;
    } catch (final RuntimeException e) {
      return -1;
    }
  }
}
