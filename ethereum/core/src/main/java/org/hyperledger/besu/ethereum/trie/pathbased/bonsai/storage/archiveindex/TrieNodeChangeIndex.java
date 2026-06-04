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

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;
import java.util.OptionalInt;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Per-node change-block index over a {@link SegmentedKeyValueStorage} (Design 5, Tasks 2.3–2.4).
 *
 * <p>Three column families are maintained:
 *
 * <ul>
 *   <li>{@code TRIE_NODE_INDEX_ARCHIVE} — per-node, per-range packed {@link
 *       RangeRelativeOffsetList} keyed by {@code naturalKey ‖ rangeId(8 bytes BE)}.
 *   <li>{@code TRIE_NODE_RANGE_MARKER_ARCHIVE} — presence-only sentinel (value = empty byte array)
 *       keyed by the same {@code naturalKey ‖ rangeId} composite. Presence means at least one
 *       change was recorded for this node in this range.
 *   <li>{@code TRIE_NODE_BLOOM_ARCHIVE} — per-range {@link RangeBloom} keyed by {@code rangeId(8
 *       bytes BE)} only. The bloom is SHARED by all nodes that changed in the range.
 * </ul>
 *
 * <h3>rangeSize contract</h3>
 *
 * The injected {@code rangeSize} governs all offset arithmetic inside this class. For full
 * key-compatibility with {@link ArchiveNodeKey} the caller MUST pass {@link
 * ArchiveNodeKey#RANGE_SIZE} (1,000,000). The constructor does not enforce this so that unit tests
 * can use smaller values if desired, but production code should always use the canonical constant.
 *
 * <h3>Bloom same-transaction hazard</h3>
 *
 * The per-range bloom is keyed by {@code rangeId} ONLY, so it is shared by every node that changes
 * in a given range. The index-list and range-marker are keyed per {@code (naturalKey, rangeId)}, so
 * they never collide and are safe to write independently in one transaction. However, a {@link
 * SegmentedKeyValueStorageTransaction} is <em>write-only</em> — reads always go to committed
 * storage. Therefore, if multiple {@link #append} calls for different nodes share one transaction
 * (as they will during block import), each reads the same pre-transaction bloom from storage, adds
 * its own node, and writes back. Only the <em>last</em> write wins; earlier nodes' bits are lost.
 * <!-- TODO(Task 3.3): the per-range bloom is shared across all nodes in a block; the Stage-3 write
 *   hook must accumulate bloom bits per range in-memory within a block and write each range's bloom
 *   once at tx end (the write-only tx has no read-your-writes), otherwise concurrent appends in one
 *   tx lose bits. Doing so also eliminates the 128 KB bloom read-modify-write that every append
 *   currently performs. The index list and range marker writes are safe in one tx because their
 *   keys are per-node (no collision). Only the bloom has this hazard. -->
 */
public final class TrieNodeChangeIndex {

  /**
   * Default sub-block split threshold: when the main list exceeds this many entries, a split is
   * triggered. After the split the first {@link #DEFAULT_SUBBLOCK_SPLIT_AT} entries move to a new
   * sub-block in {@code TRIE_NODE_SUBBLOCK_ARCHIVE}.
   */
  static final int DEFAULT_SUBBLOCK_THRESHOLD = 4096;

  /**
   * Default number of entries moved into a new sub-block on each split. Must be less than {@link
   * #DEFAULT_SUBBLOCK_THRESHOLD}.
   */
  static final int DEFAULT_SUBBLOCK_SPLIT_AT = 2048;

  /** Number of bytes used to store the sub-block count at the head of each index value. */
  private static final int SUBCOUNT_BYTES = 4;

  private final SegmentedKeyValueStorage storage;
  private final long rangeSize;
  private final int subBlockThreshold;
  private final int subBlockSplitAt;

  /**
   * Constructs a new index backed by the given segmented KV store using the default sub-block
   * thresholds.
   *
   * @param storage the underlying key-value storage (must contain the required column families)
   * @param rangeSize blocks per range; must equal {@link ArchiveNodeKey#RANGE_SIZE} for
   *     key-compatibility with the rest of Design 5
   */
  public TrieNodeChangeIndex(final SegmentedKeyValueStorage storage, final long rangeSize) {
    this(storage, rangeSize, DEFAULT_SUBBLOCK_THRESHOLD, DEFAULT_SUBBLOCK_SPLIT_AT);
  }

  /**
   * Package-private constructor for testing with custom sub-block thresholds.
   *
   * <p>Allows unit tests to exercise the split logic with small threshold/splitAt values without
   * performing thousands of appends.
   *
   * @param storage the underlying key-value storage
   * @param rangeSize blocks per range
   * @param subBlockThreshold split is triggered when list size exceeds this value
   * @param subBlockSplitAt number of entries (the oldest) moved to a new sub-block on split
   */
  TrieNodeChangeIndex(
      final SegmentedKeyValueStorage storage,
      final long rangeSize,
      final int subBlockThreshold,
      final int subBlockSplitAt) {
    if (rangeSize <= 0) {
      throw new IllegalArgumentException("rangeSize must be > 0, got " + rangeSize);
    }
    // The within-range ceiling (rangeSize - 1) is cast to int in latestChangeBlock, so rangeSize
    // must fit in an int after subtracting 1 (i.e. <= Integer.MAX_VALUE + 1) to avoid silent
    // truncation. ArchiveNodeKey.RANGE_SIZE (1,000,000) is well within this bound.
    if (rangeSize > (long) Integer.MAX_VALUE + 1L) {
      throw new IllegalArgumentException(
          "rangeSize must be <= Integer.MAX_VALUE + 1, got " + rangeSize);
    }
    if (subBlockThreshold <= 0) {
      throw new IllegalArgumentException("subBlockThreshold must be > 0, got " + subBlockThreshold);
    }
    if (subBlockSplitAt <= 0 || subBlockSplitAt >= subBlockThreshold) {
      throw new IllegalArgumentException(
          "subBlockSplitAt must be in (0, subBlockThreshold), got "
              + subBlockSplitAt
              + ", threshold="
              + subBlockThreshold);
    }
    this.storage = storage;
    this.rangeSize = rangeSize;
    this.subBlockThreshold = subBlockThreshold;
    this.subBlockSplitAt = subBlockSplitAt;
  }

  // ---------------------------------------------------------------------------
  // Write path
  // ---------------------------------------------------------------------------

  /**
   * Records that {@code naturalKey} changed at {@code block} in the given transaction.
   *
   * <p>Three writes are issued on {@code tx}:
   *
   * <ol>
   *   <li>Appends {@code offset(block)} to the packed offset list for {@code
   *       TRIE_NODE_INDEX_ARCHIVE[naturalKey‖rangeId]}. If the list exceeds {@link
   *       #subBlockThreshold}, the first {@link #subBlockSplitAt} entries (the oldest) are moved to
   *       a new sub-block in {@code TRIE_NODE_SUBBLOCK_ARCHIVE[naturalKey‖rangeId‖subId(8B BE)]}.
   *   <li>Sets {@code TRIE_NODE_RANGE_MARKER_ARCHIVE[naturalKey‖rangeId]} (presence-only, empty
   *       value).
   *   <li>OR-in {@code naturalKey} to the {@link RangeBloom} for {@code
   *       TRIE_NODE_BLOOM_ARCHIVE[rangeId]}.
   * </ol>
   *
   * <p><strong>Bloom hazard:</strong> see class-level Javadoc. For single-node appends per
   * transaction (e.g. unit tests) this is safe. For multi-node block-import transactions see {@code
   * TODO(Task 3.3)}.
   *
   * @param tx the transaction on which to issue all writes
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey})
   * @param block the block number at which the node changed
   */
  public void append(
      final SegmentedKeyValueStorageTransaction tx, final Bytes naturalKey, final long block) {
    if (block < 0) {
      throw new IllegalArgumentException("block must be >= 0, got " + block);
    }
    final long rangeId = block / rangeSize;
    final int offset = (int) (block - rangeId * rangeSize);

    // 1. Update per-node offset list
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();

    // Read current index value: [4B subCount][packed offsets]
    final int[] subCountHolder = new int[1];
    final RangeRelativeOffsetList list =
        storage
            .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes)
            .map(
                b -> {
                  final IndexValue iv = readIndexValue(b);
                  subCountHolder[0] = iv.subCount;
                  return iv.list;
                })
            .orElse(RangeRelativeOffsetList.empty());

    int subCount = subCountHolder[0];
    RangeRelativeOffsetList updated = list.append(offset);

    // Split when list size exceeds the threshold.
    if (updated.size() > subBlockThreshold) {
      // Move the first subBlockSplitAt entries (the oldest) into a new sub-block.
      final RangeRelativeOffsetList head = sliceHead(updated, subBlockSplitAt);
      final RangeRelativeOffsetList tail = sliceTail(updated, subBlockSplitAt);

      final Bytes subKey = ArchiveNodeKey.subBlockKey(naturalKey, rangeId, subCount);
      tx.put(
          KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE,
          subKey.toArrayUnsafe(),
          head.toBytes().toArrayUnsafe());

      subCount++;
      updated = tail;
    }

    tx.put(
        KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE,
        indexKeyBytes,
        writeIndexValue(subCount, updated));

    // 2. Set presence-only range marker (same composite key, different CF)
    tx.put(
        KeyValueSegmentIdentifier.TRIE_NODE_RANGE_MARKER_ARCHIVE,
        indexKeyBytes, // naturalKey ‖ rangeId
        new byte[0]);

    // 3. Update per-range bloom
    // NOTE: reads from committed storage — see bloom same-tx hazard in class Javadoc.
    // TODO(Task 3.3): the per-range bloom is shared across all nodes in a block; the Stage-3
    //   write hook must accumulate bloom bits per range in-memory within a block and write each
    //   range's bloom once at tx end (the write-only tx has no read-your-writes), otherwise
    //   concurrent appends in one tx lose bits. Doing so also eliminates the 128 KB bloom
    //   read-modify-write performed on every append below. The index list and range marker writes
    //   are safe in one tx because their keys are per-node (no collision). Only the bloom has this
    //   hazard.
    final Bytes bloomKey = ArchiveNodeKey.bloomKey(rangeId);
    final byte[] bloomKeyBytes = bloomKey.toArrayUnsafe();
    final RangeBloom bloom =
        storage
            .get(KeyValueSegmentIdentifier.TRIE_NODE_BLOOM_ARCHIVE, bloomKeyBytes)
            .map(b -> RangeBloom.fromBytes(Bytes.wrap(b)))
            .orElse(RangeBloom.empty());
    bloom.add(naturalKey);
    tx.put(
        KeyValueSegmentIdentifier.TRIE_NODE_BLOOM_ARCHIVE,
        bloomKeyBytes,
        bloom.toBytes().toArrayUnsafe());
  }

  // ---------------------------------------------------------------------------
  // Index value helpers (format: [4B subCount BE][packed 3-byte offsets])
  // ---------------------------------------------------------------------------

  /**
   * Parsed representation of a value stored in {@code TRIE_NODE_INDEX_ARCHIVE}.
   *
   * <p>Format: {@code [4B subCount (big-endian int)][3N bytes: packed offset list]}. The subCount
   * is the number of sub-blocks already stored in {@code TRIE_NODE_SUBBLOCK_ARCHIVE} for this
   * {@code (naturalKey, rangeId)} pair. The packed offsets are the current <em>tail</em> (the
   * newest entries).
   */
  private record IndexValue(int subCount, RangeRelativeOffsetList list) {}

  /**
   * Parses the {@code [4B subCount][packed offsets]} index value from raw storage bytes.
   *
   * @param raw the raw bytes from {@code TRIE_NODE_INDEX_ARCHIVE}
   * @return the parsed sub-block count and tail offset list
   */
  private static IndexValue readIndexValue(final byte[] raw) {
    if (raw.length < SUBCOUNT_BYTES) {
      // Only reachable if storage is corrupt: the 4-byte subCount prefix is written on every
      // append since Task 2.6, and no pre-2.6 production data exists. Returning subCount=0 with
      // an empty list is safe — the caller will treat the entry as having no changes.
      return new IndexValue(0, RangeRelativeOffsetList.empty());
    }
    final int subCount =
        ((raw[0] & 0xFF) << 24)
            | ((raw[1] & 0xFF) << 16)
            | ((raw[2] & 0xFF) << 8)
            | (raw[3] & 0xFF);
    final Bytes packedOffsets = Bytes.wrap(raw, SUBCOUNT_BYTES, raw.length - SUBCOUNT_BYTES);
    final RangeRelativeOffsetList list =
        packedOffsets.isEmpty()
            ? RangeRelativeOffsetList.empty()
            : RangeRelativeOffsetList.fromBytes(packedOffsets);
    return new IndexValue(subCount, list);
  }

  /**
   * Serialises a sub-block count and offset list into the {@code [4B subCount][packed offsets]}
   * format used by {@code TRIE_NODE_INDEX_ARCHIVE}.
   *
   * @param subCount the number of existing sub-blocks
   * @param list the tail offset list
   * @return the serialised bytes
   */
  private static byte[] writeIndexValue(final int subCount, final RangeRelativeOffsetList list) {
    final Bytes packed = list.toBytes();
    final byte[] result = new byte[SUBCOUNT_BYTES + packed.size()];
    result[0] = (byte) ((subCount >>> 24) & 0xFF);
    result[1] = (byte) ((subCount >>> 16) & 0xFF);
    result[2] = (byte) ((subCount >>> 8) & 0xFF);
    result[3] = (byte) (subCount & 0xFF);
    packed.copyTo(MutableBytes.wrap(result, SUBCOUNT_BYTES, packed.size()));
    return result;
  }

  /**
   * Returns the first {@code n} entries of {@code list} as a new {@link RangeRelativeOffsetList}.
   *
   * @param list the source list
   * @param n the number of entries to include (must be &lt;= list.size())
   * @return a new list containing the first {@code n} entries
   */
  private static RangeRelativeOffsetList sliceHead(
      final RangeRelativeOffsetList list, final int n) {
    final Bytes buf = list.toBytes();
    return RangeRelativeOffsetList.fromBytes(buf.slice(0, n * RangeRelativeOffsetList.ENTRY_BYTES));
  }

  /**
   * Returns entries starting at index {@code from} of {@code list} as a new {@link
   * RangeRelativeOffsetList}.
   *
   * @param list the source list
   * @param from the starting index (entries [from, size) are included)
   * @return a new list containing entries from index {@code from} onward
   */
  private static RangeRelativeOffsetList sliceTail(
      final RangeRelativeOffsetList list, final int from) {
    final Bytes buf = list.toBytes();
    return RangeRelativeOffsetList.fromBytes(buf.slice(from * RangeRelativeOffsetList.ENTRY_BYTES));
  }

  // ---------------------------------------------------------------------------
  // Fast-path query: modifiedAfter
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} iff {@code naturalKey} has at least one change in the open interval {@code
   * (t, headBlock]} (i.e., strictly after {@code t}, at or before {@code headBlock}).
   *
   * <p>This is the <em>fast path</em> for the Stage-4 proof-node loader: when this method returns
   * {@code false}, the current live-trie node is the correct historical node for the proof (no
   * re-indexing needed). Callers pass {@code chainHead.getNumber()} as {@code headBlock}.
   *
   * <h3>Algorithm — ascending range walk</h3>
   *
   * <ol>
   *   <li>Compute {@code startRange = t / rangeSize} and {@code headRange = headBlock / rangeSize}.
   *   <li>Walk ranges {@code r = startRange} to {@code r = headRange} (ascending).
   *   <li>For each range:
   *       <ul>
   *         <li><strong>Bloom short-circuit</strong> — if bloom is absent or negative for {@code
   *             naturalKey}, skip.
   *         <li><strong>Range-marker check</strong> — if the marker for {@code (naturalKey, r)} is
   *             absent, skip (eliminates bloom false-positives before list read).
   *         <li><strong>Within-range floor</strong> — for {@code r == startRange}: floor = T's
   *             within-range offset (strictly, we need any entry {@code > floor}); for {@code r >
   *             startRange}: floor = -1 (any entry qualifies).
   *         <li><strong>Has-any-above check</strong> — use {@link
   *             RangeRelativeOffsetList#latestLeq} with the full range max ({@code rangeSize - 1})
   *             to get the last (largest) entry. If that value {@code > floor}, a qualifying change
   *             exists.
   *       </ul>
   *   <li>If any range satisfies → {@code true}. If the entire walk is exhausted → {@code false}.
   * </ol>
   *
   * <p><strong>Stopping condition:</strong> the walk is bounded by {@code headRange}. Ranges beyond
   * {@code headBlock} are not inspected. An alternative bloom-based stop (stop at the first absent
   * bloom) would also be correct on a live chain where ranges are populated continuously upward,
   * but the explicit {@code headBlock} bound is simpler and does not require knowing the chain head
   * from outside.
   *
   * <h3>Correctness invariant</h3>
   *
   * A false negative (returning {@code false} when a change exists after {@code t}) is a
   * <strong>critical correctness bug</strong> — it would cause Stage 4 to serve a stale node. A
   * false positive (returning {@code true} when unchanged) is only a performance miss. Bloom false
   * positives are acceptable; the range-marker and offset-list checks must not introduce false
   * negatives.
   *
   * <p><strong>Known false-positive source:</strong> when {@code t} and {@code headBlock} share the
   * same range, {@link #hasChangeAboveFloor} uses {@code latestLeq(rangeSize - 1)} (the full-range
   * max) not {@code headBlock}'s within-range offset as the ceiling. Therefore a change that exists
   * strictly after {@code headBlock} but before the range boundary will be reported as {@code
   * true}. Stage 4 must tolerate this — it triggers a {@link #latestChangeBlock} lookup which will
   * find the actual latest change ≤ T and serve the correct node.
   *
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey})
   * @param t the target proof block (exclusive lower bound of the search window)
   * @param headBlock the chain head block number (inclusive upper bound of the search window);
   *     callers should pass {@code chainHead.getNumber()}
   * @return {@code true} iff a change exists in {@code (t, headBlock]}
   * @throws IllegalArgumentException if {@code t < 0}, {@code headBlock < 0}, or {@code headBlock <
   *     t}
   */
  public boolean modifiedAfter(final Bytes naturalKey, final long t, final long headBlock) {
    if (t < 0) {
      throw new IllegalArgumentException("t must be >= 0, got " + t);
    }
    if (headBlock < 0) {
      throw new IllegalArgumentException("headBlock must be >= 0, got " + headBlock);
    }
    if (headBlock < t) {
      throw new IllegalArgumentException(
          "headBlock must be >= t, got headBlock=" + headBlock + ", t=" + t);
    }

    final long startRange = t / rangeSize;
    final long headRange = headBlock / rangeSize;
    // Maximum within-range offset value (= rangeSize - 1); cast is safe because rangeSize
    // is guarded to be <= Integer.MAX_VALUE + 1 in the constructor.
    final int maxOffset = (int) (rangeSize - 1);

    for (long r = startRange; r <= headRange; r++) {
      // Within-range floor (exclusive): for the startRange we need offset strictly > T's offset;
      // for higher ranges every offset in the range is > T, so floor = -1 (any entry qualifies).
      final int floor = (r == startRange) ? (int) (t - r * rangeSize) : -1;

      if (hasChangeAboveFloor(naturalKey, r, floor, maxOffset)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if range {@code rangeId} contains a change for {@code naturalKey} with an
   * offset strictly greater than {@code floor}.
   *
   * <p>Short-circuit order (cheapest-first):
   *
   * <ol>
   *   <li><strong>Bloom</strong> — if absent or negative, return false.
   *   <li><strong>Range marker</strong> — if absent, return false (eliminates bloom
   *       false-positives).
   *   <li><strong>Offset list</strong> — read the list and check whether the largest entry
   *       (obtained via {@code latestLeq(maxOffset)}) is greater than {@code floor}.
   * </ol>
   *
   * @param naturalKey the node's natural key
   * @param rangeId the range to search
   * @param floor the exclusive lower bound (offsets must be strictly {@code > floor}); pass {@code
   *     -1} to accept any entry (used for ranges entirely above {@code startRange})
   * @param maxOffset the maximum valid offset for this range ({@code rangeSize - 1})
   * @return {@code true} if any offset {@code > floor} exists in this range for this key
   */
  private boolean hasChangeAboveFloor(
      final Bytes naturalKey, final long rangeId, final int floor, final int maxOffset) {

    // 1. Bloom short-circuit.
    if (!bloomMaybeContains(rangeId, naturalKey)) {
      return false;
    }

    // 2. Range-marker short-circuit.
    if (!rangeMarkerPresent(naturalKey, rangeId)) {
      return false;
    }

    // 3. Offset list: get the last (largest) entry from the TAIL using latestLeq(maxOffset).
    //    The tail (main list in TRIE_NODE_INDEX_ARCHIVE) holds the NEWEST (largest) entries.
    //    Sub-blocks hold older entries, so any entry in a sub-block is ≤ any entry in the tail.
    //    Therefore: if the tail's largest entry > floor → a qualifying change exists. If the
    //    tail's largest entry ≤ floor, no sub-block entry can exceed floor either (all sub-block
    //    entries are smaller than the tail's smallest entry). No sub-block reads are needed.
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    return storage
        .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe())
        .map(
            bytes -> {
              // Parse the [4B subCount][packed offsets] format; only the tail (offsets) matters.
              final RangeRelativeOffsetList tail = readIndexValue(bytes).list;
              // The last entry is the largest. latestLeq(maxOffset) returns the largest entry
              // that is <= maxOffset — which is simply the last entry, since all offsets are
              // in [0, maxOffset]. If that value > floor, a change strictly after T exists.
              return tail.latestLeq(maxOffset).stream().anyMatch(last -> last > floor);
            })
        .orElse(false);
  }

  // ---------------------------------------------------------------------------
  // Read helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} if the range-marker for {@code (naturalKey, rangeId)} is present in
   * committed storage.
   *
   * @param naturalKey the node's natural key
   * @param rangeId the range identifier
   * @return whether at least one change was recorded for this node in this range
   */
  public boolean rangeMarkerPresent(final Bytes naturalKey, final long rangeId) {
    final Bytes compositeKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    return storage.containsKey(
        KeyValueSegmentIdentifier.TRIE_NODE_RANGE_MARKER_ARCHIVE, compositeKey.toArrayUnsafe());
  }

  /**
   * Returns {@code true} if the bloom for {@code rangeId} reports that {@code naturalKey} may have
   * changed in this range.
   *
   * <p>A return of {@code false} is a definitive negative (no change in this range). A return of
   * {@code true} may be a false positive; the caller should then consult the offset list.
   *
   * @param rangeId the range identifier
   * @param naturalKey the node's natural key
   * @return {@code false} if definitely not present; {@code true} if maybe present
   */
  public boolean bloomMaybeContains(final long rangeId, final Bytes naturalKey) {
    final Bytes bloomKey = ArchiveNodeKey.bloomKey(rangeId);
    return storage
        .get(KeyValueSegmentIdentifier.TRIE_NODE_BLOOM_ARCHIVE, bloomKey.toArrayUnsafe())
        .map(b -> RangeBloom.fromBytes(Bytes.wrap(b)).mightContain(naturalKey))
        .orElse(false);
  }

  /**
   * Returns the latest block ≤ {@code t} at which {@code naturalKey} changed, searching all ranges
   * from {@code rangeId(t)} down to 0.
   *
   * <p>The descending walk visits ranges in order from highest (the range containing {@code t}) to
   * lowest (range 0). For each range the per-range search is:
   *
   * <ol>
   *   <li><strong>Bloom short-circuit</strong> — if the range bloom is absent or negative for
   *       {@code naturalKey}, skip this range immediately (O(1), no list read).
   *   <li><strong>Range-marker check</strong> — if the range marker is absent for {@code
   *       (naturalKey, rangeId)}, skip (O(1); guards against bloom false-positives before a list
   *       read).
   *   <li><strong>Offset-list lookup</strong> — read the packed offset list and return the largest
   *       offset ≤ {@code withinRangeCeil}, converted to an absolute block number.
   * </ol>
   *
   * <p>The first range that yields a non-empty result is returned immediately (first-hit-wins from
   * the top is correct because we walk from the highest range downward: any hit in range {@code r}
   * is necessarily the latest change ≤ T, since all ranges above {@code r} either have no entry or
   * have no offset ≤ their ceiling, and ranges below {@code r} have only smaller block numbers).
   *
   * <p><strong>Lower bound:</strong> the walk always continues to range 0. Ranges below the
   * backfill coverage floor simply have no bloom or marker and are skipped in O(1). An {@code
   * indexStartBlock}-based early stop is a later optimisation (the coverage gate enforcing that
   * {@code t ≥ indexStartBlock} will be applied at the provider level in Stage 4).
   *
   * @param naturalKey the node's natural key
   * @param t the query block (inclusive upper bound)
   * @return the latest change block ≤ t, or empty if no such change exists in any range
   * @throws IllegalArgumentException if {@code t} is negative
   */
  public Optional<Long> latestChangeBlock(final Bytes naturalKey, final long t) {
    if (t < 0) {
      throw new IllegalArgumentException("t must be >= 0, got " + t);
    }
    final long startRange = t / rangeSize;
    for (long r = startRange; r >= 0; r--) {
      // Within-range ceiling: for the T-range use T's offset; for all earlier ranges the entire
      // range is ≤ T, so the ceiling is the maximum possible offset (rangeSize - 1).
      final int ceil = (r == startRange) ? (int) (t - r * rangeSize) : (int) (rangeSize - 1);
      final Optional<Long> hit = latestChangeInRange(naturalKey, r, ceil);
      if (hit.isPresent()) {
        return hit;
      }
    }
    return Optional.empty();
  }

  /**
   * Returns the latest change block within a single range, at or before {@code withinRangeCeil}.
   *
   * <p>Short-circuit order (cheapest-first):
   *
   * <ol>
   *   <li><strong>Bloom</strong> — if the range bloom is absent or negative, return empty
   *       immediately (O(1), no storage read for this node).
   *   <li><strong>Range marker</strong> — if the presence marker for {@code (naturalKey, rangeId)}
   *       is absent, return empty (O(1); eliminates bloom false-positives before reading the offset
   *       list).
   *   <li><strong>Offset list</strong> — read the packed list and return the largest offset ≤
   *       {@code withinRangeCeil}, converted to an absolute block number.
   * </ol>
   *
   * <p>For range {@code rangeId} the absolute block for offset {@code o} is {@code rangeId *
   * rangeSize + o}.
   *
   * @param naturalKey the node's natural key
   * @param rangeId the range to search
   * @param withinRangeCeil the offset ceiling (inclusive) within the range
   * @return the absolute block number of the latest change ≤ ceiling, or empty
   */
  private Optional<Long> latestChangeInRange(
      final Bytes naturalKey, final long rangeId, final int withinRangeCeil) {

    // 1. Bloom short-circuit: if the range bloom is absent or negative, no changes here.
    if (!bloomMaybeContains(rangeId, naturalKey)) {
      return Optional.empty();
    }

    // 2. Range-marker short-circuit: eliminates bloom false-positives before a list read.
    if (!rangeMarkerPresent(naturalKey, rangeId)) {
      return Optional.empty();
    }

    // 3. Read the TAIL (main list in TRIE_NODE_INDEX_ARCHIVE) and, if needed, walk sub-blocks.
    //
    // Index value format: [4B subCount (BE int)][packed 3-byte offsets = tail].
    // The tail holds the NEWEST (largest) entries for this key/range. Sub-blocks hold older
    // entries: subId=0 is the oldest, subId=subCount-1 is the most-recently-split (but still
    // older than the current tail). Walk: tail first, then sub-blocks from highest subId down.
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();
    final Optional<byte[]> rawOpt =
        storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes);
    if (rawOpt.isEmpty()) {
      return Optional.empty();
    }
    final IndexValue iv = readIndexValue(rawOpt.get());
    final int subCount = iv.subCount;
    final RangeRelativeOffsetList tail = iv.list;

    // 3a. Check the tail first (newest entries).
    final OptionalInt tailHit = tail.latestLeq(withinRangeCeil);
    if (tailHit.isPresent()) {
      return Optional.of(rangeId * rangeSize + tailHit.getAsInt());
    }

    // 3b. If the tail has no entry ≤ ceil (all tail entries are newer than ceil, or tail is
    //     empty), walk sub-blocks from highest subId downward (most-recently-split first).
    //     Each sub-block was evicted from the tail before all current tail entries, so its
    //     entries are strictly smaller. We stop at the first sub-block that has an entry ≤ ceil.
    for (int subId = subCount - 1; subId >= 0; subId--) {
      final Bytes subKey = ArchiveNodeKey.subBlockKey(naturalKey, rangeId, subId);
      final Optional<byte[]> subRaw =
          storage.get(KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE, subKey.toArrayUnsafe());
      if (subRaw.isEmpty()) {
        continue; // should not happen in well-formed data, but skip gracefully
      }
      final RangeRelativeOffsetList subList =
          RangeRelativeOffsetList.fromBytes(Bytes.wrap(subRaw.get()));
      final OptionalInt subHit = subList.latestLeq(withinRangeCeil);
      if (subHit.isPresent()) {
        return Optional.of(rangeId * rangeSize + subHit.getAsInt());
      }
    }

    return Optional.empty();
  }

  // ===========================================================================
  // RangeBloom — fixed-size bloom filter for per-range node presence
  // ===========================================================================

  /**
   * A simple, self-contained Bloom filter for tracking which trie nodes changed within a Design 5
   * block range.
   *
   * <h3>Parameters</h3>
   *
   * <ul>
   *   <li>{@code M = 1 << 20} bits = 1,048,576 bits = 128 KiB per range (≈ "~1M bits" from the
   *       design doc).
   *   <li>{@code k = 7} hash probes, derived by slicing the 32-byte keccak256 digest of the key
   *       into 4-byte (unsigned int) chunks, each taken modulo M.
   * </ul>
   *
   * <h3>Serialisation</h3>
   *
   * {@link #toBytes()} returns the raw bit-array (128 KiB). {@link #fromBytes(Bytes)} re-wraps it.
   * {@link #empty()} returns a fresh zero-filled instance.
   *
   * <h3>False-negative guarantee</h3>
   *
   * {@link #mightContain} returns {@code false} only when at least one probe bit is 0, meaning the
   * key was definitely never added. It may return {@code true} for keys not added (false positive),
   * but never returns {@code false} for a key that was added.
   */
  static final class RangeBloom {

    /** Number of bits in the filter: 2^20 = 1,048,576 bits = 131,072 bytes. */
    static final int M = 1 << 20;

    /**
     * Number of hash probes (k). Each probe is a 4-byte slice of keccak256(key) masked to the low
     * 20 bits (equivalent to mod M since M is a power of two).
     */
    static final int K = 7;

    /** Number of bytes in the serialised form. */
    static final int BYTE_SIZE = M / 8; // 131,072

    /** Mutable backing bit array. */
    private final byte[] bits;

    private RangeBloom(final byte[] bits) {
      this.bits = bits;
    }

    /** Returns a new, empty (all-zeros) bloom filter. */
    static RangeBloom empty() {
      return new RangeBloom(new byte[BYTE_SIZE]);
    }

    /**
     * Wraps an existing serialised bloom. The supplied {@link Bytes} must be exactly {@value
     * BYTE_SIZE} bytes; if not an {@link IllegalArgumentException} is thrown.
     *
     * @param packed the serialised bit-array
     * @return bloom wrapping the supplied data (mutable — callers should not retain a reference to
     *     the underlying array)
     */
    static RangeBloom fromBytes(final Bytes packed) {
      if (packed.size() != BYTE_SIZE) {
        throw new IllegalArgumentException(
            "RangeBloom.fromBytes expects " + BYTE_SIZE + " bytes, got " + packed.size());
      }
      // Copy to keep the mutable state isolated from the Bytes view.
      return new RangeBloom(packed.toArrayUnsafe().clone());
    }

    /**
     * Sets the {@code k} probe bits for {@code key} in the filter.
     *
     * @param key the natural key to add
     */
    void add(final Bytes key) {
      final int[] probes = probes(key);
      for (final int bitIndex : probes) {
        bits[bitIndex >>> 3] |= (byte) (1 << (bitIndex & 7));
      }
    }

    /**
     * Returns {@code true} if all {@code k} probe bits for {@code key} are set.
     *
     * @param key the natural key to test
     * @return {@code false} → definitely not present; {@code true} → maybe present
     */
    boolean mightContain(final Bytes key) {
      final int[] probes = probes(key);
      for (final int bitIndex : probes) {
        if ((bits[bitIndex >>> 3] & (1 << (bitIndex & 7))) == 0) {
          return false;
        }
      }
      return true;
    }

    /**
     * Returns the raw bit-array as a {@link Bytes} value suitable for storage.
     *
     * @return the serialised bloom (exactly {@value BYTE_SIZE} bytes)
     */
    Bytes toBytes() {
      // Return a mutable copy so that subsequent mutations to this instance do not affect the
      // returned view.
      final MutableBytes copy = MutableBytes.create(BYTE_SIZE);
      Bytes.wrap(bits).copyTo(copy);
      return copy;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Derives {@value K} bit-indices in {@code [0, M)} from {@code keccak256(key)}.
     *
     * <p>The 32-byte digest is split into 8 × 4-byte big-endian integers. The first {@link #K} (=
     * 7) values are masked to the low 20 bits (equivalent to mod {@link #M} since M is a power of
     * two) to produce the probe bit-indices.
     */
    private static int[] probes(final Bytes key) {
      final Bytes32 digest = Hash.keccak256(key);
      final int[] result = new int[K];
      for (int i = 0; i < K; i++) {
        // Read 4 bytes at offset i*4 as a big-endian int.
        final int raw =
            ((digest.get(i * 4) & 0xFF) << 24)
                | ((digest.get(i * 4 + 1) & 0xFF) << 16)
                | ((digest.get(i * 4 + 2) & 0xFF) << 8)
                | (digest.get(i * 4 + 3) & 0xFF);
        // M = 1<<20 is a power of two, so (x mod M) == (x & (M-1)): the mask extracts the low 20
        // bits directly. This is faster than %, always yields a non-negative index in [0, M), and
        // is used identically by both add() and mightContain() (no false negatives).
        result[i] = raw & (M - 1);
      }
      return result;
    }
  }
}
