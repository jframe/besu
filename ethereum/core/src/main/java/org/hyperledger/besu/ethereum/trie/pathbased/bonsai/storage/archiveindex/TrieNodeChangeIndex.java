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

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Per-node change-block index over a {@link SegmentedKeyValueStorage} (Design 5, Tasks 2.3–2.4).
 *
 * <p>One column family is maintained for the index:
 *
 * <ul>
 *   <li>{@code TRIE_NODE_INDEX_ARCHIVE} — per-node, per-range packed {@link
 *       RangeRelativeOffsetList} keyed by {@code naturalKey ‖ rangeId(8 bytes BE)}.
 * </ul>
 *
 * <h3>rangeSize contract</h3>
 *
 * The injected {@code rangeSize} governs all offset arithmetic inside this class. For full
 * key-compatibility with {@link ArchiveNodeKey} the caller MUST pass {@link
 * ArchiveNodeKey#RANGE_SIZE} (1,000,000). The constructor does not enforce this so that unit tests
 * can use smaller values if desired, but production code should always use the canonical constant.
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
   *
   * <p>Exposed as {@code public} so that external callers (e.g. the write hook in {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy})
   * can reconstruct the total mutation count from the stored {@code [subCount][tail]} format.
   */
  public static final int DEFAULT_SUBBLOCK_SPLIT_AT = 2048;

  /** Number of bytes used to store the sub-block count at the head of each index value. */
  private static final int SUBCOUNT_BYTES = 4;

  private final SegmentedKeyValueStorage storage;

  /**
   * Blocks per range. Package-private so that {@link TrieNodeHistoryReader} can compute rangeId
   * arithmetic without a separate accessor method.
   */
  final long rangeSize;

  private final int subBlockThreshold;
  private final int subBlockSplitAt;

  /**
   * Maximum number of entries in the write-through LRU index cache. Each entry is an indexKey →
   * serialised index value mapping that avoids re-reading committed storage on the next append for
   * the same key. At ~350 bytes/entry this is roughly 350 MB for a 1 M-entry active trie.
   */
  static final int CACHE_MAX_SIZE = 1_000_000;

  /**
   * Write-through LRU cache for {@code TRIE_NODE_INDEX_ARCHIVE} entries written during migration.
   * Keyed by the full index key ({@link ArchiveNodeKey#rangeKey}); value is the serialised {@code
   * [4B subCount][packed offsets]} bytes. Populated on each successful {@link #append} / {@link
   * #appendAndGetPreviousCount} write; checked before the committed-storage read on the next call
   * for the same key.
   *
   * <p>Only the write path ({@code append*}) reads from and writes to this cache. The query-only
   * methods ({@link #latestChangeBlock}, {@link #modifiedAfter}, etc.) bypass it intentionally —
   * they need committed-storage semantics.
   */
  private final LinkedHashMap<Bytes, byte[]> indexCache =
      new LinkedHashMap<>(CACHE_MAX_SIZE, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<Bytes, byte[]> eldest) {
          return size() > CACHE_MAX_SIZE;
        }
      };

  /**
   * Maximum number of entries in the earlier-range count cache. At ~150 bytes/entry this is roughly
   * 150 MB for a 1 M-entry active trie.
   */
  static final int EARLIER_RANGE_COUNT_CACHE_MAX_SIZE = 1_000_000;

  /**
   * LRU cache of the summed mutation count for a node in all <em>earlier</em> ranges {@code [0,
   * rangeId)}, keyed by {@link ArchiveNodeKey#rangeKey} ({@code naturalKey‖rangeId}).
   *
   * <p>Migration and live import advance through blocks in strictly increasing order, so every
   * range below the one currently being written is complete and never changes again (reorgs only
   * touch the head's range, which is whole ranges away from any earlier range). The earlier-range
   * sum is therefore stable and safe to memoise. This removes the per-append {@code storage.get}
   * sweep over earlier ranges in {@link #appendAndGetPreviousCount} — otherwise one uncached read
   * per earlier range on every deep-node change once block ≥ rangeSize, the single largest source
   * of migration read I/O.
   *
   * <p>A stale value (were the immutability assumption ever violated) only mis-places a checkpoint,
   * which lengthens the bounded backward walk at query time — never a correctness error.
   */
  private final LinkedHashMap<Bytes, Long> earlierRangeCountCache =
      new LinkedHashMap<>(1024, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<Bytes, Long> eldest) {
          return size() > EARLIER_RANGE_COUNT_CACHE_MAX_SIZE;
        }
      };

  /**
   * Buffered per-(naturalKey,rangeId) index state for batch migration. Non-null only between a
   * {@link #beginBuffered()} and {@link #flushBuffer}/{@link #discardBuffer} call pair. Keyed by
   * the range key ({@link ArchiveNodeKey#rangeKey}); value holds the committed-storage base (read
   * once on first touch) and the in-memory pending offset list.
   */
  private LinkedHashMap<Bytes, BufferedEntry> buffer = null;

  /** Accumulated index state for a single {@code (naturalKey, rangeId)} within a batch. */
  private static final class BufferedEntry {
    final Bytes naturalKey;
    final long rangeId;
    int baseSubCount;
    RangeRelativeOffsetList baseTail;

    /**
     * {@code true} once {@link #baseSubCount} and {@link #baseTail} have been populated (either
     * from {@link #indexCache} on first touch or via the bulk {@link #flushBuffer} multiGet). When
     * {@code false} the fields hold empty/zero defaults and must be loaded before the merge is
     * written.
     */
    boolean baseLoaded;

    final List<Integer> pending = new ArrayList<>();

    BufferedEntry(final Bytes naturalKey, final long rangeId) {
      this.naturalKey = naturalKey;
      this.rangeId = rangeId;
      this.baseTail = RangeRelativeOffsetList.empty();
    }
  }

  /**
   * Starts buffering mode. Subsequent {@link #append} and {@link #appendAndGetPreviousCount} calls
   * accumulate offsets in memory and perform no storage writes; the {@code tx} argument is unused
   * for the index value (only the running count is served from memory). Call {@link
   * #flushBuffer(SegmentedKeyValueStorageTransaction)} to write all buffered entries atomically, or
   * {@link #discardBuffer()} to abandon them (crash/rollback path).
   */
  public void beginBuffered() {
    buffer = new LinkedHashMap<>();
  }

  /**
   * Drops all buffered entries without writing to storage. Safe to call when not buffering (no-op).
   */
  public void discardBuffer() {
    buffer = null;
  }

  /**
   * Clears the write-through LRU index cache. Call this when a batch transaction commit fails after
   * {@link #flushBuffer} has already updated the cache with values that were never actually
   * persisted to storage. A subsequent batch will repopulate the cache from committed storage.
   */
  public void clearIndexCache() {
    indexCache.clear();
  }

  /**
   * Writes all buffered per-node offset lists into {@code tx} using the existing packed format,
   * applying the sub-block split logic incrementally as pending offsets are folded in. Updates the
   * LRU index cache with the final committed values. Safe to call when not buffering (no-op).
   *
   * <p>Before merging, issues a single {@code multiGet} for all buffered entries whose base values
   * were not available in {@link #indexCache} at first-touch time (i.e. entries with {@link
   * BufferedEntry#baseLoaded} {@code = false}). This consolidates what would otherwise be N
   * sequential per-key storage reads during the trie walk into one parallel batch read at flush
   * time, eliminating the dominant I/O cost on resumed migrations.
   *
   * @param tx the transaction into which all buffered index values are written
   */
  public void flushBuffer(final SegmentedKeyValueStorageTransaction tx) {
    if (buffer == null) {
      return;
    }

    // ── Phase 1: bulk-load base values for entries not found in indexCache at first touch ──────
    final List<Bytes> missKeys = new ArrayList<>();
    final List<byte[]> missKeyBytes = new ArrayList<>();
    for (final Map.Entry<Bytes, BufferedEntry> entry : buffer.entrySet()) {
      final BufferedEntry be = entry.getValue();
      if (!be.baseLoaded) {
        final byte[] indexKeyBytes = entry.getKey().toArrayUnsafe();
        // Fresh-migration mode: key definitely absent from DB → skip multiGet for this key.
        if (sessionWrittenKeys != null && !sessionWrittenKeys.mightContain(indexKeyBytes)) {
          be.baseLoaded = true; // treat as empty (new key); nothing to load
        } else {
          missKeys.add(entry.getKey());
          missKeyBytes.add(indexKeyBytes);
        }
      }
    }
    if (!missKeys.isEmpty()) {
      final List<Optional<byte[]>> results =
          storage.multiGet(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, missKeyBytes);
      for (int i = 0; i < missKeys.size(); i++) {
        final Bytes indexKey = missKeys.get(i);
        final Optional<byte[]> raw = results.get(i);
        final BufferedEntry be = buffer.get(indexKey);
        be.baseLoaded = true;
        raw.ifPresent(
            bytes -> {
              final IndexValue iv = readIndexValue(bytes);
              be.baseSubCount = iv.subCount;
              be.baseTail = iv.list;
              indexCache.put(indexKey, bytes);
            });
      }
    }

    // ── Phase 2: merge pending offsets and write ──────────────────────────────────────────────
    for (final Map.Entry<Bytes, BufferedEntry> entry : buffer.entrySet()) {
      final Bytes indexKey = entry.getKey();
      final BufferedEntry be = entry.getValue();
      if (be.pending.isEmpty()) {
        continue;
      }
      int subCount = be.baseSubCount;
      RangeRelativeOffsetList current = be.baseTail;
      for (final int offset : be.pending) {
        current = current.append(offset);
        if (current.size() > subBlockThreshold) {
          final RangeRelativeOffsetList head = sliceHead(current, subBlockSplitAt);
          final RangeRelativeOffsetList tail = sliceTail(current, subBlockSplitAt);
          final Bytes subKey = ArchiveNodeKey.subBlockKey(be.naturalKey, be.rangeId, subCount);
          tx.put(
              KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE,
              subKey.toArrayUnsafe(),
              head.toBytes().toArrayUnsafe());
          subCount++;
          current = tail;
        }
      }
      final byte[] newValue = writeIndexValue(subCount, current);
      tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe(), newValue);
      indexCache.put(indexKey, newValue);
    }
    buffer = null;
  }

  /**
   * Initialises a new {@link BufferedEntry} for {@code (naturalKey, rangeId)}. If the index key is
   * already in {@link #indexCache} the base values are loaded immediately and {@link
   * BufferedEntry#baseLoaded} is set to {@code true}. Otherwise the entry is returned with empty
   * defaults and {@code baseLoaded = false}; {@link #flushBuffer} will bulk-load all such entries
   * via a single {@code multiGet} before writing.
   *
   * <p>This method intentionally performs <em>no</em> storage read. Moving storage reads out of the
   * per-node hot path (where they occur once per unique key per trie-walk) and into the single
   * {@code flushBuffer} multiGet call is the key I/O optimisation for resumed migrations: instead
   * of N sequential preads interspersed with CPU work, all index reads happen together as one
   * parallel batch.
   */
  private BufferedEntry initBufferedEntry(
      final Bytes indexKey, final Bytes naturalKey, final long rangeId) {
    final BufferedEntry e = new BufferedEntry(naturalKey, rangeId);
    final byte[] cached = indexCache.get(indexKey);
    if (cached != null) {
      final IndexValue iv = readIndexValue(cached);
      e.baseSubCount = iv.subCount;
      e.baseTail = iv.list;
      e.baseLoaded = true;
    }
    // baseLoaded stays false → flushBuffer will issue a multiGet for this key.
    return e;
  }

  /**
   * In-session Bloom filter for fresh-migration mode. Non-null only when {@link
   * #enableFreshMigrationMode()} has been called. Tracks every index key written in this session so
   * that first-time-encounter keys (absent from the DB) can be identified without a {@code
   * storage.get()} call.
   *
   * <p>Sized for 30 M expected unique trie-node paths at 1 % FPP (≈ 36 MB). False positives cause
   * extra {@code storage.get()} calls (same as the non-optimised path) but never incorrect results.
   * If more than 30 M unique keys are inserted the FPP degrades gracefully rather than failing.
   */
  private BloomFilter<byte[]> sessionWrittenKeys = null;

  /**
   * Switches the index into <em>fresh-migration mode</em>: when active, a {@link
   * #appendAndGetPreviousCount} or {@link #append} call for a key that is neither in the LRU cache
   * nor in the in-session Bloom filter skips the committed-storage read entirely and assumes the
   * key is absent (previousCount = 0).
   *
   * <p>This is safe only on a <em>fresh</em> migration (one that starts from block 0 with an empty
   * {@code TRIE_NODE_INDEX_ARCHIVE}). On a resumed migration the filter would be empty even for
   * keys already written in a previous session, producing wrong previousCount values. The migrator
   * calls this method only when {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.BonsaiFlatDbToArchiveMigrator#getMigrationProgress()}
   * returns empty.
   */
  public void enableFreshMigrationMode() {
    sessionWrittenKeys = BloomFilter.create(Funnels.byteArrayFunnel(), 30_000_000, 0.01);
  }

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
   * <p>One write is issued on {@code tx}:
   *
   * <ol>
   *   <li>Appends {@code offset(block)} to the packed offset list for {@code
   *       TRIE_NODE_INDEX_ARCHIVE[naturalKey‖rangeId]}. If the list exceeds {@link
   *       #subBlockThreshold}, the first {@link #subBlockSplitAt} entries (the oldest) are moved to
   *       a new sub-block in {@code TRIE_NODE_SUBBLOCK_ARCHIVE[naturalKey‖rangeId‖subId(8B BE)]}.
   * </ol>
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

    // Update per-node offset list
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();

    if (buffer != null) {
      // Buffered path: accumulate offset in memory; no storage read or write.
      BufferedEntry e = buffer.get(indexKey);
      if (e == null) {
        e = initBufferedEntry(indexKey, naturalKey, rangeId);
        buffer.put(indexKey, e);
      }
      if (sessionWrittenKeys != null) {
        sessionWrittenKeys.put(indexKeyBytes);
      }
      e.pending.add(offset);
      return;
    }

    // Read current index value: check write-through cache before hitting committed storage.
    final int[] subCountHolder = new int[1];
    final RangeRelativeOffsetList list;
    final byte[] cached = indexCache.get(indexKey);
    if (cached != null) {
      final IndexValue iv = readIndexValue(cached);
      subCountHolder[0] = iv.subCount;
      list = iv.list;
    } else if (sessionWrittenKeys != null && !sessionWrittenKeys.mightContain(indexKeyBytes)) {
      // Fresh-migration mode: key is definitely absent from the DB (never written this session).
      list = RangeRelativeOffsetList.empty();
    } else {
      list =
          storage
              .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes)
              .map(
                  b -> {
                    final IndexValue iv = readIndexValue(b);
                    subCountHolder[0] = iv.subCount;
                    return iv.list;
                  })
              .orElse(RangeRelativeOffsetList.empty());
    }

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

    final byte[] newValue = writeIndexValue(subCount, updated);
    tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes, newValue);
    indexCache.put(indexKey, newValue);
    if (sessionWrittenKeys != null) {
      sessionWrittenKeys.put(indexKeyBytes);
    }
  }

  /**
   * Records that {@code naturalKey} changed at {@code block} (like {@link #append}), and returns
   * the number of prior mutations for this key — i.e. the mutation count as it was <em>before</em>
   * the current block's write. Combines what was previously two separate reads ({@code
   * countMutationsUpTo(key, block-1)} + {@code append(tx, key, block)}) into a single
   * committed-storage read for the current range.
   *
   * <p>For earlier ranges (when {@code rangeId(block) > 0}) the earlier-range counts are still read
   * individually; in practice all dev-chain and mainnet blocks fall in range 0 (first 1M blocks) so
   * this is effectively a single read.
   *
   * @param tx the transaction on which to write the updated index
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey})
   * @param block the block number at which the node changed
   * @return the number of mutations recorded before {@code block} (checkpoint detection value)
   */
  public long appendAndGetPreviousCount(
      final SegmentedKeyValueStorageTransaction tx, final Bytes naturalKey, final long block) {
    if (block < 0) {
      throw new IllegalArgumentException("block must be >= 0, got " + block);
    }
    final long rangeId = block / rangeSize;
    final int offset = (int) (block - rangeId * rangeSize);
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();

    // Count mutations in earlier ranges (rarely non-zero for chains < rangeSize blocks). Earlier
    // ranges are complete and immutable once block ≥ rangeSize, so the sum is memoised per
    // (naturalKey, rangeId) to avoid re-reading them on every deep-node append.
    final long earlierCount = earlierRangeCount(naturalKey, rangeId, indexKey);

    if (buffer != null) {
      // Buffered path: serve the count from in-memory state; no storage read or write.
      BufferedEntry e = buffer.get(indexKey);
      if (e == null) {
        e = initBufferedEntry(indexKey, naturalKey, rangeId);
        buffer.put(indexKey, e);
      }
      if (sessionWrittenKeys != null) {
        sessionWrittenKeys.put(indexKeyBytes);
      }
      final long previousCount =
          earlierCount
              + (long) e.baseSubCount * DEFAULT_SUBBLOCK_SPLIT_AT
              + e.baseTail.size()
              + e.pending.size();
      e.pending.add(offset);
      return previousCount;
    }

    // Read the current range once — check write-through cache before hitting committed storage.
    final int[] subCountHolder = new int[1];
    final RangeRelativeOffsetList list;
    final byte[] cached = indexCache.get(indexKey);
    if (cached != null) {
      final IndexValue iv = readIndexValue(cached);
      subCountHolder[0] = iv.subCount;
      list = iv.list;
    } else if (sessionWrittenKeys != null && !sessionWrittenKeys.mightContain(indexKeyBytes)) {
      // Fresh-migration mode: key is definitely absent from the DB (never written this session).
      list = RangeRelativeOffsetList.empty();
    } else {
      list =
          storage
              .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes)
              .map(
                  b -> {
                    final IndexValue iv = readIndexValue(b);
                    subCountHolder[0] = iv.subCount;
                    return iv.list;
                  })
              .orElse(RangeRelativeOffsetList.empty());
    }

    final int tailEntries = list.size();
    final long previousCount =
        earlierCount + (long) subCountHolder[0] * DEFAULT_SUBBLOCK_SPLIT_AT + tailEntries;

    // Append and write back (same logic as append()).
    int subCount = subCountHolder[0];
    RangeRelativeOffsetList updated = list.append(offset);

    if (updated.size() > subBlockThreshold) {
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

    final byte[] newValue = writeIndexValue(subCount, updated);
    tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes, newValue);
    indexCache.put(indexKey, newValue);
    if (sessionWrittenKeys != null) {
      sessionWrittenKeys.put(indexKeyBytes);
    }

    return previousCount;
  }

  /**
   * Returns the summed mutation count for {@code naturalKey} across all ranges strictly before
   * {@code rangeId}, memoised in {@link #earlierRangeCountCache} under {@code cacheKey} ({@code
   * naturalKey‖rangeId}). Returns 0 immediately when {@code rangeId == 0}. On a cache miss the
   * earlier ranges are summed from committed storage; the result is immutable (earlier ranges are
   * complete once block ≥ rangeSize) and cached.
   *
   * @param naturalKey the account or storage natural key
   * @param rangeId the range whose earlier-range total is needed
   * @param cacheKey the {@code naturalKey‖rangeId} index key, reused as the cache key
   * @return the total mutations recorded in ranges {@code [0, rangeId)}
   */
  private long earlierRangeCount(final Bytes naturalKey, final long rangeId, final Bytes cacheKey) {
    if (rangeId == 0) {
      return 0L;
    }
    final Long memoised = earlierRangeCountCache.get(cacheKey);
    if (memoised != null) {
      return memoised;
    }
    long earlierCount = 0L;
    for (long r = 0; r < rangeId; r++) {
      final Bytes rKey = ArchiveNodeKey.rangeKey(naturalKey, r);
      final Optional<byte[]> raw =
          storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, rKey.toArrayUnsafe());
      if (raw.isPresent()) {
        final byte[] b = raw.get();
        if (b.length >= SUBCOUNT_BYTES) {
          final int sc =
              ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
          final int te = (b.length - SUBCOUNT_BYTES) / RangeRelativeOffsetList.ENTRY_BYTES;
          earlierCount += (long) sc * DEFAULT_SUBBLOCK_SPLIT_AT + te;
        }
      }
    }
    earlierRangeCountCache.put(cacheKey, earlierCount);
    return earlierCount;
  }

  /**
   * Returns the total number of diff-index mutations recorded for {@code naturalKey} at or before
   * {@code block}, summing across all index ranges from 0 to {@code rangeId(block)}.
   *
   * <p>For each range the count is {@code subCount * DEFAULT_SUBBLOCK_SPLIT_AT + tailEntries},
   * derived from the packed {@code [4B subCount][3N offsets]} index value format. Ranges with no
   * index entry for this key are skipped.
   *
   * <p>This method reads from committed storage and is intended to be called with {@code block =
   * currentBlock - 1} to obtain the number of mutations <em>before</em> the block being written.
   *
   * @param naturalKey the node's natural key (from {@link ArchiveNodeKey})
   * @param block the inclusive upper bound; pass a negative value to get 0 immediately
   * @return the total mutation count at or before {@code block}, or 0 if none
   */
  public long countMutationsUpTo(final Bytes naturalKey, final long block) {
    if (block < 0) {
      return 0L;
    }
    final long maxRangeId = block / rangeSize;
    long total = 0L;

    for (long r = 0; r <= maxRangeId; r++) {
      final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, r);
      final Optional<byte[]> raw =
          storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe());
      if (raw.isEmpty()) {
        continue;
      }
      final byte[] b = raw.get();
      if (b.length < SUBCOUNT_BYTES) {
        continue;
      }
      final int subCount =
          ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
      final int tailEntries = (b.length - SUBCOUNT_BYTES) / RangeRelativeOffsetList.ENTRY_BYTES;
      total += (long) subCount * DEFAULT_SUBBLOCK_SPLIT_AT + tailEntries;
    }

    return total;
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
  // Optimised read helpers for TrieNodeHistoryReader
  // ---------------------------------------------------------------------------

  /**
   * Returns the full assembled {@link RangeRelativeOffsetList} (sub-blocks + tail) for {@code
   * (naturalKey, rangeId)} via a direct index-list read.
   *
   * <p>Returns {@link Optional#empty()} if the index entry is absent for this key/range.
   *
   * <p>The returned list is the full set of within-range offsets assembled from all sub-blocks
   * (oldest first) followed by the tail (newest), sorted ascending. It is NOT filtered by any
   * ceiling — all recorded offsets for this key in this range are included. Callers should use
   * {@link RangeRelativeOffsetList#latestLeq} or {@link RangeRelativeOffsetList#last} to query.
   *
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey})
   * @param rangeId the range identifier
   * @return the full offset list for this key/range, or empty if no data found
   */
  Optional<RangeRelativeOffsetList> readRangeList(final Bytes naturalKey, final long rangeId) {
    return assembleFullRangeList(naturalKey, rangeId);
  }

  /**
   * Assembles the full (sub-blocks + tail) {@link RangeRelativeOffsetList} for {@code (naturalKey,
   * rangeId)} directly from storage.
   *
   * <p>Shared implementation used by both {@link #readRangeList} and {@link #getChangeBlocksUpTo}
   * (which adds a ceiling filter). Returns empty when no index entry exists for the key/range.
   *
   * @param naturalKey the account or storage natural key
   * @param rangeId the range identifier
   * @return the full offset list assembled from sub-blocks + tail, or empty if the index entry is
   *     absent
   */
  private Optional<RangeRelativeOffsetList> assembleFullRangeList(
      final Bytes naturalKey, final long rangeId) {
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final Optional<byte[]> rawOpt =
        storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe());
    if (rawOpt.isEmpty()) {
      return Optional.empty();
    }
    final IndexValue iv = readIndexValue(rawOpt.get());
    final int subCount = iv.subCount;
    final RangeRelativeOffsetList tail = iv.list;

    // Fast path: no sub-blocks — the tail IS the full list.
    if (subCount == 0) {
      return Optional.of(tail);
    }

    // Slow path: prepend sub-block entries then append the tail entries.
    // Sub-block entries are strictly older (smaller offsets) than the tail.

    // Build the combined list by appending all offsets in ascending order.
    RangeRelativeOffsetList combined = RangeRelativeOffsetList.empty();
    for (int subId = 0; subId < subCount; subId++) {
      final Bytes subKey = ArchiveNodeKey.subBlockKey(naturalKey, rangeId, subId);
      final Optional<byte[]> subRaw =
          storage.get(KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE, subKey.toArrayUnsafe());
      if (subRaw.isEmpty()) {
        continue; // should not happen in well-formed data, but skip gracefully
      }
      final RangeRelativeOffsetList subList =
          RangeRelativeOffsetList.fromBytes(Bytes.wrap(subRaw.get()));
      final int subSize = subList.size();
      for (int i = 0; i < subSize; i++) {
        combined = combined.append(subList.get(i));
      }
    }
    final int tailSize = tail.size();
    for (int i = 0; i < tailSize; i++) {
      combined = combined.append(tail.get(i));
    }
    return Optional.of(combined);
  }

  /**
   * Returns the sorted (ascending) list of all absolute block numbers at which {@code naturalKey}
   * changed within the range containing {@code block}, restricted to blocks ≤ {@code block}.
   *
   * <p>The returned array collects entries from all sub-blocks (oldest first) and the tail,
   * converting each within-range offset to an absolute block number ({@code rangeId * rangeSize +
   * offset}). Only offsets ≤ {@code block}'s within-range offset are included.
   *
   * <p>Returns {@link java.util.Optional#empty()} if no entries exist ≤ {@code block} within the
   * range.
   *
   * <p>Used by {@link TrieNodeHistoryReader} to locate the nearest FULL checkpoint without repeated
   * {@link #latestChangeBlock} calls — one index-list read replaces up to 15 individual reads in
   * the hot case (CHECKPOINT_INTERVAL = 16).
   *
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey})
   * @param block the inclusive upper bound
   * @return sorted absolute block numbers ≤ block in this key's range, or empty if none
   */
  Optional<long[]> getChangeBlocksUpTo(final Bytes naturalKey, final long block) {
    final long rangeId = block / rangeSize;
    final int withinRangeCeil = (int) (block - rangeId * rangeSize);

    // Assemble the full range list (sub-blocks + tail) and filter by ceiling.
    final Optional<RangeRelativeOffsetList> fullListOpt =
        assembleFullRangeList(naturalKey, rangeId);
    if (fullListOpt.isEmpty()) {
      return Optional.empty();
    }
    final RangeRelativeOffsetList fullList = fullListOpt.get();

    // Accumulate all offsets ≤ withinRangeCeil as absolute block numbers.
    final long rangeBase = rangeId * rangeSize;
    final int listSize = fullList.size();
    final ArrayList<Long> blocks = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      final int offset = fullList.get(i);
      if (offset > withinRangeCeil) {
        break; // list is sorted ascending; no need to scan further
      }
      blocks.add(rangeBase + offset);
    }

    if (blocks.isEmpty()) {
      return Optional.empty();
    }

    final long[] result = new long[blocks.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = blocks.get(i);
    }
    return Optional.of(result);
  }

  /**
   * Returns the total number of mutations recorded for {@code naturalKey} in all ranges strictly
   * before {@code rangeId} (i.e. ranges 0, 1, …, rangeId − 1).
   *
   * <p>Used by {@link TrieNodeHistoryReader} alongside {@link #getChangeBlocksUpTo} to compute the
   * global mutation index of {@code b*} when the node's history spans multiple ranges, so that the
   * correct FULL checkpoint position can be determined regardless of range boundaries.
   *
   * <p>Each range contributes {@code subCount * DEFAULT_SUBBLOCK_SPLIT_AT + tailEntries} mutations,
   * derived from the packed {@code [4B subCount][3N offsets]} index value. Ranges with no index
   * entry for this key are skipped.
   *
   * @param naturalKey the node's natural key (from {@link ArchiveNodeKey})
   * @param rangeId the (exclusive) upper bound; pass 0 to get 0 immediately
   * @return the total mutation count in ranges [0, rangeId)
   */
  int countMutationsInEarlierRanges(final Bytes naturalKey, final long rangeId) {
    if (rangeId <= 0) {
      return 0;
    }
    int total = 0;
    for (long r = 0; r < rangeId; r++) {
      final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, r);
      final Optional<byte[]> raw =
          storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKey.toArrayUnsafe());
      if (raw.isEmpty()) {
        continue;
      }
      final byte[] b = raw.get();
      if (b.length < SUBCOUNT_BYTES) {
        continue;
      }
      final int subCount =
          ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
      final int tailEntries = (b.length - SUBCOUNT_BYTES) / RangeRelativeOffsetList.ENTRY_BYTES;
      total += subCount * DEFAULT_SUBBLOCK_SPLIT_AT + tailEntries;
    }
    return total;
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
   * {@code headBlock} are not inspected.
   *
   * <h3>Correctness invariant</h3>
   *
   * A false negative (returning {@code false} when a change exists after {@code t}) is a
   * <strong>critical correctness bug</strong> — it would cause Stage 4 to serve a stale node. A
   * false positive (returning {@code true} when unchanged) is only a performance miss.
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
   * @param naturalKey the node's natural key
   * @param rangeId the range to search
   * @param floor the exclusive lower bound (offsets must be strictly {@code > floor}); pass {@code
   *     -1} to accept any entry (used for ranges entirely above {@code startRange})
   * @param maxOffset the maximum valid offset for this range ({@code rangeSize - 1})
   * @return {@code true} if any offset {@code > floor} exists in this range for this key
   */
  private boolean hasChangeAboveFloor(
      final Bytes naturalKey, final long rangeId, final int floor, final int maxOffset) {

    // Offset list: get the last (largest) entry from the TAIL using latestLeq(maxOffset).
    // The tail (main list in TRIE_NODE_INDEX_ARCHIVE) holds the NEWEST (largest) entries.
    // Sub-blocks hold older entries, so any entry in a sub-block is ≤ any entry in the tail.
    // Therefore: if the tail's largest entry > floor → a qualifying change exists. If the
    // tail's largest entry ≤ floor, no sub-block entry can exceed floor either (all sub-block
    // entries are smaller than the tail's smallest entry). No sub-block reads are needed.
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

  /**
   * Returns the latest block ≤ {@code t} at which {@code naturalKey} changed, searching all ranges
   * from {@code rangeId(t)} down to 0.
   *
   * <p>The descending walk visits ranges in order from highest (the range containing {@code t}) to
   * lowest (range 0). For each range the per-range search reads the packed offset list and returns
   * the largest offset ≤ {@code withinRangeCeil}, converted to an absolute block number.
   *
   * <p>The first range that yields a non-empty result is returned immediately (first-hit-wins from
   * the top is correct because we walk from the highest range downward: any hit in range {@code r}
   * is necessarily the latest change ≤ T, since all ranges above {@code r} either have no entry or
   * have no offset ≤ their ceiling, and ranges below {@code r} have only smaller block numbers).
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
   * <p>Reads the packed offset list and returns the largest offset ≤ {@code withinRangeCeil},
   * converted to an absolute block number.
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

    // Read the TAIL (main list in TRIE_NODE_INDEX_ARCHIVE) and, if needed, walk sub-blocks.
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
}
