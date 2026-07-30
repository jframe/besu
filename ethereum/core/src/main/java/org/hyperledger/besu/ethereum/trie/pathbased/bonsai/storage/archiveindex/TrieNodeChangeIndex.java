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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.tuweni.bytes.Bytes;

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
   * Write-through LRU cache for {@code TRIE_NODE_INDEX_META_ARCHIVE} entries written during
   * migration. Keyed by the full index key ({@link ArchiveNodeKey#rangeKey}); value is the
   * serialised 8-byte {@link IndexMetadata} ({@code [4B subCount][4B tailCount]}) — <em>not</em>
   * the packed offset content, which is only ever merge-appended and never read back on the hot
   * write path. Populated on each successful {@link #append} / {@link #appendAndGetPreviousCount}
   * write; checked before the committed-storage read on the next call for the same key.
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
   * Drain threshold for the background base-value prefetch queue (Task 2b). Adjustable via {@link
   * #enablePrefetchDrainThresholdForTesting(int)} so unit tests can force an immediate drain
   * without needing thousands of buffered entries.
   *
   * <p>Deliberately small (not the migration batch size, e.g. 256 blocks). A 256-block batch
   * commonly touches far fewer than a few hundred distinct cold index keys, so a large threshold
   * (originally 512) was rarely crossed mid-batch — the only drain that ran was the unconditional
   * one at the start of {@link #flushBuffer}, submitted with essentially zero lead time before
   * Phase 1 reads it back, so the background read routinely lost the race and fell through to
   * {@code flushBuffer}'s own synchronous {@code multiGet} anyway (confirmed by profiling: {@code
   * flushBuffer}'s synchronous-read share was statistically unchanged from the pre-prefetch
   * baseline at comparable migration depth). A small threshold drains incrementally throughout the
   * batch instead, giving each background {@code multiGet} real wall-clock time to complete before
   * {@code flushBuffer} needs the result.
   */
  private static int prefetchDrainThreshold = 64;

  /**
   * Call-count interval for a periodic base-value prefetch drain, independent of {@link
   * #prefetchDrainThreshold}. Adjustable via {@link
   * #enablePrefetchDrainCallIntervalForTesting(int)}.
   *
   * <p>{@link #prefetchDrainThreshold} alone only drains once the queue accumulates that many
   * distinct cold keys — but how many distinct <em>repeat-touched</em> keys (the only ones eligible
   * for prefetch; first-touches are skipped via the fresh-migration bloom) a given batch contains
   * depends on that batch's block content, not a fixed rate. Profiling a live migration confirmed
   * batches whose repeat-touch count stayed under the size threshold got no benefit from prefetch
   * at all — the queue never drained until the unconditional, zero-lead-time call at the start of
   * {@link #flushBuffer}. This periodic, call-count-based trigger forces a drain of however many
   * keys are currently queued (even far below the size threshold) at a regular cadence throughout
   * the batch, so a sparse batch still gets real background lead time instead of depending on
   * hitting the size threshold at all.
   */
  private static int prefetchDrainCallInterval = 4096;

  /**
   * Buffered {@code append}/{@code appendAndGetPreviousCount} calls observed since the last
   * periodic drain (see {@link #prefetchDrainCallInterval}). Reset per batch alongside the other
   * per-batch prefetch state.
   */
  private int callsSinceLastPeriodicDrain = 0;

  /**
   * Executor used for background base-value prefetch reads. {@code null} (the default) means
   * prefetch is disabled and every code path below behaves exactly as it did before Task 2b.
   */
  private Executor prefetchExecutor;

  /** Bounds the number of prefetch reads in flight at once; only used when prefetch is enabled. */
  private Semaphore prefetchInFlight;

  /**
   * Cold (not yet loaded) index keys accumulated during the current batch, awaiting a background
   * {@code multiGet}. Only ever touched by the migrator thread (enqueue) and {@link
   * #drainPrefetch()} (poll), both of which run under the same single-writer discipline as the rest
   * of the buffered-append path.
   */
  private final ConcurrentLinkedQueue<Bytes> prefetchQueue = new ConcurrentLinkedQueue<>();

  /**
   * Staging map for background-prefetched base index values, keyed by index key. {@code
   * Optional.empty()} means the key is definitively absent from committed storage (a valid,
   * meaningful result — not "not yet fetched").
   *
   * <p><strong>Must be a fresh instance per batch</strong>, swapped (never {@code .clear()}'d) in
   * {@link #beginBuffered()}, {@link #discardBuffer()}, {@link #clearIndexCache()}, and at the end
   * of {@link #flushBuffer}. A background drain task submitted in batch N captures the batch-N map
   * reference at submission time ({@code final var target = prefetchedBase;} inside {@link
   * #drainPrefetch()}); if that task is still running when batch N+1 begins, swapping the field to
   * a new map ensures the late task can only ever write into the (now-abandoned) old map, never
   * into batch N+1's staging — because the committed base values for a key can differ between
   * batches (this class's own {@link #flushBuffer} writes them), a stale value written into the
   * wrong batch's map would silently corrupt that batch's merge.
   */
  private volatile ConcurrentHashMap<Bytes, Optional<byte[]>> prefetchedBase =
      new ConcurrentHashMap<>();

  /**
   * Counts base values consumed from {@link #prefetchedBase} by {@link #flushBuffer} — i.e. a
   * background-staged hit that was used instead of falling back to a synchronous {@code multiGet}.
   * Exposed via {@link #prefetchBaseHits()} purely as a test/observability hook so that invariant
   * tests can prove the background prefetch path actually ran.
   */
  private final AtomicLong prefetchBaseHits = new AtomicLong();

  /**
   * Returns the number of base index values that {@link #flushBuffer} consumed directly from the
   * background prefetch staging map (rather than via its own synchronous {@code multiGet}) since
   * this index was constructed. Always {@code 0} when prefetch was never enabled.
   *
   * @return the cumulative count of prefetch-staged hits consumed by {@code flushBuffer}
   */
  public long prefetchBaseHits() {
    return prefetchBaseHits.get();
  }

  /**
   * Enables opt-in background prefetch of committed base index values during buffered migration.
   *
   * <p>When enabled, cold keys touched for the first time in a batch (see the {@code append}/{@code
   * appendAndGetPreviousCount} buffered-path enqueue logic) are queued and, once the queue reaches
   * {@link #prefetchDrainThreshold}, drained into a background {@code multiGet} submitted to {@code
   * executor}. Results land in {@link #prefetchedBase}; {@link #flushBuffer} consults that map
   * before falling back to its own synchronous {@code multiGet} for any keys still missing.
   *
   * <p>Never called in production code paths that don't opt in: leaving this unset means {@link
   * #prefetchExecutor} stays {@code null} and every prefetch-related branch below is skipped,
   * preserving byte-for-byte identical behaviour to the pre-Task-2b code.
   *
   * @param executor the executor on which background {@code multiGet} drains are submitted
   * @param inFlight bounds the number of concurrently in-flight background drains; a full semaphore
   *     causes {@link #drainPrefetch()} to skip the drain and leave the keys for {@link
   *     #flushBuffer}'s synchronous fallback
   */
  public void enablePrefetch(final Executor executor, final Semaphore inFlight) {
    this.prefetchExecutor = executor;
    this.prefetchInFlight = inFlight;
  }

  /**
   * Overrides the prefetch drain threshold for testing, so a single enqueued key can trigger an
   * immediate drain instead of requiring {@link #prefetchDrainThreshold} (64) keys.
   *
   * @param n the new drain threshold
   */
  @VisibleForTesting
  void enablePrefetchDrainThresholdForTesting(final int n) {
    prefetchDrainThreshold = n;
  }

  /**
   * Overrides the periodic drain call-count interval for testing, so a single buffered append call
   * can trigger the periodic drain instead of requiring {@link #prefetchDrainCallInterval} (4096)
   * calls.
   *
   * @param n the new call-count interval
   */
  @VisibleForTesting
  void enablePrefetchDrainCallIntervalForTesting(final int n) {
    prefetchDrainCallInterval = n;
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
    prefetchQueue.clear();
    // Fresh map per batch — see the field javadoc for why this must be a swap, not a clear().
    prefetchedBase = new ConcurrentHashMap<>();
    callsSinceLastPeriodicDrain = 0;
  }

  /**
   * Drops all buffered entries without writing to storage. Safe to call when not buffering (no-op).
   */
  public void discardBuffer() {
    buffer = null;
    prefetchQueue.clear();
    prefetchedBase = new ConcurrentHashMap<>();
    callsSinceLastPeriodicDrain = 0;
  }

  /**
   * Clears the write-through LRU index cache. Call this when a batch transaction commit fails after
   * {@link #flushBuffer} has already updated the cache with values that were never actually
   * persisted to storage. A subsequent batch will repopulate the cache from committed storage.
   */
  public void clearIndexCache() {
    indexCache.clear();
    // A failed commit must not leave stale staged bases visible to whatever batch follows.
    prefetchQueue.clear();
    prefetchedBase = new ConcurrentHashMap<>();
    callsSinceLastPeriodicDrain = 0;
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

    // Drain any still-queued keys so in-flight background reads cover as much as possible before
    // we fall back to a synchronous read for whatever remains uncovered.
    drainPrefetch();

    // ── Phase 1: bulk-load base values for entries not found in indexCache at first touch ──────
    final List<Bytes> missKeys = new ArrayList<>();
    final List<byte[]> missKeyBytes = new ArrayList<>();
    for (final Map.Entry<Bytes, BufferedEntry> entry : buffer.entrySet()) {
      final BufferedEntry be = entry.getValue();
      if (!be.baseLoaded) {
        final byte[] indexKeyBytes = entry.getKey().toArrayUnsafe();
        final Optional<byte[]> staged = prefetchedBase.get(entry.getKey());
        if (staged != null) {
          // A background prefetch already resolved this key (present or definitively absent) —
          // consume it instead of issuing a synchronous read.
          be.baseLoaded = true;
          prefetchBaseHits.incrementAndGet();
          staged.ifPresent(
              bytes -> {
                final IndexValue iv = readIndexValue(bytes);
                be.baseSubCount = iv.subCount;
                be.baseTail = iv.list;
                indexCache.put(entry.getKey(), bytes);
              });
        } else if (sessionWrittenKeys != null && !sessionWrittenKeys.mightContain(indexKeyBytes)) {
          // Fresh-migration mode: key definitely absent from DB → skip multiGet for this key.
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
    prefetchQueue.clear();
    // Fresh map, not clear(): see the field javadoc — a late background task from this batch that
    // is still running must not be able to write into the next batch's staging map.
    prefetchedBase = new ConcurrentHashMap<>();
    callsSinceLastPeriodicDrain = 0;
  }

  /**
   * Enqueues {@code indexKey} for background base-value prefetch and triggers a drain once the
   * queue reaches {@link #prefetchDrainThreshold}. No-op when prefetch is disabled ({@link
   * #prefetchExecutor} is {@code null}) — but callers only invoke this after already checking that,
   * so this method itself does not need to re-check.
   *
   * @param indexKey the index key to prefetch the committed base value for
   */
  private void enqueueBasePrefetch(final Bytes indexKey) {
    prefetchQueue.add(indexKey);
    if (prefetchQueue.size() >= prefetchDrainThreshold) {
      drainPrefetch();
    }
  }

  /**
   * Forces a drain of however many keys are currently queued — even far below {@link
   * #prefetchDrainThreshold} — every {@link #prefetchDrainCallInterval} buffered append calls, so a
   * batch that never accumulates enough distinct cold keys to cross the size threshold still gets
   * real background lead time instead of relying solely on {@link #flushBuffer}'s zero-lead-time
   * drain. Called unconditionally from the buffered branch of {@code append}/{@code
   * appendAndGetPreviousCount} (i.e. every buffered call, whether or not that particular call
   * enqueued a key), so the interval reflects overall batch progress rather than enqueue volume
   * alone. No-op when prefetch is disabled.
   */
  private void maybeDrainPeriodically() {
    if (prefetchExecutor == null) {
      return;
    }
    if (++callsSinceLastPeriodicDrain >= prefetchDrainCallInterval) {
      callsSinceLastPeriodicDrain = 0;
      drainPrefetch();
    }
  }

  /**
   * Drains up to {@link #prefetchDrainThreshold} queued keys and, if any were drained and a permit
   * is available, submits a background {@code multiGet} for them on {@link #prefetchExecutor}.
   *
   * <p>Thread-safety: the background task below only calls {@code storage.multiGet} (read-only) and
   * writes to the captured {@code target} map. It never touches {@link #buffer}, {@link
   * #indexCache}, {@link #earlierRangeCountCache}, or {@link #sessionWrittenKeys} — the migrator
   * thread remains the sole mutator of those structures.
   *
   * <p>The batch's {@link #prefetchedBase} reference is captured into a local {@code target}
   * variable at submission time, before the executor runs the task (which may be immediately, on
   * the calling thread, or arbitrarily later on a pool thread). This is the mechanism that prevents
   * cross-batch poisoning: even if this task is still pending when the next {@link
   * #beginBuffered()} swaps {@link #prefetchedBase} to a new instance, this task's closure still
   * only ever sees and writes to {@code target} (the old batch's map), never the new one.
   *
   * <p>If the queue is empty, or the in-flight semaphore has no permits available, this method
   * returns immediately without submitting anything — the keys remain queued (or, if drained but
   * not submitted due to no permit, are effectively dropped from the queue but simply uncovered)
   * and {@link #flushBuffer}'s own synchronous {@code multiGet} fallback will read them at flush
   * time. A saturated prefetch pipeline is therefore never a correctness issue, only a lost
   * optimisation opportunity for this batch.
   */
  private void drainPrefetch() {
    if (prefetchExecutor == null) {
      return;
    }
    final List<Bytes> batch = new ArrayList<>();
    for (Bytes k; (k = prefetchQueue.poll()) != null && batch.size() < prefetchDrainThreshold; ) {
      batch.add(k);
    }
    if (batch.isEmpty() || !prefetchInFlight.tryAcquire()) {
      return; // saturated: flushBuffer will read these keys itself
    }
    // Capture this batch's staging map now — before submission — so a late-running task can never
    // write into a different (later) batch's map. See the field javadoc on prefetchedBase.
    final ConcurrentHashMap<Bytes, Optional<byte[]>> target = prefetchedBase;
    try {
      prefetchExecutor.execute(
          () -> {
            try {
              final List<byte[]> keyBytes = new ArrayList<>(batch.size());
              for (final Bytes k : batch) {
                keyBytes.add(k.toArrayUnsafe());
              }
              final List<Optional<byte[]>> res =
                  storage.multiGet(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, keyBytes);
              for (int i = 0; i < batch.size(); i++) {
                target.put(batch.get(i), res.get(i));
              }
            } catch (final RuntimeException ignored) {
              // Best-effort: flushBuffer falls back to a synchronous read for any key missing from
              // the staging map, so a failed background read never causes incorrect results.
            } finally {
              prefetchInFlight.release();
            }
          });
    } catch (final RuntimeException rejected) {
      // Executor rejected the task (e.g. shutting down); release the permit we acquired above so it
      // isn't leaked, and let flushBuffer's synchronous fallback cover these keys.
      prefetchInFlight.release();
    }
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
   * <p>Two writes are issued on {@code tx}:
   *
   * <ol>
   *   <li>A {@code merge} of {@code offset(block)}'s 3-byte packed form onto {@code
   *       TRIE_NODE_INDEX_ARCHIVE[naturalKey‖rangeId]}, which the append merge operator
   *       concatenates onto the existing packed offset list without this method having to read it
   *       back.
   *   <li>A {@code put} of the updated {@link IndexMetadata} on {@code
   *       TRIE_NODE_INDEX_META_ARCHIVE[naturalKey‖rangeId]}.
   * </ol>
   *
   * <p>If the tail would exceed {@link #subBlockThreshold} entries, the content is instead read,
   * split, and re-{@code put}: the first {@link #subBlockSplitAt} entries (the oldest) move to a
   * new sub-block in {@code TRIE_NODE_SUBBLOCK_ARCHIVE[naturalKey‖rangeId‖subId(8B BE)]}.
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
    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();

    if (buffer != null) {
      // Buffered path: accumulate offset in memory; no storage read or write.
      maybeDrainPeriodically();
      BufferedEntry e = buffer.get(indexKey);
      if (e == null) {
        e = initBufferedEntry(indexKey, naturalKey, rangeId);
        buffer.put(indexKey, e);
        // Prefetch the committed base only for cold keys that might actually exist on disk.
        // Evaluate the bloom BEFORE recording this touch (below) so fresh first-appearances
        // (definitely absent) are not prefetched.
        if (prefetchExecutor != null
            && !e.baseLoaded
            && (sessionWrittenKeys == null || sessionWrittenKeys.mightContain(indexKeyBytes))) {
          enqueueBasePrefetch(indexKey);
        }
      }
      if (sessionWrittenKeys != null) {
        sessionWrittenKeys.put(indexKeyBytes);
      }
      e.pending.add(offset);
      return;
    }

    writeAndGetPreviousMetadata(tx, naturalKey, rangeId, indexKey, indexKeyBytes, offset);
  }

  /**
   * Core write shared by {@link #append} and {@link #appendAndGetPreviousCount}: reads the current
   * (cheap, fixed-width) metadata, blind-merges {@code offset} onto the content key in the common
   * case, and performs the (rare) sub-block split — which does require reading the actual content
   * bytes — when the new tail count would exceed {@link #subBlockThreshold}.
   *
   * @param tx the transaction on which to issue all writes
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey})
   * @param rangeId the range identifier for {@code block}
   * @param indexKey the range key ({@link ArchiveNodeKey#rangeKey})
   * @param indexKeyBytes {@code indexKey.toArrayUnsafe()}, passed in to avoid re-deriving it
   * @param offset the within-range offset of the block being recorded
   * @return the metadata as it was <em>before</em> this write (used by {@link
   *     #appendAndGetPreviousCount} to compute the previous mutation count; ignored by {@link
   *     #append})
   */
  private IndexMetadata writeAndGetPreviousMetadata(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final long rangeId,
      final Bytes indexKey,
      final byte[] indexKeyBytes,
      final int offset) {
    final IndexMetadata before = readMetadataForWrite(indexKey, indexKeyBytes);
    final int newTailCount = before.tailCount() + 1;

    if (newTailCount > subBlockThreshold) {
      final byte[] rawContent =
          storage
              .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes)
              .orElse(new byte[0]);
      RangeRelativeOffsetList current =
          (rawContent.length == 0
                  ? RangeRelativeOffsetList.empty()
                  : RangeRelativeOffsetList.fromBytes(Bytes.wrap(rawContent)))
              .append(offset);
      final RangeRelativeOffsetList head = sliceHead(current, subBlockSplitAt);
      final RangeRelativeOffsetList tail = sliceTail(current, subBlockSplitAt);
      final Bytes subKey = ArchiveNodeKey.subBlockKey(naturalKey, rangeId, before.subCount());
      tx.put(
          KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE,
          subKey.toArrayUnsafe(),
          head.toBytes().toArrayUnsafe());
      // Fresh base value for content: resets the merge-operand chain for this key.
      tx.put(
          KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE,
          indexKeyBytes,
          tail.toBytes().toArrayUnsafe());
      final byte[] newMetadata = writeMetadataValue(before.subCount() + 1, tail.size());
      tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes, newMetadata);
      indexCache.put(indexKey, newMetadata);
    } else {
      tx.merge(
          KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE,
          indexKeyBytes,
          threeByteOffset(offset));
      final byte[] newMetadata = writeMetadataValue(before.subCount(), newTailCount);
      tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes, newMetadata);
      indexCache.put(indexKey, newMetadata);
    }
    if (sessionWrittenKeys != null) {
      sessionWrittenKeys.put(indexKeyBytes);
    }
    return before;
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
      maybeDrainPeriodically();
      BufferedEntry e = buffer.get(indexKey);
      if (e == null) {
        e = initBufferedEntry(indexKey, naturalKey, rangeId);
        buffer.put(indexKey, e);
        // Prefetch the committed base only for cold keys that might actually exist on disk.
        // Evaluate the bloom BEFORE recording this touch (below) so fresh first-appearances
        // (definitely absent) are not prefetched.
        if (prefetchExecutor != null
            && !e.baseLoaded
            && (sessionWrittenKeys == null || sessionWrittenKeys.mightContain(indexKeyBytes))) {
          enqueueBasePrefetch(indexKey);
        }
      }
      if (sessionWrittenKeys != null) {
        sessionWrittenKeys.put(indexKeyBytes);
      }
      final long previousCount =
          earlierCount
              + (long) e.baseSubCount * DEFAULT_SUBBLOCK_SPLIT_AT
              + e.baseTail.size() // unchanged field name for now — Task 11 replaces this
              + e.pending.size();
      e.pending.add(offset);
      return previousCount;
    }

    final IndexMetadata before =
        writeAndGetPreviousMetadata(tx, naturalKey, rangeId, indexKey, indexKeyBytes, offset);
    return earlierCount + (long) before.subCount() * DEFAULT_SUBBLOCK_SPLIT_AT + before.tailCount();
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
  // Index metadata helpers (format: [4B subCount BE][4B tailCount BE])
  // ---------------------------------------------------------------------------

  /** Number of bytes in a serialised {@link IndexMetadata} value. */
  private static final int METADATA_BYTES = 8;

  /**
   * Parsed representation of a value stored in {@code TRIE_NODE_INDEX_META_ARCHIVE}: the number of
   * sub-blocks already stored in {@code TRIE_NODE_SUBBLOCK_ARCHIVE} for a {@code (naturalKey,
   * rangeId)} pair, and the number of entries currently in the tail (the packed offset list stored
   * in {@code TRIE_NODE_INDEX_ARCHIVE}, which no longer carries this count itself).
   */
  record IndexMetadata(int subCount, int tailCount) {
    static final IndexMetadata EMPTY = new IndexMetadata(0, 0);
  }

  /**
   * Parses an {@link IndexMetadata} from raw {@code TRIE_NODE_INDEX_META_ARCHIVE} bytes. Returns
   * {@link IndexMetadata#EMPTY} for missing or short (corrupt) values.
   *
   * @param raw the raw bytes from {@code TRIE_NODE_INDEX_META_ARCHIVE}
   * @return the parsed metadata, or {@link IndexMetadata#EMPTY} if {@code raw} is too short
   */
  static IndexMetadata readMetadataValue(final byte[] raw) {
    if (raw.length < METADATA_BYTES) {
      return IndexMetadata.EMPTY;
    }
    final int subCount =
        ((raw[0] & 0xFF) << 24)
            | ((raw[1] & 0xFF) << 16)
            | ((raw[2] & 0xFF) << 8)
            | (raw[3] & 0xFF);
    final int tailCount =
        ((raw[4] & 0xFF) << 24)
            | ((raw[5] & 0xFF) << 16)
            | ((raw[6] & 0xFF) << 8)
            | (raw[7] & 0xFF);
    return new IndexMetadata(subCount, tailCount);
  }

  /**
   * Serialises a sub-block count and tail entry count into the 8-byte {@code
   * TRIE_NODE_INDEX_META_ARCHIVE} value format.
   *
   * @param subCount the number of existing sub-blocks
   * @param tailCount the number of entries currently in the tail content value
   * @return the serialised 8-byte value
   */
  static byte[] writeMetadataValue(final int subCount, final int tailCount) {
    final byte[] result = new byte[METADATA_BYTES];
    result[0] = (byte) ((subCount >>> 24) & 0xFF);
    result[1] = (byte) ((subCount >>> 16) & 0xFF);
    result[2] = (byte) ((subCount >>> 8) & 0xFF);
    result[3] = (byte) (subCount & 0xFF);
    result[4] = (byte) ((tailCount >>> 24) & 0xFF);
    result[5] = (byte) ((tailCount >>> 16) & 0xFF);
    result[6] = (byte) ((tailCount >>> 8) & 0xFF);
    result[7] = (byte) (tailCount & 0xFF);
    return result;
  }

  /**
   * Reads the current {@link IndexMetadata} for {@code indexKey}, checking the write-through {@link
   * #indexCache} (which now caches metadata bytes, not full content) before falling back to
   * committed storage. Honours fresh-migration bloom short-circuiting like the old content read
   * did.
   *
   * @param indexKey the range key ({@link ArchiveNodeKey#rangeKey})
   * @param indexKeyBytes {@code indexKey.toArrayUnsafe()}, passed in to avoid re-deriving it
   * @return the current metadata, or {@link IndexMetadata#EMPTY} if absent
   */
  private IndexMetadata readMetadataForWrite(final Bytes indexKey, final byte[] indexKeyBytes) {
    final byte[] cached = indexCache.get(indexKey);
    if (cached != null) {
      return readMetadataValue(cached);
    }
    if (sessionWrittenKeys != null && !sessionWrittenKeys.mightContain(indexKeyBytes)) {
      return IndexMetadata.EMPTY;
    }
    return storage
        .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes)
        .map(TrieNodeChangeIndex::readMetadataValue)
        .orElse(IndexMetadata.EMPTY);
  }

  /**
   * Returns the metadata for {@code (naturalKey, rangeId)} directly from committed storage,
   * bypassing {@link #indexCache}. Used by read-only query paths that must not be affected by
   * uncommitted write-path caching.
   *
   * @param indexKeyBytes the range key bytes ({@link ArchiveNodeKey#rangeKey})
   * @return the current metadata, or {@link IndexMetadata#EMPTY} if absent
   */
  private IndexMetadata readCommittedMetadata(final byte[] indexKeyBytes) {
    return storage
        .get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_META_ARCHIVE, indexKeyBytes)
        .map(TrieNodeChangeIndex::readMetadataValue)
        .orElse(IndexMetadata.EMPTY);
  }

  /** Packs a single within-range offset into its 3-byte big-endian merge-operand form. */
  private static byte[] threeByteOffset(final int offset) {
    return new byte[] {
      (byte) ((offset >> 16) & 0xFF), (byte) ((offset >> 8) & 0xFF), (byte) (offset & 0xFF)
    };
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

    // Slow path: concatenate all sub-block buffers (oldest first) followed by the tail.
    // Sub-block entries are strictly older (smaller offsets) than the tail, and each stored buffer
    // is already a valid ascending packed list, so their concatenation is itself ascending.
    //
    // Assembling via RangeRelativeOffsetList.concat allocates the combined buffer once and copies
    // each chunk in a single pass — O(total entries). The previous approach appended offsets one at
    // a time, which was O(n²): RangeRelativeOffsetList.append reallocates and copies the entire
    // growing backing array on every call, so hot nodes with many recorded offsets dominated CPU.
    // Batch-read every sub-block buffer in a single storage round-trip. The sub-block keys are all
    // known (subId 0..subCount-1), so one multiGet replaces subCount sequential store.get calls
    // that
    // each blocked on disk before issuing the next.
    final List<byte[]> subKeys = new ArrayList<>(subCount);
    for (int subId = 0; subId < subCount; subId++) {
      subKeys.add(ArchiveNodeKey.subBlockKey(naturalKey, rangeId, subId).toArrayUnsafe());
    }
    final List<Optional<byte[]>> subRaws =
        storage.multiGet(KeyValueSegmentIdentifier.TRIE_NODE_SUBBLOCK_ARCHIVE, subKeys);

    final List<Bytes> chunks = new ArrayList<>(subCount + 1);
    for (final Optional<byte[]> subRaw : subRaws) {
      if (subRaw.isEmpty()) {
        continue; // should not happen in well-formed data, but skip gracefully
      }
      chunks.add(Bytes.wrap(subRaw.get()));
    }
    chunks.add(tail.toBytes());
    return Optional.of(RangeRelativeOffsetList.concat(chunks));
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
