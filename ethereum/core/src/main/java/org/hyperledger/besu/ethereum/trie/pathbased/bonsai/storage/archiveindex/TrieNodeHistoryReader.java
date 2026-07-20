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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconstructs the historical state of a trie node at a given block by combining point-access reads
 * from {@link TrieNodeHistoryStore} with change-block lookups from {@link TrieNodeChangeIndex}
 * (Design 5, Task 3.2).
 *
 * <h2>Algorithm — {@link #nodeAt(Bytes, long)}</h2>
 *
 * <ol>
 *   <li><strong>Find the latest change block ≤ targetBlock</strong> — call {@link
 *       TrieNodeChangeIndex#latestChangeBlock(Bytes, long)} to find {@code b*}, the most-recent
 *       block at which the node changed. If absent, the node was never written for this key before
 *       {@code targetBlock}: return empty.
 *   <li><strong>Fetch the entry at b*</strong> — read from the store. If absent (shouldn't happen
 *       in well-formed data), log a warning and return empty.
 *   <li><strong>Decode and check for tombstone</strong> — if the entry is a DELETION tombstone, the
 *       node was deleted: return empty.
 *   <li><strong>If entry is FULL</strong> — no reconstruction needed; return the embedded node
 *       directly.
 *   <li><strong>Locate the nearest FULL checkpoint</strong> — obtain all change blocks in b*'s
 *       range via a single index-list read ({@link TrieNodeChangeIndex#getChangeBlocksUpTo}), then
 *       scan the trailing {@link #RECONSTRUCT_WINDOW} entries backward — read in one batched
 *       multiGet — for the newest FULL at or before b*. Scanning (rather than computing a
 *       checkpoint position) is required because the write/migration path does not always place
 *       FULL entries at globally {@link #CHECKPOINT_INTERVAL}-aligned positions. If no FULL is
 *       found within the window the checkpoint lies further back (an earlier range or a long diff
 *       chain) and the bounded backward walk is used as a fallback.
 *   <li><strong>Reconstruct</strong> — call {@link TrieNodeDiffCodec#reconstruct(Bytes, List)} with
 *       the FULL entry as the base and the ordered list of DIFF entries between the checkpoint and
 *       {@code b*} (inclusive) to produce the final node RLP.
 * </ol>
 *
 * <h2>Termination guarantee</h2>
 *
 * The write path (Task 3.3) emits a FULL checkpoint at most every {@code
 * BonsaiArchiveTrieNodeStrategy.SHALLOW_CHECKPOINT_INTERVAL} (32) mutations for a key — the largest
 * of the depth-tiered checkpoint intervals — so as long as {@link #RECONSTRUCT_WINDOW} is at least
 * that large, the newest FULL is normally found within the batched window scan. The {@link
 * #MAX_BACKWARD_WALK_STEPS} guard bounds the cross-range backward-walk fallback for backfill
 * scenarios and corrupt data.
 */
public final class TrieNodeHistoryReader {

  private static final Logger LOG = LoggerFactory.getLogger(TrieNodeHistoryReader.class);

  /**
   * Deep-tier checkpoint spacing: nodes at depth &gt;= 3 emit a FULL entry every {@code
   * CHECKPOINT_INTERVAL}-th mutation (see {@code
   * BonsaiArchiveTrieNodeStrategy.DEEP_CHECKPOINT_INTERVAL}). This is not the binding safety
   * invariant for this reader — the actual requirement is an upper bound, not an equality: {@link
   * #RECONSTRUCT_WINDOW} must be greater than or equal to the largest write-path checkpoint
   * interval across all depth tiers, which is currently {@code
   * BonsaiArchiveTrieNodeStrategy.SHALLOW_CHECKPOINT_INTERVAL} = 32, so the backward window scan in
   * {@link #reconstructFromChangeBlocks} normally finds a FULL without falling back to the walk.
   */
  static final int CHECKPOINT_INTERVAL = 16;

  /**
   * Maximum number of backward steps before giving up the walk. In steady state this bound is never
   * reached (largest write-path checkpoint interval - 1, i.e. {@code
   * BonsaiArchiveTrieNodeStrategy.SHALLOW_CHECKPOINT_INTERVAL} - 1 = 31 steps suffice), but it
   * guards against corrupt data or incomplete backfill.
   */
  public static final int MAX_BACKWARD_WALK_STEPS = 64;

  /**
   * Number of trailing change-block entries {@link #reconstructFromChangeBlocks} reads in one
   * batched multiGet when scanning backward for the nearest FULL checkpoint. Sized well above the
   * largest write-path checkpoint interval ({@code
   * BonsaiArchiveTrieNodeStrategy.SHALLOW_CHECKPOINT_INTERVAL} = 32) to absorb the observed spread
   * between where FULL entries are actually written and where a naive interval-aligned position
   * would fall; if no FULL is found within the window the reconstruction falls back to the bounded
   * cross-range backward walk.
   */
  public static final int RECONSTRUCT_WINDOW = 64;

  private final TrieNodeHistoryStore store;
  private final TrieNodeChangeIndex index;

  /**
   * Constructs a reader backed by the given store and change index.
   *
   * @param store the point-access trie-node history store; must not be {@code null}
   * @param index the change-block index; must not be {@code null}
   * @throws NullPointerException if either argument is {@code null}
   */
  public TrieNodeHistoryReader(final TrieNodeHistoryStore store, final TrieNodeChangeIndex index) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.index = Objects.requireNonNull(index, "index must not be null");
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Returns the trie-node RLP for {@code naturalKey} at {@code targetBlock}, reconstructed from the
   * nearest FULL checkpoint plus any intervening DIFF entries.
   *
   * <p>The returned bytes are the raw node RLP (not a codec entry) — suitable for direct use as a
   * trie node.
   *
   * <p><strong>Tombstone semantics:</strong> if the node was deleted at or before {@code
   * targetBlock} and not re-created before {@code targetBlock}, {@link Optional#empty()} is
   * returned.
   *
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey}); must not be
   *     {@code null}
   * @param targetBlock the block number to reconstruct the node at (inclusive)
   * @return the reconstructed node RLP, or empty if the node did not exist at {@code targetBlock}
   * @throws NullPointerException if {@code naturalKey} is {@code null}
   * @throws IllegalArgumentException if {@code targetBlock} is negative
   */
  public Optional<Bytes> nodeAt(final Bytes naturalKey, final long targetBlock) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    if (targetBlock < 0) {
      throw new IllegalArgumentException("targetBlock must be >= 0, got " + targetBlock);
    }

    // Step 1: find the latest change block <= targetBlock.
    final Optional<Long> latestOpt = index.latestChangeBlock(naturalKey, targetBlock);
    if (latestOpt.isEmpty()) {
      // No change recorded at or before targetBlock — node never written for this key.
      return Optional.empty();
    }
    final long bStar = latestOpt.get();

    // Step 2: fetch the entry at b*.
    final Optional<Bytes> entryOpt = store.get(naturalKey, bStar);
    if (entryOpt.isEmpty()) {
      LOG.warn(
          "TrieNodeHistoryReader: index references block {} for key {} but store has no entry;"
              + " index/store mismatch — returning empty",
          bStar,
          naturalKey);
      return Optional.empty();
    }
    final Bytes bStarEntry = entryOpt.get();

    // Step 3: decode and check for tombstone.
    final TrieNodeDiffCodec.Decoded bStarDecoded = TrieNodeDiffCodec.decode(bStarEntry);
    if (bStarDecoded.isDeletion()) {
      // Node was deleted at b* (or the latest version is a tombstone).
      return Optional.empty();
    }

    // Step 4: if FULL, return the embedded node directly — no reconstruction needed.
    if (bStarDecoded.isFull()) {
      return Optional.of(bStarDecoded.fullNode());
    }

    // Step 5: b* is a DIFF — locate the nearest FULL checkpoint and collect the diff chain.
    //
    // Optimised path: read all change blocks in b*'s range in a single index-list read, then
    // compute the checkpoint position from the global mutation index in O(1). This replaces the
    // old loop of up to CHECKPOINT_INTERVAL-1 individual latestChangeBlock calls (each doing 3
    // RocksDB reads: bloom, range marker, list binary search).
    //
    // Cross-range fallback: if the FULL checkpoint is in an earlier range (i.e. the node's history
    // spans multiple ranges and the checkpoint falls before the current range boundary), the
    // original backward-walk loop is used as a safe fallback. This is rare in practice for chains
    // shorter than RANGE_SIZE = 1,000,000 blocks.

    final Optional<long[]> changeBlocksOpt = index.getChangeBlocksUpTo(naturalKey, bStar);

    if (changeBlocksOpt.isPresent()) {
      return reconstructFromChangeBlocks(naturalKey, bStar, bStarEntry, changeBlocksOpt.get());
    }

    // getChangeBlocksUpTo returned empty — fall back to the backward walk.
    return backwardWalkFallback(naturalKey, bStar, bStarEntry);
  }

  // ---------------------------------------------------------------------------
  // Package-private overload with preloaded range list (avoids duplicate index read)
  // ---------------------------------------------------------------------------

  /**
   * Reconstructs the trie node at {@code bStar} using an already-fetched {@link
   * RangeRelativeOffsetList} for the same range, avoiding a second {@link
   * TrieNodeChangeIndex#getChangeBlocksUpTo} call.
   *
   * <p>This overload is used by {@link ArchiveProofNodeLoader} when it has already read the range
   * list once (via {@link TrieNodeChangeIndex#readRangeList}) to determine {@code bStar}. Passing
   * the preloaded list here prevents the triple-read pattern: bloom + marker + list read that would
   * otherwise happen in both {@link TrieNodeChangeIndex#modifiedAfter} and {@link
   * TrieNodeChangeIndex#latestChangeBlock} as well as here.
   *
   * <p>The preloaded list must cover the full range (all sub-blocks + tail, unsieved — not filtered
   * by any ceiling), as produced by {@link TrieNodeChangeIndex#readRangeList}. The method filters
   * the list in memory to obtain change blocks up to {@code bStar}.
   *
   * <p>If {@code bStar} is in a <em>different</em> range than {@code rangeId} (cross-range case),
   * this overload falls back to the standard {@link #nodeAt(Bytes, long)} to avoid incorrect
   * reconstruction.
   *
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey}); must not be
   *     {@code null}
   * @param bStar the latest change block ≤ targetBlock, already resolved by the caller
   * @param preloadedList the full (unfiltered) offset list for {@code (naturalKey, rangeId)},
   *     produced by {@link TrieNodeChangeIndex#readRangeList}
   * @param rangeId the range identifier that {@code preloadedList} covers
   * @return the reconstructed node RLP, or empty if the node did not exist at {@code bStar}
   * @throws NullPointerException if {@code naturalKey} or {@code preloadedList} is {@code null}
   */
  Optional<Bytes> nodeAt(
      final Bytes naturalKey,
      final long bStar,
      final RangeRelativeOffsetList preloadedList,
      final long rangeId) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    Objects.requireNonNull(preloadedList, "preloadedList must not be null");

    // If bStar is in a different range than the preloaded list, fall back to the standard path.
    if (bStar / index.rangeSize != rangeId) {
      return nodeAt(naturalKey, bStar);
    }

    // Fetch the entry at bStar.
    final Optional<Bytes> entryOpt = store.get(naturalKey, bStar);
    if (entryOpt.isEmpty()) {
      LOG.warn(
          "TrieNodeHistoryReader: index references block {} for key {} but store has no entry;"
              + " index/store mismatch — returning empty",
          bStar,
          naturalKey);
      return Optional.empty();
    }
    final Bytes bStarEntry = entryOpt.get();

    // Decode and check for tombstone.
    final TrieNodeDiffCodec.Decoded bStarDecoded = TrieNodeDiffCodec.decode(bStarEntry);
    if (bStarDecoded.isDeletion()) {
      return Optional.empty();
    }

    // If FULL, return the embedded node directly — no reconstruction needed.
    if (bStarDecoded.isFull()) {
      return Optional.of(bStarDecoded.fullNode());
    }

    // bStar is a DIFF — use the preloaded list to locate the nearest FULL checkpoint.
    // Filter the preloaded list to offsets ≤ bStar's within-range offset to get change blocks up
    // to bStar, then delegate to the same optimised checkpoint logic as nodeAt(naturalKey, bStar).
    final int bStarWithinRange = (int) (bStar - rangeId * index.rangeSize);
    final int listSize = preloadedList.size();
    final int[] offsets = new int[listSize];
    int count = 0;
    for (int i = 0; i < listSize; i++) {
      final int offset = preloadedList.get(i);
      if (offset > bStarWithinRange) {
        break; // sorted ascending
      }
      offsets[count++] = offset;
    }

    // Build the absolute block array (equivalent to getChangeBlocksUpTo output).
    final long rangeBase = rangeId * index.rangeSize;
    final long[] changeBlocks = new long[count];
    for (int i = 0; i < count; i++) {
      changeBlocks[i] = rangeBase + offsets[i];
    }

    if (changeBlocks.length == 0) {
      // No changes ≤ bStar in this range (shouldn't happen if bStar came from the list).
      // Fall back to the backward walk via standard nodeAt.
      return nodeAt(naturalKey, bStar);
    }

    // Delegate to the shared reconstruction logic using the in-memory change block array.
    return reconstructFromChangeBlocks(naturalKey, bStar, bStarEntry, changeBlocks);
  }

  /**
   * Shared DIFF-reconstruction helper used by both {@link #nodeAt(Bytes, long)} and the preloaded
   * overload. Given the sorted ascending {@code changeBlocks} array (all change blocks ≤ bStar in
   * bStar's range), locates the nearest FULL checkpoint and applies intervening DIFFs.
   *
   * @param naturalKey the account or storage natural key
   * @param bStar the latest change block ≤ targetBlock
   * @param bStarEntry the already-decoded raw codec entry at bStar (known to be a DIFF)
   * @param changeBlocks sorted ascending absolute block numbers ≤ bStar in bStar's range
   * @return the reconstructed node RLP, or empty if reconstruction fails
   */
  private Optional<Bytes> reconstructFromChangeBlocks(
      final Bytes naturalKey, final long bStar, final Bytes bStarEntry, final long[] changeBlocks) {

    final int inRangeCount = changeBlocks.length;

    // Locate the nearest FULL checkpoint at or before bStar by scanning the change-block list
    // backward over a bounded trailing window, read in a single batched multiGet.
    //
    // This deliberately does NOT compute the checkpoint position arithmetically: the
    // write/migration
    // path does not always place FULL entries at globally CHECKPOINT_INTERVAL-aligned positions, so
    // a computed position is unreliable and previously landed on a DIFF, forcing a per-step
    // sequential backward walk (one index + one store read per step) on essentially every deep-node
    // reconstruction. Reading the window in one round-trip and scanning it is correct regardless of
    // where the FULL was actually written, and replaces N blocking round-trips with one.
    final int windowSize = Math.min(inRangeCount, RECONSTRUCT_WINDOW);
    final long[] windowBlocks =
        Arrays.copyOfRange(changeBlocks, inRangeCount - windowSize, inRangeCount);
    final List<Optional<Bytes>> windowEntries = store.getAll(naturalKey, windowBlocks);

    // Scan newest → oldest for the nearest FULL. A missing entry or a deletion tombstone reached
    // before a FULL is handled exactly as before (index/store mismatch, or a deleted node → empty).
    int fullPos = -1;
    for (int i = windowSize - 1; i >= 0; i--) {
      final Optional<Bytes> entryOpt = windowEntries.get(i);
      if (entryOpt.isEmpty()) {
        LOG.warn(
            "TrieNodeHistoryReader: index references block {} for key {} but store has no entry;"
                + " index/store mismatch — returning empty",
            windowBlocks[i],
            naturalKey);
        return Optional.empty();
      }
      final TrieNodeDiffCodec.Decoded decoded = TrieNodeDiffCodec.decode(entryOpt.get());
      if (decoded.isDeletion()) {
        LOG.warn(
            "TrieNodeHistoryReader: tombstone in diff chain for key {} at block {} — returning"
                + " empty",
            naturalKey,
            windowBlocks[i]);
        return Optional.empty();
      }
      if (decoded.isFull()) {
        fullPos = i;
        break;
      }
    }

    if (fullPos < 0) {
      // No FULL within the window — the checkpoint lies further back (an earlier range, or an
      // unusually long diff chain). Fall back to the bounded cross-range backward walk. Rare.
      return backwardWalkFallback(naturalKey, bStar, bStarEntry);
    }

    // Base is the FULL at fullPos; every entry above it up to bStar is a DIFF by construction (the
    // backward scan stopped at the newest FULL), so apply them in ascending order.
    final Bytes fullEntry = windowEntries.get(fullPos).get();
    final int diffCount = windowSize - fullPos - 1;
    if (diffCount == 0) {
      return Optional.of(TrieNodeDiffCodec.decode(fullEntry).fullNode());
    }
    final List<Bytes> diffEntries = new ArrayList<>(diffCount);
    for (int i = fullPos + 1; i < windowSize; i++) {
      diffEntries.add(windowEntries.get(i).get());
    }
    return Optional.of(TrieNodeDiffCodec.reconstruct(fullEntry, diffEntries));
  }

  /**
   * Backward-walk fallback used when the FULL checkpoint lies in an earlier range or when the
   * checkpoint entry is unexpectedly not FULL. Identical in semantics to the original fallback in
   * {@link #nodeAt(Bytes, long)}.
   */
  private Optional<Bytes> backwardWalkFallback(
      final Bytes naturalKey, final long bStar, final Bytes bStarEntry) {
    final List<Bytes> entriesDescending = new ArrayList<>();
    entriesDescending.add(bStarEntry);

    Bytes fullEntry = null;
    long walkBlock = bStar;
    int steps = 0;

    while (steps < MAX_BACKWARD_WALK_STEPS) {
      steps++;
      if (walkBlock == 0) {
        break;
      }
      final Optional<Long> prevOpt = index.latestChangeBlock(naturalKey, walkBlock - 1);
      if (prevOpt.isEmpty()) {
        break;
      }
      final long prevBlock = prevOpt.get();
      final Optional<Bytes> prevEntryOpt = store.get(naturalKey, prevBlock);
      if (prevEntryOpt.isEmpty()) {
        LOG.warn(
            "TrieNodeHistoryReader: index references block {} for key {} but store has no entry;"
                + " index/store mismatch — returning empty",
            prevBlock,
            naturalKey);
        return Optional.empty();
      }
      final Bytes prevEntry = prevEntryOpt.get();
      final TrieNodeDiffCodec.Decoded prevDecoded = TrieNodeDiffCodec.decode(prevEntry);

      if (prevDecoded.isFull()) {
        fullEntry = prevEntry;
        break;
      }

      if (prevDecoded.isDeletion()) {
        LOG.warn(
            "TrieNodeHistoryReader: tombstone in backward chain for key {} at block {}"
                + " — returning empty",
            naturalKey,
            prevBlock);
        return Optional.empty();
      }

      entriesDescending.add(prevEntry);
      walkBlock = prevBlock;
    }

    if (fullEntry == null) {
      LOG.warn(
          "TrieNodeHistoryReader: could not find FULL checkpoint for key {} at or before block {}"
              + " after {} backward steps — returning empty",
          naturalKey,
          bStar,
          steps);
      return Optional.empty();
    }

    final List<Bytes> diffEntries = new ArrayList<>(entriesDescending.size());
    for (int i = entriesDescending.size() - 1; i >= 0; i--) {
      diffEntries.add(entriesDescending.get(i));
    }
    return Optional.of(TrieNodeDiffCodec.reconstruct(fullEntry, diffEntries));
  }
}
