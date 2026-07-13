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
 *   <li><strong>Locate the nearest FULL checkpoint</strong> — uses a single index-list read via
 *       {@link TrieNodeChangeIndex#getChangeBlocksUpTo} to obtain all change blocks in b*'s range,
 *       then computes the checkpoint position in O(1) from the global mutation index. For b* in
 *       range 0 (or any case where the checkpoint falls within the current range) this is a single
 *       RocksDB read. For cross-range cases where the checkpoint is in an earlier range, the
 *       original backward walk loop is used as a fallback.
 *   <li><strong>Reconstruct</strong> — call {@link TrieNodeDiffCodec#reconstruct(Bytes, List)} with
 *       the FULL entry as the base and the ordered list of DIFF entries between the checkpoint and
 *       {@code b*} (inclusive) to produce the final node RLP.
 * </ol>
 *
 * <h2>Termination guarantee</h2>
 *
 * The write path (Task 3.3) guarantees that every {@link #CHECKPOINT_INTERVAL} mutations for a key
 * emits a FULL checkpoint. Therefore the backward walk (when used as a fallback) terminates within
 * at most {@code CHECKPOINT_INTERVAL - 1} steps. The {@link #MAX_BACKWARD_WALK_STEPS} guard is a
 * safety net for backfill scenarios and corrupt data.
 */
public final class TrieNodeHistoryReader {

  private static final Logger LOG = LoggerFactory.getLogger(TrieNodeHistoryReader.class);

  /**
   * Every {@code CHECKPOINT_INTERVAL}-th mutation for a node emits a FULL entry. This value must
   * match {@code BonsaiArchiveTrieNodeStrategy.CHECKPOINT_INTERVAL} (same codebase, different
   * package). The optimised path in {@link #nodeAt} relies on this value to locate checkpoints in
   * O(1) from the global mutation index.
   */
  static final int CHECKPOINT_INTERVAL = 16;

  /**
   * Maximum number of backward steps before giving up the walk. In steady state this bound is never
   * reached (CHECKPOINT_INTERVAL - 1 = 15 steps suffice), but it guards against corrupt data or
   * incomplete backfill.
   */
  static final int MAX_BACKWARD_WALK_STEPS = 64;

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
    final long rangeId = bStar / index.rangeSize;
    final int earlierCount = index.countMutationsInEarlierRanges(naturalKey, rangeId);
    final long globalMutationOfBStar = (long) earlierCount + inRangeCount - 1;

    final long checkpointMutation =
        globalMutationOfBStar - (globalMutationOfBStar % CHECKPOINT_INTERVAL);
    final long checkpointWithinRange = checkpointMutation - earlierCount;

    if (checkpointWithinRange >= 0 && checkpointWithinRange < inRangeCount) {
      final int cpIdx = (int) checkpointWithinRange;
      final long checkpointBlock = changeBlocks[cpIdx];

      // Batch-read the checkpoint entry plus every intervening DIFF up to bStar in a single storage
      // round-trip. These block numbers are all known here (changeBlocks[cpIdx..inRangeCount-1]),
      // so
      // one multiGet replaces the previous chain of up to CHECKPOINT_INTERVAL sequential store.get
      // calls that each blocked on disk before issuing the next.
      final long[] spanBlocks = Arrays.copyOfRange(changeBlocks, cpIdx, inRangeCount);
      final List<Optional<Bytes>> spanEntries = store.getAll(naturalKey, spanBlocks);

      final Optional<Bytes> fullEntryOpt = spanEntries.get(0);
      if (fullEntryOpt.isEmpty()) {
        LOG.warn(
            "TrieNodeHistoryReader: index references checkpoint block {} for key {} but store"
                + " has no entry; index/store mismatch — returning empty",
            checkpointBlock,
            naturalKey);
        return Optional.empty();
      }
      Bytes fullEntry = fullEntryOpt.get();
      final TrieNodeDiffCodec.Decoded fullDecoded = TrieNodeDiffCodec.decode(fullEntry);
      if (!fullDecoded.isFull()) {
        LOG.warn(
            "TrieNodeHistoryReader: expected FULL entry at checkpoint block {} for key {} but"
                + " got metadata 0x{}; falling back to backward walk",
            checkpointBlock,
            naturalKey,
            Integer.toHexString(Byte.toUnsignedInt(fullDecoded.metadata())));
        // Fall through to the backward walk fallback below.
      } else {
        final int diffCount = spanBlocks.length - 1;
        if (diffCount == 0) {
          return Optional.of(fullDecoded.fullNode());
        }
        final List<Bytes> diffEntries = new ArrayList<>(diffCount);
        for (int i = 1; i < spanBlocks.length; i++) {
          final Optional<Bytes> diffOpt = spanEntries.get(i);
          if (diffOpt.isEmpty()) {
            LOG.warn(
                "TrieNodeHistoryReader: index references block {} for key {} but store has no"
                    + " entry; index/store mismatch — returning empty",
                spanBlocks[i],
                naturalKey);
            return Optional.empty();
          }
          final Bytes diffEntry = diffOpt.get();
          final TrieNodeDiffCodec.Decoded decoded = TrieNodeDiffCodec.decode(diffEntry);
          if (decoded.isDeletion()) {
            LOG.warn(
                "TrieNodeHistoryReader: tombstone in diff chain for key {} at block {}"
                    + " — returning empty",
                naturalKey,
                spanBlocks[i]);
            return Optional.empty();
          }
          if (decoded.isFull()) {
            // A newer FULL entry (FULL_ABOVE_DEPTH or a later checkpoint) supersedes the prior
            // checkpoint — use it as the new reconstruction base and discard accumulated DIFFs.
            fullEntry = diffEntry;
            diffEntries.clear();
          } else {
            diffEntries.add(diffEntry);
          }
        }
        return Optional.of(TrieNodeDiffCodec.reconstruct(fullEntry, diffEntries));
      }
    }

    // Fallback: backward walk (cross-range case or corrupt checkpoint entry).
    return backwardWalkFallback(naturalKey, bStar, bStarEntry);
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
