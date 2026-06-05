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
      final long[] changeBlocks = changeBlocksOpt.get(); // sorted ascending, last entry == bStar
      final int inRangeCount = changeBlocks.length; // number of mutations in b*'s range up to b*

      // Compute the global (0-based) mutation index of b*.
      final long rangeId = bStar / index.rangeSize;
      final int earlierCount = index.countMutationsInEarlierRanges(naturalKey, rangeId);
      final long globalMutationOfBStar = (long) earlierCount + inRangeCount - 1;

      // The nearest FULL checkpoint is at mutation index: checkpointMutation =
      // globalMutationOfBStar
      // - (globalMutationOfBStar % CHECKPOINT_INTERVAL). Because the write path (Task 3.3) emits
      // FULL at every CHECKPOINT_INTERVAL-th mutation (0-based: mutations 0, 16, 32, …), the
      // checkpoint mutation index's within-range position is:
      final long checkpointMutation =
          globalMutationOfBStar - (globalMutationOfBStar % CHECKPOINT_INTERVAL);
      final long checkpointWithinRange = checkpointMutation - earlierCount;

      if (checkpointWithinRange >= 0 && checkpointWithinRange < inRangeCount) {
        // Checkpoint is in the current range — use the in-memory change list.
        final int cpIdx = (int) checkpointWithinRange;
        final long checkpointBlock = changeBlocks[cpIdx];

        // Fetch the FULL entry at the checkpoint block.
        final Optional<Bytes> fullEntryOpt = store.get(naturalKey, checkpointBlock);
        if (fullEntryOpt.isEmpty()) {
          LOG.warn(
              "TrieNodeHistoryReader: index references checkpoint block {} for key {} but store"
                  + " has no entry; index/store mismatch — returning empty",
              checkpointBlock,
              naturalKey);
          return Optional.empty();
        }
        final Bytes fullEntry = fullEntryOpt.get();
        final TrieNodeDiffCodec.Decoded fullDecoded = TrieNodeDiffCodec.decode(fullEntry);
        if (!fullDecoded.isFull()) {
          // Checkpoint entry is not actually FULL — data integrity violation; fall through to
          // backward walk below rather than crashing.
          LOG.warn(
              "TrieNodeHistoryReader: expected FULL entry at checkpoint block {} for key {} but"
                  + " got metadata 0x{}; falling back to backward walk",
              checkpointBlock,
              naturalKey,
              Integer.toHexString(Byte.toUnsignedInt(fullDecoded.metadata())));
          // Fall through to the backward-walk fallback at end of method.
        } else {
          // Happy path: collect diff entries [cpIdx+1 .. inRangeCount-1] in ascending order.
          final int diffCount = inRangeCount - cpIdx - 1;
          if (diffCount == 0) {
            // The checkpoint IS b* — but b* was decoded as DIFF earlier. This shouldn't happen
            // (the write path would have stored it as FULL). Handle defensively.
            return Optional.of(fullDecoded.fullNode());
          }
          final List<Bytes> diffEntries = new ArrayList<>(diffCount);
          for (int i = cpIdx + 1; i < inRangeCount; i++) {
            final Optional<Bytes> diffOpt = store.get(naturalKey, changeBlocks[i]);
            if (diffOpt.isEmpty()) {
              LOG.warn(
                  "TrieNodeHistoryReader: index references block {} for key {} but store has no"
                      + " entry; index/store mismatch — returning empty",
                  changeBlocks[i],
                  naturalKey);
              return Optional.empty();
            }
            final Bytes diffEntry = diffOpt.get();
            final TrieNodeDiffCodec.Decoded decoded = TrieNodeDiffCodec.decode(diffEntry);
            if (decoded.isDeletion()) {
              // Tombstone mid-chain — data integrity violation.
              LOG.warn(
                  "TrieNodeHistoryReader: tombstone in diff chain for key {} at block {}"
                      + " — returning empty",
                  naturalKey,
                  changeBlocks[i]);
              return Optional.empty();
            }
            diffEntries.add(diffEntry);
          }
          return Optional.of(TrieNodeDiffCodec.reconstruct(fullEntry, diffEntries));
        }
      }
      // checkpointWithinRange < 0: checkpoint is in an earlier range — fall through to backward
      // walk. This is the cross-range case and is rare for chains < RANGE_SIZE blocks.
    }

    // Fallback: backward walk loop (original algorithm). Used when:
    //  - getChangeBlocksUpTo returned empty (bloom/marker miss, or all entries in range > bStar)
    //  - the FULL checkpoint lies in an earlier range (cross-range case)
    //  - the checkpoint entry turned out not to be FULL (data integrity issue)
    //
    // Collect non-FULL diff entries in descending block order; reverse at the end for
    // reconstruct().
    // Invariant on entry: bStarEntry is a DIFF. We add it first and walk backward.
    // The backward chain ends when we hit a FULL entry, a tombstone (error), or exhaust the index.

    final List<Bytes> entriesDescending = new ArrayList<>();
    entriesDescending.add(bStarEntry);

    Bytes fullEntry = null;
    long walkBlock = bStar;
    int steps = 0;

    while (steps < MAX_BACKWARD_WALK_STEPS) {
      steps++;
      if (walkBlock == 0) {
        // Can't walk further back than block 0.
        break;
      }
      final Optional<Long> prevOpt = index.latestChangeBlock(naturalKey, walkBlock - 1);
      if (prevOpt.isEmpty()) {
        // No earlier change for this key — data is incomplete (should not happen with CREATION
        // rule)
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
        // Found the FULL checkpoint — this is our base.
        fullEntry = prevEntry;
        // The diffs to apply are those in entriesDescending (reversed to ascending order below).
        // prevBlock is the FULL base, not a DIFF, so it is not added to entriesDescending.
        break;
      }

      if (prevDecoded.isDeletion()) {
        // A deletion tombstone in the backward chain means the node was deleted at prevBlock and
        // then re-created at some block between prevBlock and walkBlock. The CREATION rule (Task
        // 3.3) mandates that a re-creation starts with ENTRY_FULL | CREATION, which would have
        // been caught by the isFull() check above. Reaching a tombstone without finding a FULL
        // checkpoint first is a data-integrity violation — do not pass the tombstone to reconstruct
        // (it would throw IllegalArgumentException via requireDiffEntry).
        LOG.warn(
            "TrieNodeHistoryReader: tombstone in backward chain for key {} at block {}"
                + " — returning empty",
            naturalKey,
            prevBlock);
        return Optional.empty();
      }

      // Still a DIFF entry — collect it and continue walking backward.
      // Collect non-FULL diff entries in descending block order; reverse at the end for
      // reconstruct.
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

    // Step 6: reconstruct.
    // entriesDescending is [bStar_entry, b(n-1)_entry, ..., b(checkpoint+1)_entry] — i.e.
    // newest first. Reverse to get ascending order: [b(checkpoint+1), ..., bStar].
    final List<Bytes> diffEntries = new ArrayList<>(entriesDescending.size());
    for (int i = entriesDescending.size() - 1; i >= 0; i--) {
      diffEntries.add(entriesDescending.get(i));
    }

    return Optional.of(TrieNodeDiffCodec.reconstruct(fullEntry, diffEntries));
  }
}
