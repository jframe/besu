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

import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Adapts the Design-5 trie-node differential index (Stage 2/3) to the {@link NodeLoader} interface
 * used by {@link org.hyperledger.besu.ethereum.proof.WorldStateProofProvider}.
 *
 * <p>Two {@link NodeLoader} views are provided:
 *
 * <ul>
 *   <li>{@link #accountNodeLoader()} — for the account state trie; {@code naturalKey = location}.
 *   <li>{@link #storageNodeLoader(Bytes32)} — for a specific account's storage trie; {@code
 *       naturalKey = accountHash ‖ location}.
 * </ul>
 *
 * <h2>Algorithm — {@link #resolveNodeAt(Bytes, Bytes32)}</h2>
 *
 * <ol>
 *   <li><strong>Fast path</strong> — if {@code !index.modifiedAfter(naturalKey, targetBlock,
 *       headBlock)}, the live trie node (keyed by {@code naturalKey} in {@code
 *       TRIE_BRANCH_STORAGE}) is the correct historical node. Return it directly.
 *   <li><strong>History path</strong> — otherwise, delegate to {@link
 *       TrieNodeHistoryReader#nodeAt(Bytes, long)} using {@code targetBlock} as the target. If the
 *       reader returns empty (node absent at target block), return empty.
 *   <li><strong>Hash verification</strong> — compute {@code keccak256(node)} and compare to {@code
 *       expectedHash}. A mismatch indicates index/store inconsistency; throw {@link
 *       IllegalStateException} (fail-closed — never serve silently incorrect proof data).
 * </ol>
 *
 * <h2>Fail-closed invariant</h2>
 *
 * A hash mismatch is always thrown as {@link IllegalStateException}. This is intentional: a
 * mismatch means the index, history store, or live trie are inconsistent (data corruption). Serving
 * a node that does not match the expected hash would silently produce an incorrect proof, which is
 * worse than failing loudly.
 */
public final class ArchiveProofNodeLoader {

  private final TrieNodeChangeIndex index;
  private final TrieNodeHistoryReader historyReader;
  private final SegmentedKeyValueStorage liveStorage;
  private final long targetBlock;
  private final long headBlock;

  /**
   * Constructs a new loader for historical proof resolution.
   *
   * @param index the trie-node change index (Design 5, Stage 2); must not be {@code null}
   * @param historyReader the history reader for reconstructing past node versions (Stage 3); must
   *     not be {@code null}
   * @param liveStorage the live segmented KV storage containing {@code TRIE_BRANCH_STORAGE} (for
   *     the fast-path live-trie read); must not be {@code null}
   * @param targetBlock T — the historical block being proved (inclusive)
   * @param headBlock the chain-head block number at the time of the proof request; passed to {@link
   *     TrieNodeChangeIndex#modifiedAfter} to bound the change-detection window
   * @throws NullPointerException if any reference argument is {@code null}
   * @throws IllegalArgumentException if {@code headBlock < targetBlock}
   */
  public ArchiveProofNodeLoader(
      final TrieNodeChangeIndex index,
      final TrieNodeHistoryReader historyReader,
      final SegmentedKeyValueStorage liveStorage,
      final long targetBlock,
      final long headBlock) {
    this.index = Objects.requireNonNull(index, "index must not be null");
    this.historyReader = Objects.requireNonNull(historyReader, "historyReader must not be null");
    this.liveStorage = Objects.requireNonNull(liveStorage, "liveStorage must not be null");
    if (headBlock < targetBlock) {
      throw new IllegalArgumentException(
          "headBlock must be >= targetBlock, got headBlock="
              + headBlock
              + ", targetBlock="
              + targetBlock);
    }
    this.targetBlock = targetBlock;
    this.headBlock = headBlock;
  }

  // ---------------------------------------------------------------------------
  // Public API — NodeLoader factories
  // ---------------------------------------------------------------------------

  /**
   * Returns a {@link NodeLoader} for the account state trie.
   *
   * <p>The natural key for each node is {@code location} (the compact path).
   *
   * @return a NodeLoader for account-trie nodes at {@code targetBlock}
   */
  public NodeLoader accountNodeLoader() {
    return (location, expectedHash) ->
        resolveNodeAt(ArchiveNodeKey.account(location), expectedHash);
  }

  /**
   * Returns a {@link NodeLoader} for the storage trie of the given account.
   *
   * <p>The natural key for each node is {@code accountHash ‖ location}.
   *
   * @param accountHash the 32-byte hash of the account that owns the storage trie; must be exactly
   *     32 bytes
   * @return a NodeLoader for storage-trie nodes at {@code targetBlock}
   */
  public NodeLoader storageNodeLoader(final Bytes32 accountHash) {
    Objects.requireNonNull(accountHash, "accountHash must not be null");
    return (location, expectedHash) ->
        resolveNodeAt(ArchiveNodeKey.storage(accountHash, location), expectedHash);
  }

  // ---------------------------------------------------------------------------
  // Core resolution logic
  // ---------------------------------------------------------------------------

  /**
   * Resolves the trie node for {@code naturalKey} at {@code targetBlock}.
   *
   * <h2>Algorithm</h2>
   *
   * <ol>
   *   <li><strong>Single-list-read optimisation</strong> — call {@link
   *       TrieNodeChangeIndex#readRangeList} once for {@code (naturalKey, targetBlock's rangeId)}.
   *       This single bloom + marker + list read provides all information needed to determine both
   *       whether the node changed after T (fast-path check) and the latest change ≤ T (for history
   *       reconstruction), avoiding the triple-read pattern of the old two-call sequence.
   *   <li><strong>Fast path</strong> — if the range list shows no change after targetBlock AND no
   *       higher ranges have changes (single-range chain or confirmed by {@link
   *       TrieNodeChangeIndex#modifiedAfter}), the live trie node is correct. Return it directly.
   *   <li><strong>History path</strong> — otherwise, find {@code b*} (latest change ≤ T) using the
   *       preloaded list and delegate to {@link TrieNodeHistoryReader#nodeAt(Bytes, long,
   *       RangeRelativeOffsetList, long)} with the preloaded list to avoid a second list read.
   *   <li><strong>Hash verification</strong> — compute {@code keccak256(node)} and compare to
   *       {@code expectedHash}. A mismatch throws {@link IllegalStateException} (fail-closed).
   * </ol>
   *
   * <p>For chains where all blocks are in range 0 (< 1,000,000 blocks), a single {@code
   * readRangeList} call handles ALL three logical checks for each node — reducing 9 RocksDB reads
   * to 3.
   *
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey}); must not be
   *     {@code null}
   * @param expectedHash the keccak256 hash the caller expects for this node (from the trie
   *     framework); must not be {@code null}
   * @return the node bytes if found, or empty if the node was absent at {@code targetBlock}
   * @throws IllegalStateException if the node is found but its keccak256 does not match {@code
   *     expectedHash} (fail-closed on data inconsistency)
   */
  private Optional<Bytes> resolveNodeAt(final Bytes naturalKey, final Bytes32 expectedHash) {
    final long rangeId = targetBlock / index.rangeSize;
    final long headRangeId = headBlock / index.rangeSize;
    final int withinRangeT = (int) (targetBlock - rangeId * index.rangeSize);

    // Step 1: single bloom + marker + list read for targetBlock's range.
    final Optional<RangeRelativeOffsetList> listOpt = index.readRangeList(naturalKey, rangeId);

    if (listOpt.isEmpty()) {
      // No changes at all for this key in targetBlock's range.
      // Fast path if also no changes in higher ranges (i.e. live trie is the T-version).
      if (rangeId == headRangeId) {
        // Single-range chain (or T and HEAD share the same range) — no changes anywhere.
        return liveStorage
            .get(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe())
            .map(Bytes::wrap);
      }
      // Multi-range: check whether higher ranges have any changes after T.
      if (!index.modifiedAfter(naturalKey, targetBlock, headBlock)) {
        return liveStorage
            .get(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe())
            .map(Bytes::wrap);
      }
      // Changed after T but no recorded change ≤ T in any range → node absent at T.
      // Need to check earlier ranges for a bStar — fall back to the standard path.
      return resolveFromHistory(naturalKey, expectedHash);
    }

    // We have the list for targetBlock's range. Use it to answer both questions in memory:
    //   (a) Was the node modified after T (in this range)?
    //   (b) What is the latest change ≤ T (bStar)?

    final RangeRelativeOffsetList list = listOpt.get();

    // (b) Latest change ≤ T within this range.
    final OptionalInt bStarOffsetOpt = list.latestLeq(withinRangeT);

    // (a) Modified after T: any entry in this range > withinRangeT?
    //     The last entry is the largest; if it is > withinRangeT then yes.
    final OptionalInt lastOpt = list.last();
    final boolean modifiedAfterTInThisRange =
        lastOpt.isPresent() && lastOpt.getAsInt() > withinRangeT;

    // Determine whether the node was modified after T across ALL ranges up to headBlock.
    final boolean modifiedAfterT;
    if (rangeId == headRangeId) {
      // T and HEAD are in the same range — the list we already have covers everything.
      modifiedAfterT = modifiedAfterTInThisRange;
    } else {
      // T and HEAD are in different ranges. If this range has a change after T, that's enough.
      // Otherwise we need to check higher ranges (fall back to modifiedAfter for ranges > rangeId).
      modifiedAfterT =
          modifiedAfterTInThisRange || index.modifiedAfter(naturalKey, targetBlock, headBlock);
    }

    if (!modifiedAfterT) {
      // Fast path: live trie is the T-version.
      // Note: fast path does NOT verify the hash (same reasoning as original implementation).
      return liveStorage
          .get(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe())
          .map(Bytes::wrap);
    }

    // History path — find bStar and reconstruct.
    final long bStar;
    if (bStarOffsetOpt.isPresent()) {
      // bStar is within this range — compute absolute block number.
      bStar = rangeId * index.rangeSize + bStarOffsetOpt.getAsInt();
    } else {
      // No change ≤ T in this range — need to check earlier ranges.
      final Optional<Long> bStarOpt = index.latestChangeBlock(naturalKey, targetBlock);
      if (bStarOpt.isEmpty()) {
        // Never changed before T (but changed after T) → node absent at T.
        return Optional.empty();
      }
      bStar = bStarOpt.get();
    }

    // Reconstruct using the preloaded list to avoid a third index read (for single-range case
    // where bStar is in targetBlock's range).
    final Optional<Bytes> nodeOpt = historyReader.nodeAt(naturalKey, bStar, list, rangeId);
    if (nodeOpt.isEmpty()) {
      return Optional.empty();
    }

    // Step 3: hash verification — fail-closed on mismatch.
    final Bytes node = nodeOpt.get();
    final Bytes32 actualHash = keccak256(node);
    if (!actualHash.equals(expectedHash)) {
      throw new IllegalStateException(
          "trie node hash mismatch for naturalKey="
              + naturalKey
              + " at targetBlock="
              + targetBlock
              + ": expected="
              + expectedHash
              + ", actual="
              + actualHash
              + " — index/store inconsistency detected");
    }

    return nodeOpt;
  }

  /**
   * Fallback for the case where {@code readRangeList} returned empty but {@code modifiedAfter}
   * indicated changes exist after T. Uses the existing two-step approach to resolve the node.
   *
   * <p>This path handles chains where targetBlock is in a range with no recorded changes for the
   * key, but the key had changes in earlier ranges and/or higher ranges.
   */
  private Optional<Bytes> resolveFromHistory(final Bytes naturalKey, final Bytes32 expectedHash) {
    final Optional<Bytes> nodeOpt = historyReader.nodeAt(naturalKey, targetBlock);
    if (nodeOpt.isEmpty()) {
      return Optional.empty();
    }
    final Bytes node = nodeOpt.get();
    final Bytes32 actualHash = keccak256(node);
    if (!actualHash.equals(expectedHash)) {
      throw new IllegalStateException(
          "trie node hash mismatch for naturalKey="
              + naturalKey
              + " at targetBlock="
              + targetBlock
              + ": expected="
              + expectedHash
              + ", actual="
              + actualHash
              + " — index/store inconsistency detected");
    }
    return nodeOpt;
  }
}
