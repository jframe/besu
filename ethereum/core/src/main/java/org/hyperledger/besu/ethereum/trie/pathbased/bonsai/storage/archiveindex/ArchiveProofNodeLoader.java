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
 *   <li><strong>Hash-first fast path</strong> — read {@code TRIE_BRANCH_STORAGE[naturalKey]} once.
 *       The stored value is {@code hash[32] ‖ nodeBytes}. If the stored 32-byte hash equals {@code
 *       expectedHash}, the live trie node IS the historical node at {@code targetBlock} — return it
 *       directly (1 read total). This is the common case for nodes that haven't changed since T.
 *   <li><strong>Index path</strong> — hash mismatch or absent. Read the range list for {@code
 *       targetBlock}'s range via {@link TrieNodeChangeIndex#readRangeList}. Use {@link
 *       RangeRelativeOffsetList#latestLeq} to find {@code b*} (latest change ≤ T) in-memory. If
 *       found, delegate to {@link TrieNodeHistoryReader#nodeAt(Bytes, long,
 *       RangeRelativeOffsetList, long)} with the preloaded list.
 *   <li><strong>History fallback</strong> — if the range list is absent or contains no change ≤ T,
 *       delegate to {@link TrieNodeHistoryReader#nodeAt(Bytes, long)} which walks earlier ranges.
 *   <li><strong>Hash verification</strong> — after index/history reconstruction, compute {@code
 *       keccak256(node)} and compare to {@code expectedHash}. A mismatch throws {@link
 *       IllegalStateException} (fail-closed — never serve silently incorrect proof data).
 * </ol>
 *
 * <h2>Fail-closed invariant</h2>
 *
 * A hash mismatch after history reconstruction is always thrown as {@link IllegalStateException}.
 * This is intentional: a mismatch means the index, history store, or live trie are inconsistent
 * (data corruption). Serving a node that does not match the expected hash would silently produce an
 * incorrect proof, which is worse than failing loudly.
 */
public final class ArchiveProofNodeLoader {

  private final TrieNodeChangeIndex index;
  private final TrieNodeHistoryReader historyReader;
  private final SegmentedKeyValueStorage liveStorage;
  private final long targetBlock;

  /**
   * Constructs a new loader for historical proof resolution.
   *
   * @param index the trie-node change index (Design 5, Stage 2); must not be {@code null}
   * @param historyReader the history reader for reconstructing past node versions (Stage 3); must
   *     not be {@code null}
   * @param liveStorage the live segmented KV storage containing {@code TRIE_BRANCH_STORAGE} (for
   *     the hash-first fast-path live-trie read); must not be {@code null}
   * @param targetBlock T — the historical block being proved (inclusive)
   * @throws NullPointerException if any reference argument is {@code null}
   */
  public ArchiveProofNodeLoader(
      final TrieNodeChangeIndex index,
      final TrieNodeHistoryReader historyReader,
      final SegmentedKeyValueStorage liveStorage,
      final long targetBlock) {
    this.index = Objects.requireNonNull(index, "index must not be null");
    this.historyReader = Objects.requireNonNull(historyReader, "historyReader must not be null");
    this.liveStorage = Objects.requireNonNull(liveStorage, "liveStorage must not be null");
    this.targetBlock = targetBlock;
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
   *   <li><strong>Hash-first fast path</strong> — read {@code TRIE_BRANCH_STORAGE[naturalKey]}. The
   *       value is stored as {@code hash[32] ‖ nodeBytes}. Compare the first 32 bytes to {@code
   *       expectedHash}. Match → return node bytes directly (1 read).
   *   <li><strong>Index path</strong> — call {@link TrieNodeChangeIndex#readRangeList} once for
   *       {@code (naturalKey, targetBlock's rangeId)}. Use {@link
   *       RangeRelativeOffsetList#latestLeq} to find {@code b*} in-memory. If found, reconstruct
   *       from {@link TrieNodeHistoryReader#nodeAt(Bytes, long, RangeRelativeOffsetList, long)}.
   *   <li><strong>History fallback</strong> — if the list is empty or has no change ≤ T, delegate
   *       to {@link TrieNodeHistoryReader#nodeAt(Bytes, long)} which walks earlier ranges.
   *   <li><strong>Hash verification</strong> — verify reconstructed node; throw on mismatch.
   * </ol>
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
    // Step 1: Hash-first fast path — read the live node once and compare its hash.
    // TRIE_BRANCH_STORAGE holds bare node bytes (legacy Bonsai format), so recompute
    // keccak256(node) to test whether the live node is still the T-version. Most nodes are
    // unchanged between T and HEAD, so this resolves the common case in a single read.
    final byte[] rawLive =
        liveStorage
            .get(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe())
            .orElse(null);
    if (rawLive != null) {
      final Bytes liveNode = Bytes.wrap(rawLive);
      if (keccak256(liveNode).equals(expectedHash)) {
        // Live node's hash matches expectedHash → the live IS the T-version. Return directly.
        return Optional.of(liveNode);
      }
    }

    // Step 2: Hash mismatch or absent → consult the index.
    final long rangeId = targetBlock / index.rangeSize;
    final int withinRangeT = (int) (targetBlock - rangeId * index.rangeSize);

    final Optional<RangeRelativeOffsetList> listOpt = index.readRangeList(naturalKey, rangeId);

    if (listOpt.isPresent()) {
      final RangeRelativeOffsetList list = listOpt.get();
      final OptionalInt bStarOffsetOpt = list.latestLeq(withinRangeT);
      if (bStarOffsetOpt.isPresent()) {
        final long bStar = rangeId * index.rangeSize + bStarOffsetOpt.getAsInt();
        final Optional<Bytes> nodeOpt = historyReader.nodeAt(naturalKey, bStar, list, rangeId);
        return verifyAndReturn(nodeOpt, naturalKey, expectedHash);
      }
    }

    // No change ≤ T in T's range (or no list at all) → walk earlier ranges via historyReader.
    return resolveFromHistory(naturalKey, expectedHash);
  }

  /**
   * Fallback for when the range list for T's range is absent or has no change ≤ T. Delegates to
   * {@link TrieNodeHistoryReader#nodeAt(Bytes, long)} which walks earlier ranges.
   */
  private Optional<Bytes> resolveFromHistory(final Bytes naturalKey, final Bytes32 expectedHash) {
    final Optional<Bytes> nodeOpt = historyReader.nodeAt(naturalKey, targetBlock);
    return verifyAndReturn(nodeOpt, naturalKey, expectedHash);
  }

  private Optional<Bytes> verifyAndReturn(
      final Optional<Bytes> nodeOpt, final Bytes naturalKey, final Bytes32 expectedHash) {
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
