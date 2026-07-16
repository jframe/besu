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

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Adapts the Design-5 trie-node history (V2 append-only store) to the {@link NodeLoader} interface
 * used by {@link org.hyperledger.besu.ethereum.proof.WorldStateProofProvider}.
 *
 * <p>Two {@link NodeLoader} views are provided via static factory methods:
 *
 * <ul>
 *   <li>{@link #accountNodeLoader} — for the account state trie; {@code naturalKey = location}.
 *   <li>{@link #storageNodeLoader} — for a specific account's storage trie; {@code naturalKey =
 *       accountHash ‖ location}.
 * </ul>
 *
 * <h2>Algorithm — {@link #resolveNodeAt(Bytes, Bytes32)}</h2>
 *
 * <ol>
 *   <li><strong>Hash-first fast path</strong> — read {@code TRIE_BRANCH_STORAGE[naturalKey]} once.
 *       If the stored node's keccak256 equals {@code expectedHash}, the live trie node IS the
 *       historical node at {@code targetBlock} — return it directly (1 read total). This is the
 *       common case for nodes that haven't changed since T.
 *   <li><strong>History path</strong> — hash mismatch or absent. Delegate to {@link
 *       TrieNodeHistoryReaderV2#nodeAt} which performs a single nearest-before lookup and, if
 *       needed, a bounded backward walk to reconstruct the node from DIFF entries.
 *   <li><strong>Hash verification</strong> — after history reconstruction, compute {@code
 *       keccak256(node)} and compare to {@code expectedHash}. A mismatch throws {@link
 *       IllegalStateException} (fail-closed — never serve silently incorrect proof data).
 * </ol>
 *
 * <h2>Fail-closed invariant</h2>
 *
 * A hash mismatch after history reconstruction is always thrown as {@link IllegalStateException}.
 * This is intentional: a mismatch means the history store or live trie is inconsistent (data
 * corruption). Serving a node that does not match the expected hash would silently produce an
 * incorrect proof, which is worse than failing loudly.
 */
public final class ArchiveProofNodeLoader {

  private final SegmentedKeyValueStorage liveStorage;
  private final TrieNodeHistoryReaderV2 historyReader;
  private final long targetBlock;
  private final byte domain;

  private ArchiveProofNodeLoader(
      final SegmentedKeyValueStorage liveStorage,
      final TrieNodeHistoryReaderV2 historyReader,
      final long targetBlock,
      final byte domain) {
    this.liveStorage = liveStorage;
    this.historyReader = historyReader;
    this.targetBlock = targetBlock;
    this.domain = domain;
  }

  // ---------------------------------------------------------------------------
  // Public API — NodeLoader factories
  // ---------------------------------------------------------------------------

  /**
   * Returns a {@link NodeLoader} for the account state trie.
   *
   * <p>The natural key for each node is {@code location} (the compact path).
   *
   * @param liveStorage the live segmented KV storage containing {@code TRIE_BRANCH_STORAGE}; must
   *     not be {@code null}
   * @param historyReader the V2 history reader backed by {@code TRIE_NODE_HISTORY_ARCHIVE_V2}; must
   *     not be {@code null}
   * @param targetBlock T — the historical block being proved (inclusive)
   * @return a NodeLoader for account-trie nodes at {@code targetBlock}
   */
  public static NodeLoader accountNodeLoader(
      final SegmentedKeyValueStorage liveStorage,
      final TrieNodeHistoryReaderV2 historyReader,
      final long targetBlock) {
    final ArchiveProofNodeLoader delegate =
        new ArchiveProofNodeLoader(
            liveStorage, historyReader, targetBlock, HistoryKey.DOMAIN_ACCOUNT);
    return (location, hash) -> delegate.resolveNodeAt(HistoryKey.accountNaturalKey(location), hash);
  }

  /**
   * Returns a {@link NodeLoader} for the storage trie of the given account.
   *
   * <p>The natural key for each node is {@code accountHash ‖ location}.
   *
   * @param liveStorage the live segmented KV storage containing {@code TRIE_BRANCH_STORAGE}; must
   *     not be {@code null}
   * @param historyReader the V2 history reader backed by {@code TRIE_NODE_HISTORY_ARCHIVE_V2}; must
   *     not be {@code null}
   * @param targetBlock T — the historical block being proved (inclusive)
   * @param accountHash the 32-byte hash of the account that owns the storage trie; must be exactly
   *     32 bytes
   * @return a NodeLoader for storage-trie nodes at {@code targetBlock}
   */
  public static NodeLoader storageNodeLoader(
      final SegmentedKeyValueStorage liveStorage,
      final TrieNodeHistoryReaderV2 historyReader,
      final long targetBlock,
      final Bytes32 accountHash) {
    final ArchiveProofNodeLoader delegate =
        new ArchiveProofNodeLoader(
            liveStorage, historyReader, targetBlock, HistoryKey.DOMAIN_STORAGE);
    return (location, hash) ->
        delegate.resolveNodeAt(HistoryKey.storageNaturalKey(accountHash, location), hash);
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
   *   <li><strong>Hash-first fast path</strong> — read {@code TRIE_BRANCH_STORAGE[naturalKey]}. If
   *       keccak256(live) equals {@code expectedHash}, the live version IS the T-version.
   *   <li><strong>History path</strong> — call {@link TrieNodeHistoryReaderV2#nodeAt} which
   *       performs a nearest-before lookup and, if needed, a bounded backward DIFF-chain walk.
   *   <li><strong>Hash verification</strong> — verify reconstructed node; throw on mismatch.
   * </ol>
   *
   * @param naturalKey the account or storage natural key; must not be {@code null}
   * @param expectedHash the keccak256 hash the caller expects for this node; must not be {@code
   *     null}
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

    // Step 2: Hash mismatch or absent → consult the V2 history store.
    // One call replaces the old readRangeList + latestLeq + windowed reconstruct +
    // backwardWalkFallback chain.
    final Optional<Bytes> nodeOpt = historyReader.nodeAt(domain, naturalKey, targetBlock);
    return verifyAndReturn(nodeOpt, naturalKey, expectedHash);
  }

  private Optional<Bytes> verifyAndReturn(
      final Optional<Bytes> nodeOpt, final Bytes naturalKey, final Bytes32 expectedHash) {
    // Step 3: fail-closed keccak verify — unchanged from before.
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
