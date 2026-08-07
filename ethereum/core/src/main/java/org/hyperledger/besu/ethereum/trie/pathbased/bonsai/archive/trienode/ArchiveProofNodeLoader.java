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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Live-first, fail-closed {@link NodeLoader} for historical archive proofs.
 *
 * <p>Resolution order:
 *
 * <ol>
 *   <li>If {@code hash} is the empty-trie sentinel, return {@link MerkleTrie#EMPTY_TRIE_NODE}
 *       immediately — no storage lookup needed.
 *   <li>Query the live flat DB via {@code trieNodeStrategy} by location. If the returned node's
 *       keccak-256 matches {@code hash}, return it. This is the common path: nodes that haven't
 *       changed since {@code targetBlock} are still on disk and fast to read.
 *   <li>Fall back to {@link ArchiveHistoryReader#nodeAt} for the {@code targetBlock}. Hash-verify
 *       the archive result. Return empty if the hash doesn't match (fail-closed — never return a
 *       wrong node).
 * </ol>
 *
 * <p>Use the factory methods {@link #forAccount} and {@link #forStorage} to create instances.
 */
public final class ArchiveProofNodeLoader implements NodeLoader {

  private final BiFunction<Bytes, Bytes32, Optional<Bytes>> liveLookup;
  private final Function<Bytes, Bytes> naturalKeyFn;
  private final ArchiveHistoryReader historyReader;
  private final long targetBlock;

  private ArchiveProofNodeLoader(
      final BiFunction<Bytes, Bytes32, Optional<Bytes>> liveLookup,
      final Function<Bytes, Bytes> naturalKeyFn,
      final ArchiveHistoryReader historyReader,
      final long targetBlock) {
    this.liveLookup = Objects.requireNonNull(liveLookup);
    this.naturalKeyFn = Objects.requireNonNull(naturalKeyFn);
    this.historyReader = Objects.requireNonNull(historyReader);
    this.targetBlock = targetBlock;
  }

  /**
   * Creates a node loader for the account state trie.
   *
   * @param trieStrategy strategy used to read live trie nodes
   * @param storage the live world-state segmented storage
   * @param historyReader archive reader providing historical node versions
   * @param targetBlock proof target block number (inclusive)
   * @return a {@link NodeLoader} for account-trie nodes
   */
  public static NodeLoader forAccount(
      final TrieNodeStrategy trieStrategy,
      final SegmentedKeyValueStorage storage,
      final ArchiveHistoryReader historyReader,
      final long targetBlock) {
    return new ArchiveProofNodeLoader(
        (location, hash) -> trieStrategy.getFlatAccountTrieNode(location, hash, storage),
        ArchiveNodeKey::account,
        historyReader,
        targetBlock);
  }

  /**
   * Creates a node loader for a specific account's storage trie.
   *
   * @param trieStrategy strategy used to read live trie nodes
   * @param storage the live world-state segmented storage
   * @param accountHash the account whose storage trie we are proving
   * @param historyReader archive reader providing historical node versions
   * @param targetBlock proof target block number (inclusive)
   * @return a {@link NodeLoader} for storage-trie nodes of the given account
   */
  public static NodeLoader forStorage(
      final TrieNodeStrategy trieStrategy,
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final ArchiveHistoryReader historyReader,
      final long targetBlock) {
    final Bytes accountHashBytes = accountHash.getBytes();
    return new ArchiveProofNodeLoader(
        (location, hash) ->
            trieStrategy.getFlatStorageTrieNode(accountHash, location, hash, storage),
        location -> ArchiveNodeKey.storage(accountHashBytes, location),
        historyReader,
        targetBlock);
  }

  @Override
  public Optional<Bytes> getNode(final Bytes location, final Bytes32 hash) {
    if (hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    // Live-first: if the node hasn't changed since targetBlock it is still on disk.
    final Optional<Bytes> live = liveLookup.apply(location, hash);
    if (live.isPresent() && hashMatches(live.get(), hash)) {
      return live;
    }
    // Archive fallback: reconstruct the node as it was at targetBlock.
    return historyReader
        .nodeAt(naturalKeyFn.apply(location), targetBlock)
        .filter(node -> hashMatches(node, hash));
  }

  private static boolean hashMatches(final Bytes node, final Bytes32 expected) {
    return Hash.hash(node).getBytes().equals(expected);
  }
}
