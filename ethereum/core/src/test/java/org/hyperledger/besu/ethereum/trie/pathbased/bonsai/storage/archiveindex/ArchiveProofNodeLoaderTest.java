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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ArchiveProofNodeLoader}.
 *
 * <p>Three main scenarios:
 *
 * <ol>
 *   <li>Hash-first fast path: stored hash matches expectedHash → live trie read, no index accessed.
 *   <li>Changed node: stored hash differs → index path reconstructs version at b*, hash verified.
 *   <li>Hash mismatch: history returns valid node but expected hash doesn't match → throws.
 * </ol>
 *
 * <p>Live nodes are stored in the new {@code hash[32] ‖ nodeBytes} format that {@link
 * BonsaiTrieNodeStrategy} writes. The {@link #putLiveNode} helper encodes this format.
 *
 * <p>Note on types: {@link NodeLoader#getNode} takes a {@link Bytes32} for the hash parameter.
 * Tests use {@code keccak256(node)} from {@link org.hyperledger.besu.crypto.Hash} which returns
 * {@link Bytes32} directly — the same idiom used by {@link TrieNodeChangeIndex}.
 */
class ArchiveProofNodeLoaderTest {

  // Account trie location (compact nibble path)
  private static final Bytes ACCOUNT_LOCATION = Bytes.fromHexString("0xdeadbeef");

  // Account hash (32 bytes) for storage trie tests
  private static final Bytes32 ACCOUNT_HASH =
      Bytes32.fromHexString("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

  // Storage trie location
  private static final Bytes STORAGE_LOCATION = Bytes.fromHexString("0xcafebabe");

  private SegmentedInMemoryKeyValueStorage kv;
  private TrieNodeHistoryStore store;
  private TrieNodeChangeIndex index;
  private TrieNodeHistoryReader historyReader;

  @BeforeEach
  void setUp() {
    kv = new SegmentedInMemoryKeyValueStorage();
    store = new TrieNodeHistoryStore(kv);
    index = new TrieNodeChangeIndex(kv, 1_000_000);
    historyReader = new TrieNodeHistoryReader(store, index);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Build a branch-node RLP with a single occupied child slot to produce distinct nodes. */
  private static Bytes branchWith(final int slotIndex, final int markerByte) {
    return RLP.encode(
        out -> {
          out.startList();
          for (int i = 0; i < 16; i++) {
            if (i == slotIndex) {
              out.writeBytes(Bytes32.leftPad(Bytes.of(markerByte)));
            } else {
              out.writeNull();
            }
          }
          out.writeNull(); // branch terminal value: empty
          out.endList();
        });
  }

  /**
   * Compute the keccak256 of {@code node} as a {@link Bytes32} — the type that {@link
   * NodeLoader#getNode} expects for its {@code hash} argument.
   */
  private static Bytes32 keccak(final Bytes node) {
    return keccak256(node);
  }

  /** Write a single (key, block, entry) pair to the history store and commit. */
  private void putEntry(final Bytes naturalKey, final long block, final Bytes entry) {
    var tx = kv.startTransaction();
    store.put(tx, naturalKey, block, entry);
    tx.commit();
  }

  /** Append a single (key, block) change record to the index and commit. */
  private void appendIndex(final Bytes naturalKey, final long block) {
    var tx = kv.startTransaction();
    index.append(tx, naturalKey, block);
    tx.commit();
  }

  /**
   * Write a node directly to the live TRIE_BRANCH_STORAGE in the {@code hash[32] ‖ nodeBytes}
   * format used by {@link BonsaiTrieNodeStrategy}.
   *
   * <p>Key = naturalKey (account trie: location; storage trie: accountHash ‖ location).
   */
  private void putLiveNode(final Bytes naturalKey, final Bytes node) {
    final Bytes32 hash = keccak(node);
    final byte[] value = Bytes.concatenate(hash, node).toArrayUnsafe();
    var tx = kv.startTransaction();
    tx.put(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe(), value);
    tx.commit();
  }

  // ---------------------------------------------------------------------------
  // Test 1: Hash-first fast path — hash matches live node, returned directly
  // ---------------------------------------------------------------------------

  /**
   * Node changed only at block 50 (before T=100). The live node's stored hash matches
   * expectedHash → hash-first fast path: loader returns the live trie node without reading the
   * index at all.
   */
  @Test
  void unchangedNodeReturnedFromLiveTrie() {
    final long targetBlock = 100;

    final Bytes accountNaturalKey = ArchiveNodeKey.account(ACCOUNT_LOCATION);
    final Bytes liveNode = branchWith(3, 50);

    // Index records block 50 only (not consulted — hash-first takes the fast path).
    appendIndex(accountNaturalKey, 50);
    // Live trie holds the current version in hash-prefixed format.
    putLiveNode(accountNaturalKey, liveNode);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock);

    final NodeLoader accountLoader = loader.accountNodeLoader();
    final Bytes32 expectedHash = keccak(liveNode);
    final Optional<Bytes> result = accountLoader.getNode(ACCOUNT_LOCATION, expectedHash);

    assertThat(result).hasValue(liveNode);
  }

  // ---------------------------------------------------------------------------
  // Test 2: Changed node after T — history reader reconstructs version at b*
  // ---------------------------------------------------------------------------

  /**
   * Node changed at block 50 (before T=60) and again at block 80 (after T=60). The live node is
   * v80; its stored hash != keccak(v50) → hash-first fails → index path reconstructs v50. Keccak
   * of result matches expected hash.
   */
  @Test
  void changedNodeReconstructedFromHistory() {
    final long targetBlock = 60;

    final Bytes accountNaturalKey = ArchiveNodeKey.account(ACCOUNT_LOCATION);

    final Bytes v50 = branchWith(3, 50); // FULL checkpoint at block 50
    final Bytes v80 = branchWith(5, 80); // live version at block 80 (after T)

    // History: FULL@50, DIFF@80
    putEntry(accountNaturalKey, 50, TrieNodeDiffCodec.encodeFull(v50));
    appendIndex(accountNaturalKey, 50);
    putEntry(accountNaturalKey, 80, TrieNodeDiffCodec.encodeDiff(v50, v80));
    appendIndex(accountNaturalKey, 80);

    // Live trie holds v80 (current head version).
    putLiveNode(accountNaturalKey, v80);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock);

    // At T=60, the correct version is v50 (latest change ≤ 60 was at block 50).
    final NodeLoader accountLoader = loader.accountNodeLoader();
    final Bytes32 expectedHash = keccak(v50);
    final Optional<Bytes> result = accountLoader.getNode(ACCOUNT_LOCATION, expectedHash);

    assertThat(result).isPresent();
    assertThat(keccak(result.get())).isEqualTo(expectedHash);
    assertThat(result.get()).isEqualTo(v50);
  }

  // ---------------------------------------------------------------------------
  // Test 3: Hash mismatch — throws IllegalStateException (fail-closed)
  // ---------------------------------------------------------------------------

  /**
   * The live node is v80 (hash = keccak(v80)). At T=60, the historical version is v50 (hash =
   * keccak(v50)). The caller supplies a hash that matches neither live nor historical — the loader
   * must throw fail-closed after reconstruction.
   */
  @Test
  void hashMismatchThrowsIllegalStateException() {
    final long targetBlock = 60;

    final Bytes accountNaturalKey = ArchiveNodeKey.account(ACCOUNT_LOCATION);

    final Bytes v50 = branchWith(3, 50);
    final Bytes v80 = branchWith(5, 80);

    // History: FULL@50, DIFF@80 — node changed after T=60.
    putEntry(accountNaturalKey, 50, TrieNodeDiffCodec.encodeFull(v50));
    appendIndex(accountNaturalKey, 50);
    putEntry(accountNaturalKey, 80, TrieNodeDiffCodec.encodeDiff(v50, v80));
    appendIndex(accountNaturalKey, 80);

    putLiveNode(accountNaturalKey, v80);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock);

    final NodeLoader accountLoader = loader.accountNodeLoader();
    // wrongHash matches neither v50 nor v80 — hash-first fails, history returns v50, keccak(v50) !=
    // wrongHash → throw.
    final Bytes32 wrongHash = keccak(branchWith(7, 99));

    assertThatThrownBy(() -> accountLoader.getNode(ACCOUNT_LOCATION, wrongHash))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("trie node hash mismatch");
  }

  // ---------------------------------------------------------------------------
  // Test 4: Absent node — no history entry before T returns empty
  // ---------------------------------------------------------------------------

  /**
   * Node first recorded at block 100 (after T=50). The live node is v100 but its hash does not
   * match the {@code expectedHash} used in the query (which represents a plausible node at T=50
   * that does not correspond to v100). The loader finds no change ≤ T=50 and returns empty.
   */
  @Test
  void noHistoryBeforeTargetReturnsEmpty() {
    final long targetBlock = 50;

    final Bytes accountNaturalKey = ArchiveNodeKey.account(ACCOUNT_LOCATION);
    final Bytes v100 = branchWith(3, 100);

    // Node only recorded at block 100 (after T=50).
    putEntry(accountNaturalKey, 100, TrieNodeDiffCodec.encodeFull(v100));
    appendIndex(accountNaturalKey, 100);
    putLiveNode(accountNaturalKey, v100);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock);

    final NodeLoader accountLoader = loader.accountNodeLoader();
    // Use a hash that doesn't match the live node (keccak(v100)) so hash-first fails and we
    // consult the index. The index confirms no change ≤ T=50 → return empty.
    final Bytes32 absentNodeHash = keccak(branchWith(0, 1));
    final Optional<Bytes> result = accountLoader.getNode(ACCOUNT_LOCATION, absentNodeHash);

    assertThat(result).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Test 5: Storage node loader — changed after T, reconstructed from history
  // ---------------------------------------------------------------------------

  /**
   * Storage trie node changed at block 50 and again at block 80. T=60. The storage node loader
   * reconstructs version at b*=50 and verifies the keccak.
   */
  @Test
  void storageNodeLoaderReconstructsChangedNode() {
    final long targetBlock = 60;

    final Bytes storageNaturalKey = ArchiveNodeKey.storage(ACCOUNT_HASH, STORAGE_LOCATION);

    final Bytes v50 = branchWith(1, 50);
    final Bytes v80 = branchWith(2, 80);

    putEntry(storageNaturalKey, 50, TrieNodeDiffCodec.encodeFull(v50));
    appendIndex(storageNaturalKey, 50);
    putEntry(storageNaturalKey, 80, TrieNodeDiffCodec.encodeDiff(v50, v80));
    appendIndex(storageNaturalKey, 80);

    // Live storage trie node keyed by naturalKey = accountHash ‖ location.
    putLiveNode(storageNaturalKey, v80);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock);

    final NodeLoader storageLoader = loader.storageNodeLoader(ACCOUNT_HASH);
    final Bytes32 expectedHash = keccak(v50);
    final Optional<Bytes> result = storageLoader.getNode(STORAGE_LOCATION, expectedHash);

    assertThat(result).isPresent();
    assertThat(keccak(result.get())).isEqualTo(expectedHash);
    assertThat(result.get()).isEqualTo(v50);
  }

  // ---------------------------------------------------------------------------
  // Test 6: Storage node loader — unchanged after T (hash-first fast path)
  // ---------------------------------------------------------------------------

  /**
   * Storage trie node unchanged after T: only changed at block 50. T=100. The storage node loader
   * takes the hash-first fast path (stored hash matches expectedHash) and returns the live node.
   */
  @Test
  void storageNodeLoaderFastPathForUnchangedNode() {
    final long targetBlock = 100;

    final Bytes storageNaturalKey = ArchiveNodeKey.storage(ACCOUNT_HASH, STORAGE_LOCATION);
    final Bytes liveNode = branchWith(4, 50);

    appendIndex(storageNaturalKey, 50);
    putLiveNode(storageNaturalKey, liveNode);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock);

    final NodeLoader storageLoader = loader.storageNodeLoader(ACCOUNT_HASH);
    final Bytes32 expectedHash = keccak(liveNode);
    final Optional<Bytes> result = storageLoader.getNode(STORAGE_LOCATION, expectedHash);

    assertThat(result).hasValue(liveNode);
  }

  // ---------------------------------------------------------------------------
  // Test 7: Hash-first fast path — node never indexed (no range entry at all)
  // ---------------------------------------------------------------------------

  /**
   * Node has never been indexed (no index entries). Live node's stored hash matches expectedHash →
   * hash-first fast path returns the live node directly. T=100.
   */
  @Test
  void nodeNeverIndexedReturnsLiveNodeDirectly() {
    final long targetBlock = 100;

    final Bytes accountNaturalKey = ArchiveNodeKey.account(ACCOUNT_LOCATION);
    final Bytes liveNode = branchWith(7, 42);

    // No index entries — node was never recorded as changed.
    putLiveNode(accountNaturalKey, liveNode);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock);

    final NodeLoader accountLoader = loader.accountNodeLoader();
    final Bytes32 expectedHash = keccak(liveNode);
    final Optional<Bytes> result = accountLoader.getNode(ACCOUNT_LOCATION, expectedHash);

    assertThat(result).hasValue(liveNode);
  }

  // ---------------------------------------------------------------------------
  // Test 8: Single-range — changed exactly at T (bStar == T), FULL entry at T
  // ---------------------------------------------------------------------------

  /**
   * Node changed exactly at T and again after T. The historical version at T is the FULL entry at
   * T. T=50, change at 50 (FULL) and 80 (DIFF after T). The single-range list read finds bStar=50
   * via latestLeq(50) and the preloaded list is passed to the history reader.
   */
  @Test
  void nodeChangedExactlyAtTReturnsTVersion() {
    final long targetBlock = 50;

    final Bytes accountNaturalKey = ArchiveNodeKey.account(ACCOUNT_LOCATION);
    final Bytes vT = branchWith(2, 50); // version at T=50
    final Bytes vHead = branchWith(6, 80); // live version after T

    putEntry(accountNaturalKey, 50, TrieNodeDiffCodec.encodeFull(vT));
    appendIndex(accountNaturalKey, 50);
    putEntry(accountNaturalKey, 80, TrieNodeDiffCodec.encodeDiff(vT, vHead));
    appendIndex(accountNaturalKey, 80);

    putLiveNode(accountNaturalKey, vHead);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock);

    final NodeLoader accountLoader = loader.accountNodeLoader();
    final Bytes32 expectedHash = keccak(vT);
    final Optional<Bytes> result = accountLoader.getNode(ACCOUNT_LOCATION, expectedHash);

    assertThat(result).hasValue(vT);
  }

  // ---------------------------------------------------------------------------
  // Test 9: Single-range — node changed only before T, no change after T
  // ---------------------------------------------------------------------------

  /**
   * Node changed only at block 30 (before T=80). No change after T. Live node's hash matches
   * expectedHash → hash-first fast path returns the live node directly, without consulting the
   * index.
   */
  @Test
  void nodeChangedOnlyBeforeTReturnedFromLiveTrieViaSingleRangeList() {
    final long targetBlock = 80;

    final Bytes accountNaturalKey = ArchiveNodeKey.account(ACCOUNT_LOCATION);
    final Bytes liveNode = branchWith(1, 30);

    appendIndex(accountNaturalKey, 30);
    putLiveNode(accountNaturalKey, liveNode);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock);

    final NodeLoader accountLoader = loader.accountNodeLoader();
    final Bytes32 expectedHash = keccak(liveNode);
    final Optional<Bytes> result = accountLoader.getNode(ACCOUNT_LOCATION, expectedHash);

    assertThat(result).hasValue(liveNode);
  }
}
