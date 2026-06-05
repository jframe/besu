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
 *   <li>Unchanged node (fast-path): no change after T → live trie read, no history accessed.
 *   <li>Changed node: changed after T → history reader reconstructs version at b*, hash verified.
 *   <li>Hash mismatch: history returns valid node but expected hash doesn't match → throws.
 * </ol>
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
   * Write a node directly to the live TRIE_BRANCH_STORAGE (simulates the live trie at HEAD).
   *
   * <p>Key = naturalKey (account trie: location; storage trie: accountHash ‖ location).
   */
  private void putLiveNode(final Bytes naturalKey, final Bytes node) {
    var tx = kv.startTransaction();
    tx.put(
        KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
        naturalKey.toArrayUnsafe(),
        node.toArrayUnsafe());
    tx.commit();
  }

  // ---------------------------------------------------------------------------
  // Test 1: Fast-path — unchanged node after T returns live trie value
  // ---------------------------------------------------------------------------

  /**
   * Node changed only at block 50 (before T=100). HEAD=200. No change in (100, 200] → fast path:
   * loader returns the live trie node directly without reading history.
   */
  @Test
  void unchangedNodeReturnedFromLiveTrie() {
    final long targetBlock = 100;
    final long headBlock = 200;

    final Bytes accountNaturalKey = ArchiveNodeKey.account(ACCOUNT_LOCATION);
    final Bytes liveNode = branchWith(3, 50);

    // Index records block 50 only (before T=100 → no change in (100, 200]).
    appendIndex(accountNaturalKey, 50);
    // Live trie holds the current version.
    putLiveNode(accountNaturalKey, liveNode);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock, headBlock);

    final NodeLoader accountLoader = loader.accountNodeLoader();
    final Bytes32 expectedHash = keccak(liveNode);
    final Optional<Bytes> result = accountLoader.getNode(ACCOUNT_LOCATION, expectedHash);

    assertThat(result).hasValue(liveNode);
  }

  // ---------------------------------------------------------------------------
  // Test 2: Changed node after T — history reader reconstructs version at b*
  // ---------------------------------------------------------------------------

  /**
   * Node changed at block 50 (before T=60) and again at block 80 (after T=60). HEAD=200. Loader
   * detects a change in (60, 200] → history path: reconstructs version at b*=50 (latest change ≤
   * T=60). Keccak of result matches expected hash.
   */
  @Test
  void changedNodeReconstructedFromHistory() {
    final long targetBlock = 60;
    final long headBlock = 200;

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
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock, headBlock);

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
   * Index and history return a valid node at T=60, but the expected hash supplied by the caller
   * doesn't match the reconstructed node. The loader must throw fail-closed to prevent serving
   * silently incorrect proof data.
   */
  @Test
  void hashMismatchThrowsIllegalStateException() {
    final long targetBlock = 60;
    final long headBlock = 200;

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
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock, headBlock);

    final NodeLoader accountLoader = loader.accountNodeLoader();
    // Wrong expected hash: hash of v80, but the historical version at T=60 is v50.
    final Bytes32 wrongHash = keccak(v80);

    assertThatThrownBy(() -> accountLoader.getNode(ACCOUNT_LOCATION, wrongHash))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("trie node hash mismatch");
  }

  // ---------------------------------------------------------------------------
  // Test 4: Absent node — no history entry before T returns empty
  // ---------------------------------------------------------------------------

  /**
   * No history entry exists at or before T. The loader returns empty (the node didn't exist at that
   * block). The test uses the changed-after-T path (index has a change at 100, after T=50).
   */
  @Test
  void noHistoryBeforeTargetReturnsEmpty() {
    final long targetBlock = 50;
    final long headBlock = 200;

    final Bytes accountNaturalKey = ArchiveNodeKey.account(ACCOUNT_LOCATION);
    final Bytes v100 = branchWith(3, 100);

    // Node only recorded at block 100 (after T=50) → history path taken but latestChangeBlock
    // returns empty for T=50.
    putEntry(accountNaturalKey, 100, TrieNodeDiffCodec.encodeFull(v100));
    appendIndex(accountNaturalKey, 100);

    putLiveNode(accountNaturalKey, v100);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock, headBlock);

    final NodeLoader accountLoader = loader.accountNodeLoader();
    // Any hash — the loader must return empty before hash verification.
    final Bytes32 hash = keccak(v100);
    final Optional<Bytes> result = accountLoader.getNode(ACCOUNT_LOCATION, hash);

    assertThat(result).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Test 5: Storage node loader — changed after T, reconstructed from history
  // ---------------------------------------------------------------------------

  /**
   * Storage trie node changed at block 50 and again at block 80. T=60, HEAD=200. The storage node
   * loader reconstructs version at b*=50 and verifies the keccak.
   */
  @Test
  void storageNodeLoaderReconstructsChangedNode() {
    final long targetBlock = 60;
    final long headBlock = 200;

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
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock, headBlock);

    final NodeLoader storageLoader = loader.storageNodeLoader(ACCOUNT_HASH);
    final Bytes32 expectedHash = keccak(v50);
    final Optional<Bytes> result = storageLoader.getNode(STORAGE_LOCATION, expectedHash);

    assertThat(result).isPresent();
    assertThat(keccak(result.get())).isEqualTo(expectedHash);
    assertThat(result.get()).isEqualTo(v50);
  }

  // ---------------------------------------------------------------------------
  // Test 6: Storage node loader — unchanged after T (fast path)
  // ---------------------------------------------------------------------------

  /**
   * Storage trie node unchanged after T: only changed at block 50. T=100, HEAD=200. The storage
   * node loader takes the fast path and returns the live trie node.
   */
  @Test
  void storageNodeLoaderFastPathForUnchangedNode() {
    final long targetBlock = 100;
    final long headBlock = 200;

    final Bytes storageNaturalKey = ArchiveNodeKey.storage(ACCOUNT_HASH, STORAGE_LOCATION);
    final Bytes liveNode = branchWith(4, 50);

    appendIndex(storageNaturalKey, 50);
    putLiveNode(storageNaturalKey, liveNode);

    final ArchiveProofNodeLoader loader =
        new ArchiveProofNodeLoader(index, historyReader, kv, targetBlock, headBlock);

    final NodeLoader storageLoader = loader.storageNodeLoader(ACCOUNT_HASH);
    final Bytes32 expectedHash = keccak(liveNode);
    final Optional<Bytes> result = storageLoader.getNode(STORAGE_LOCATION, expectedHash);

    assertThat(result).hasValue(liveNode);
  }
}
