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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryStore;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiArchiveTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;
  private BonsaiArchiveTrieNodeStrategy strategy;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
    strategy =
        new BonsaiArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), historyStore, new TrieNodeHistoryProgress());
  }

  /**
   * Sets the committed world block number, i.e. the number of the LAST persisted block. A write
   * issued afterwards is treated as belonging to block {@code committedBlock + 1}. To model genesis
   * (the very first block), simply do not call this at all — absence of the key is what production
   * genesis actually looks like.
   */
  private void setCommittedWorldBlockNumber(final long committedBlock) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(committedBlock).toArrayUnsafe());
    tx.commit();
  }

  /**
   * Builds a valid 2-item ("short node": {@code [path, value]}) RLP list, distinct per {@code i},
   * so {@link org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeCodec}'s
   * arity check accepts it as a diffable node. See the note at this method's call site.
   */
  private static Bytes shortNodeRlp(final int i) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(Bytes.of(0x01));
    out.writeBytes(Bytes.of(i));
    out.endList();
    return out.encoded();
  }

  private void putAccountTrieNode(final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    // NOTE: putFlatAccountTrieNode takes Bytes32, and org.hyperledger.besu.datatypes.Hash is NOT a
    // Bytes32 (it extends BytesHolder) — use crypto.Hash.keccak256, which returns Bytes32.
    strategy.putFlatAccountTrieNode(
        storage, tx, location, org.hyperledger.besu.crypto.Hash.keccak256(node), node);
    tx.commit();
  }

  @Test
  void creationAlwaysWritesFullEntryWithCounterZero() {
    setCommittedWorldBlockNumber(9L); // next write lands at block 10
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = Bytes.fromHexString("0xaa");
    putAccountTrieNode(location, node);

    final TrieNodeHistoryStore.HistoryEntry entry =
        historyStore.get(ArchiveNodeKey.account(location), 10L).orElseThrow();
    assertThat(entry.codecEntry().isFull()).isTrue();
    assertThat(entry.codecEntry().isCreation()).isTrue();
    assertThat(entry.counter()).isEqualTo(0);
  }

  @Test
  void nonRootNodeChecksInFullExactlyEveryCheckpointIntervalMutations() {
    final Bytes location = Bytes.fromHexString("0x030405"); // depth 3, non-root
    // NOTE (test-fixture fix, not a production-code change): the brief's original fixture data
    // used bare single bytes (e.g. 0x00) as "node" RLP. That is fine for the creation write (the
    // priorNode == null branch in captureTrieNodeDiff never calls ArchiveTrieNodeCodec.encodeDiff's
    // nodeArity() check), but every subsequent mid-chain write here does take that path, and
    // nodeArity() requires a genuine 2-item (short node) or 17-item (branch node) RLP list —
    // it throws RLPException on a bare byte. Real Bonsai trie nodes are always valid RLP, so this
    // is a gap in the brief's test fixture, not a bug in production code; fixed by encoding each
    // mutation as a valid 2-item short-node RLP list ([path, value]) with a varying value so each
    // write is still a distinct node.
    Bytes node = shortNodeRlp(0);
    // No setCommittedWorldBlockNumber call: models genesis, so this write lands at block 0.
    putAccountTrieNode(location, node); // block 0: creation, FULL

    for (int i = 1; i <= TrieNodeHistoryReader.CHECKPOINT_INTERVAL; i++) {
      setCommittedWorldBlockNumber(i - 1L);
      node = shortNodeRlp(i);
      putAccountTrieNode(location, node);
    }
    // The CHECKPOINT_INTERVAL-th mutation after creation (counter reaches CHECKPOINT_INTERVAL - 1,
    // then wraps) must be FULL.
    final TrieNodeHistoryStore.HistoryEntry checkpointEntry =
        historyStore
            .get(ArchiveNodeKey.account(location), (long) TrieNodeHistoryReader.CHECKPOINT_INTERVAL)
            .orElseThrow();
    assertThat(checkpointEntry.codecEntry().isFull()).isTrue();
    assertThat(checkpointEntry.counter()).isEqualTo(0);

    final TrieNodeHistoryStore.HistoryEntry midChainEntry =
        historyStore.get(ArchiveNodeKey.account(location), 1L).orElseThrow();
    assertThat(midChainEntry.codecEntry().isFull()).isFalse();
  }

  @Test
  void rootNodeIsAlwaysFullRegardlessOfMutationCount() {
    final Bytes location = Bytes.EMPTY; // root
    putAccountTrieNode(location, Bytes.fromHexString("0x01")); // genesis: block 0
    setCommittedWorldBlockNumber(0L);
    putAccountTrieNode(location, Bytes.fromHexString("0x02"));
    setCommittedWorldBlockNumber(1L);
    putAccountTrieNode(location, Bytes.fromHexString("0x03"));

    for (final long block : new long[] {0L, 1L, 2L}) {
      assertThat(
              historyStore
                  .get(ArchiveNodeKey.account(location), block)
                  .orElseThrow()
                  .codecEntry()
                  .isFull())
          .as("root entry at block %s must be FULL", block)
          .isTrue();
    }
  }

  @Test
  void deletionWritesATombstoneThatMakesNodeAtReturnEmptyAfterward() {
    final Bytes location = Bytes.fromHexString("0x0708");
    // Genesis: no committed block number written, so the next write lands at block 0.
    putAccountTrieNode(location, Bytes.fromHexString("0xaa")); // block 0: creation

    setCommittedWorldBlockNumber(0L); // next write lands at block 1
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeFlatAccountStateTrieNode(storage, tx, location);
    tx.commit();

    final org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryReader reader =
        new org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.TrieNodeHistoryReader(
            historyStore);
    assertThat(reader.nodeAt(ArchiveNodeKey.account(location), 1L)).isEmpty();
  }

  @Test
  void firstTrieNodeWriteAfterGenesisIsRecordedUnderBlockOneNotBlockZero() {
    // Simulates genesis (block 0) having already committed WORLD_BLOCK_NUMBER_KEY = 0, then
    // block 1 processing its trie-node writes. This is the exact scenario the design5 POC got
    // wrong (see the design spec's "Block-number determination" section) — this test exists so
    // that if the shared BonsaiArchiveFlatDbStrategy pattern this class reuses has the same latent
    // issue, it is caught here rather than silently reproduced.
    setCommittedWorldBlockNumber(0L); // genesis already committed
    final Bytes location = Bytes.fromHexString("0x0a0b");
    putAccountTrieNode(location, Bytes.fromHexString("0xcc")); // block 1's write

    assertThat(historyStore.get(ArchiveNodeKey.account(location), 1L)).isPresent();
    assertThat(historyStore.get(ArchiveNodeKey.account(location), 0L)).isEmpty();
  }

  @Test
  void storageTrieNodeDeletionHasNoTombstoneHookDocumentedLimitation() {
    // TrieNodeStrategy has no removeFlatStorageTrieNode (Task 7) — storage-trie nodes removed via
    // bulk clearStorage never get a tombstone in this PR. This test documents the gap rather than
    // silently omitting coverage for it: putFlatStorageTrieNode followed by never calling any
    // removal method still leaves the last-written entry queryable, which is the (documented,
    // known, and out-of-scope-to-fix-here) limitation.
    final Hash accountHash = Hash.hash(Bytes.fromHexString("0xaa"));
    final Bytes location = Bytes.fromHexString("0x0c");
    // Genesis: no committed block number written, so the next write lands at block 0.
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatStorageTrieNode(
        storage,
        tx,
        accountHash,
        location,
        org.hyperledger.besu.crypto.Hash.keccak256(Bytes.fromHexString("0xdd")),
        Bytes.fromHexString("0xdd"));
    tx.commit();

    final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), location);
    // No API in this class can express "this storage slot's trie node was later self-destructed" —
    // the entry remains queryable forever, which is the documented limitation.
    assertThat(historyStore.get(naturalKey, 0L)).isPresent();
  }
}
