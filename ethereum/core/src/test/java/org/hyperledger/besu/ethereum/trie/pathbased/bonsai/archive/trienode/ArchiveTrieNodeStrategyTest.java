/*
 * Copyright contributors to Besu.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveCoverageTracker coverageTracker;
  private ArchiveTrieNodeWriter capture;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    coverageTracker = new ArchiveCoverageTracker(storage);
    capture =
        new ArchiveTrieNodeWriter(
            historyStore,
            coverageTracker,
            Executors.newFixedThreadPool(2),
            DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
            DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL);
  }

  private ArchiveTrieNodeStrategy strategy(final long frontier) {
    final ArchiveTrieNodeStrategy strategy =
        new ArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), capture, storage, coverageTracker);
    strategy.armFrontier(() -> frontier);
    return strategy;
  }

  private ArchiveTrieNodeStrategy strategy() {
    return strategy(Long.MAX_VALUE);
  }

  /** Commits a capture-free genesis coverage record so tests can start at block 1. */
  private void seedCoverageAt(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    coverageTracker.record(tx, block);
    tx.commit();
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  private void put(final ArchiveTrieNodeStrategy strategy, final Bytes key, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putTrieNode(storage, tx, key, hash(node), node);
    strategy.onBeforeBlockCommit(storage, tx, nextBlock());
    tx.commit();
  }

  private long nextBlock() {
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(b -> Bytes.wrap(b).toLong() + 1L)
        .orElse(0L);
  }

  private void setStoredBlockNumber(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(block).toArrayUnsafe());
    tx.commit();
  }

  @Test
  void archivesGenesisWhenCoverageEmpty() {
    // Empty coverage and no stored block number: genesis must be archived.
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putTrieNode(storage, tx, location, hash(node), node);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 0L)).isPresent();
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void archivesStorageTrieNodeAtGenesis() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Hash accountHash = Hash.hash(Bytes.of(0xAA));
    final Bytes location = Bytes.of(0x0f);
    final Bytes node = Bytes.fromHexString("0xcafe");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putTrieNode(storage, tx, TrieNodeKey.of(accountHash, location), hash(node), node);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(
            historyStore.getLatestBefore(
                ArchiveNodeKey.storage(accountHash.getBytes(), location), 0L))
        .isPresent();
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void multipleNodesInSameTransactionBothArchivedWithOneCoverageRecord() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location1 = Bytes.of(0x01);
    final Bytes location2 = Bytes.of(0x02);
    final Bytes node1 = Bytes.fromHexString("0x1111");
    final Bytes node2 = Bytes.fromHexString("0x2222");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putTrieNode(storage, tx, location1, hash(node1), node1);
    strategy.putTrieNode(storage, tx, location2, hash(node2), node2);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location1), 0L)).isPresent();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location2), 0L)).isPresent();
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void skipsArchivingNoOpRewriteOfUnchangedNode() {
    // A node re-written with its already-committed bytes (the no-op storage write Bonsai still
    // re-commits along a touched path) must not add a redundant archive entry: getLatestBefore
    // resolves to the last real change, whose value is identical.
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x07);
    final Bytes node = Bytes.fromHexString("0xfeed");

    // Block 1: real write — archived.
    seedCoverageAt(0L);
    setStoredBlockNumber(0L);
    put(strategy, location, node);

    // Block 2: identical bytes re-written — no-op, must be skipped (no new entry).
    setStoredBlockNumber(1L);
    put(strategy, location, node);

    final var latest = historyStore.getLatestBefore(ArchiveNodeKey.account(location), 2L);
    assertThat(latest).isPresent();
    assertThat(latest.get().block())
        .as("no entry written at the no-op block; resolves to the block-1 entry")
        .isEqualTo(1L);
  }

  @Test
  void accountReadsDelegateToLiveStorage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0d);
    final Bytes node = Bytes.fromHexString("0xabcd");

    put(strategy, location, node);

    assertThat(strategy.getTrieNode(storage, location, hash(node))).contains(node);
  }

  @Test
  void storageReadsDelegateToLiveStorage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Hash accountHash = Hash.hash(Bytes.of(0xBB));
    final Bytes location = Bytes.of(0x0d);
    final Bytes node = Bytes.fromHexString("0xabcd");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putTrieNode(storage, tx, TrieNodeKey.of(accountHash, location), hash(node), node);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(strategy.getTrieNode(storage, TrieNodeKey.of(accountHash, location), hash(node)))
        .contains(node);
  }

  @Test
  void removingExistingNodeWritesDeletionTombstone() {
    // Block 0: create the node so it exists in committed storage.
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x05);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");

    put(strategy, location, node);

    // Block 1: remove the node — prior lookup finds it, tombstone must be captured.
    setStoredBlockNumber(0L);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeTrieNode(storage, tx, location);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    final var tombstone = historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L);
    assertThat(tombstone).as("deletion tombstone written at block 1").isPresent();
    assertThat(tombstone.get().codecEntry().isDeletion())
        .as("entry at block 1 must be a deletion tombstone")
        .isTrue();
    assertThat(tombstone.get().block()).isEqualTo(1L);
    assertThat(coverageTracker.hasArchiveBlock(1L)).isTrue();
  }

  @Test
  void removingExistingStorageTrieNodeWritesDeletionTombstoneUnderStorageKey() {
    // Block 0: create the node so it exists in committed storage.
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Hash accountHash = Hash.hash(Bytes.of(0xAA));
    final Bytes location = Bytes.of(0x05);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");

    put(strategy, TrieNodeKey.of(accountHash, location), node);

    // Block 1: remove the node — prior lookup finds it, tombstone must be captured.
    setStoredBlockNumber(0L);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeTrieNode(storage, tx, TrieNodeKey.of(accountHash, location));
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    final var tombstone =
        historyStore.getLatestBefore(ArchiveNodeKey.storage(accountHash.getBytes(), location), 1L);
    assertThat(tombstone).as("deletion tombstone written at block 1").isPresent();
    assertThat(tombstone.get().codecEntry().isDeletion()).isTrue();
    assertThat(tombstone.get().block()).isEqualTo(1L);
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L)).isEmpty();
  }

  @Test
  void removeDoesNotWriteToArchive() {
    seedCoverageAt(5L);
    setStoredBlockNumber(5L);
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeTrieNode(storage, tx, location);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 6L)).isEmpty();
    assertThat(coverageTracker.hasArchiveBlock(6L)).isFalse();
  }

  @Test
  void archivesNextBlockOnLiveStorage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    seedCoverageAt(0L);
    setStoredBlockNumber(0L);
    final Bytes location = Bytes.of(0x0a);
    put(strategy, location, Bytes.fromHexString("0xaaaa"));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L)).isPresent();
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(1L);
  }

  @Test
  void skipsBlockAboveFrontier() {
    final ArchiveTrieNodeStrategy strategy = strategy(0L);
    seedCoverageAt(0L);
    setStoredBlockNumber(0L);
    final Bytes location = Bytes.of(0x0b);
    put(strategy, location, Bytes.fromHexString("0xbbbb"));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L)).isEmpty();
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(0L);
  }

  @Test
  void skipsNonContiguousBlockOnLiveStorage() {
    // e.g. head import while the roller is still behind: head block != last + 1
    final ArchiveTrieNodeStrategy strategy = strategy();
    seedCoverageAt(0L);
    setStoredBlockNumber(4L);
    final Bytes location = Bytes.of(0x0c);
    put(strategy, location, Bytes.fromHexString("0xcccc"));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 5L)).isEmpty();
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(0L);
  }

  @Test
  void skipsNonGenesisBlockWhenCoverageEmpty() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    setStoredBlockNumber(0L);
    put(strategy, Bytes.of(0x0d), Bytes.fromHexString("0xdddd"));

    assertThat(coverageTracker.lastIndexedBlock()).isEmpty();
  }

  @Test
  void ignoresStorageThatIsNeitherLiveNorRoller() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    seedCoverageAt(0L);
    final SegmentedKeyValueStorage other =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    // stored block 0 makes the candidate block 1, which is "next" for the coverage seeded above
    final SegmentedKeyValueStorageTransaction seedOther = other.startTransaction();
    seedOther.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(0L).toArrayUnsafe());
    seedOther.commit();
    final SegmentedKeyValueStorageTransaction tx = other.startTransaction();
    final Bytes node = Bytes.fromHexString("0xeeee");
    strategy.putTrieNode(other, tx, Bytes.of(0x0e), hash(node), node);
    strategy.onBeforeCommit(other, tx);
    tx.commit();

    assertThat(coverageTracker.lastIndexedBlock()).hasValue(0L);
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.of(0x0e)), 1L)).isEmpty();
  }

  @Test
  void rollerStorageCapturesDetachedIntoCanonicalStorage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    seedCoverageAt(0L);
    final SegmentedKeyValueStorage roller =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    final SegmentedKeyValueStorageTransaction seed = roller.startTransaction();
    seed.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(0L).toArrayUnsafe());
    seed.commit();
    strategy.registerRollerStorage(roller);

    final SegmentedKeyValueStorageTransaction tx = roller.startTransaction();
    final Bytes node = Bytes.fromHexString("0xf0f0");
    strategy.putTrieNode(roller, tx, Bytes.of(0x0f), hash(node), node);
    strategy.onBeforeCommit(roller, tx);
    tx.rollback(); // scratch writes discarded

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.of(0x0f)), 1L))
        .isPresent();
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(1L);
    assertThat(roller.stream(TRIE_BRANCH_STORAGE_ARCHIVE).count()).isZero();

    // advance the roller's stored block so block 2 would be "next": only unregistration blocks it
    final SegmentedKeyValueStorageTransaction advance = roller.startTransaction();
    advance.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(1L).toArrayUnsafe());
    advance.commit();
    strategy.unregisterRollerStorage(roller);
    final SegmentedKeyValueStorageTransaction after = roller.startTransaction();
    strategy.putTrieNode(roller, after, Bytes.of(0x0f), hash(node), node);
    strategy.onBeforeCommit(roller, after);
    after.rollback();
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(1L);
  }

  @Test
  void registeringASecondRollerStorageThrows() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    strategy.registerRollerStorage(new SegmentedInMemoryKeyValueStorage());
    assertThatThrownBy(() -> strategy.registerRollerStorage(new SegmentedInMemoryKeyValueStorage()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void genesisCaptureDoesNotConsultFrontier() {
    final ArchiveTrieNodeStrategy unarmed =
        new ArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), capture, storage, coverageTracker);
    put(unarmed, Bytes.of(0x01), Bytes.fromHexString("0x0101"));
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(0L);
  }

  @Test
  void unarmedFrontierFailsLoudlyForLaterBlocks() {
    final ArchiveTrieNodeStrategy unarmed =
        new ArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), capture, storage, coverageTracker);
    seedCoverageAt(0L);
    setStoredBlockNumber(0L);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    final Bytes node = Bytes.fromHexString("0x0202");
    assertThatThrownBy(() -> unarmed.putTrieNode(storage, tx, Bytes.of(0x02), hash(node), node))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("frontier");
  }

  private void blockCommitWithoutPuts(final ArchiveTrieNodeStrategy strategy) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.onBeforeBlockCommit(storage, tx, nextBlock());
    tx.commit();
  }

  @Test
  void emptyBlockStillAdvancesCoverage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    seedCoverageAt(0L);
    setStoredBlockNumber(0L);
    blockCommitWithoutPuts(strategy); // block 1, no node writes
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(1L);

    setStoredBlockNumber(1L);
    put(strategy, Bytes.of(0x21), Bytes.fromHexString("0x2121")); // block 2
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(2L);
  }

  @Test
  void blockWithOnlyNoOpRewriteStillAdvancesCoverage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x22);
    final Bytes node = Bytes.fromHexString("0x2222");
    put(strategy, location, node); // genesis
    setStoredBlockNumber(0L);
    put(strategy, location, node); // block 1: identical bytes, skipped as no-op

    assertThat(coverageTracker.lastIndexedBlock()).hasValue(1L);
  }

  @Test
  void plainCommitWithoutPutsDoesNotAdvanceCoverage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    seedCoverageAt(0L);
    setStoredBlockNumber(0L);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(coverageTracker.lastIndexedBlock()).hasValue(0L);
  }

  @Test
  void emptyAllocGenesisRecordsCoverage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    blockCommitWithoutPuts(strategy);

    assertThat(coverageTracker.lastIndexedBlock()).hasValue(0L);
  }

  @Test
  void mergedRollPersistIsNotArchivedAndDoesNotLeak() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    seedCoverageAt(0L);
    setStoredBlockNumber(0L);
    final Bytes node = Bytes.fromHexString("0x3131");
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putTrieNode(storage, tx, Bytes.of(0x31), hash(node), node);
    strategy.onBeforeBlockCommit(storage, tx, 5L); // decided block 1, persisting 5
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.of(0x31)), 10L)).isEmpty();
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(0L);

    put(strategy, Bytes.of(0x32), Bytes.fromHexString("0x3232")); // normal block 1
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.of(0x32)), 1L))
        .isPresent();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.of(0x31)), 1L)).isEmpty();
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(1L);
  }

  @Test
  void rollbackShapedPersistIsNotArchived() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    seedCoverageAt(0L);
    seedCoverageAt(1L);
    seedCoverageAt(2L);
    seedCoverageAt(3L);
    setStoredBlockNumber(3L); // candidate 4
    final Bytes node = Bytes.fromHexString("0x4141");
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putTrieNode(storage, tx, Bytes.of(0x41), hash(node), node);
    strategy.onBeforeBlockCommit(storage, tx, 2L);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.of(0x41)), 10L)).isEmpty();
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(3L);
  }

  /** A scratch storage registered as the roller's, with its stored block number at {@code head}. */
  private SegmentedKeyValueStorage registerRollerAt(
      final ArchiveTrieNodeStrategy strategy, final long head) {
    final SegmentedKeyValueStorage roller =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    final SegmentedKeyValueStorageTransaction seed = roller.startTransaction();
    seed.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(head).toArrayUnsafe());
    seed.commit();
    strategy.registerRollerStorage(roller);
    return roller;
  }

  /** Leaves {@code node} at {@code key} in {@code roller}, bypassing capture (a stale node). */
  private static void plantStaleNode(
      final SegmentedKeyValueStorage roller, final Bytes key, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = roller.startTransaction();
    new BonsaiTrieNodeStrategy().putTrieNode(roller, tx, key, hash(node), node);
    tx.commit();
  }

  /** One roller block persist writing {@code node} at {@code key}; scratch writes discarded. */
  private void rollerPut(
      final ArchiveTrieNodeStrategy strategy,
      final SegmentedKeyValueStorage roller,
      final Bytes key,
      final Bytes node,
      final long block) {
    final SegmentedKeyValueStorageTransaction tx = roller.startTransaction();
    strategy.putTrieNode(roller, tx, key, hash(node), node);
    strategy.onBeforeBlockCommit(roller, tx, block);
    tx.rollback();
  }

  /** A 96-byte node varying only at bytes 10 and 50, so a DIFF between two is smaller than FULL. */
  private static Bytes bigNode(final int at10, final int at50) {
    final byte[] bytes = new byte[96];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
    bytes[10] = (byte) at10;
    bytes[50] = (byte) at50;
    return Bytes.wrap(bytes);
  }

  @Test
  void rollerArchivesWriteEqualToStaleScratchNodeWhenArchiveHasNothing() {
    // The roller's scratch state still holds a head-era node at L that the archive never held at
    // block 0. Writing the same bytes at block 1 is a creation from the archive's view, not a
    // no-op.
    final ArchiveTrieNodeStrategy strategy = strategy();
    seedCoverageAt(0L);
    final SegmentedKeyValueStorage roller = registerRollerAt(strategy, 0L);
    final Bytes location = Bytes.of(0x51);
    final Bytes node = Bytes.fromHexString("0x5151");
    plantStaleNode(roller, location, node);

    rollerPut(strategy, roller, location, node, 1L);

    final var entry = historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L);
    assertThat(entry).isPresent();
    assertThat(entry.get().block()).isEqualTo(1L);
    assertThat(new ArchiveHistoryReader(historyStore).nodeAt(ArchiveNodeKey.account(location), 1L))
        .contains(node);
  }

  @Test
  void rollerArchivesWriteEqualToStaleScratchNodeWhenArchiveHasTombstone() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Hash accountHash = Hash.hash(Bytes.of(0x52));
    final Bytes location = Bytes.of(0x05, 0x02);
    final Bytes key = TrieNodeKey.of(accountHash, location);
    final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), location);
    final Bytes node = Bytes.fromHexString("0x5252");

    // block 0 creates the node, block 1 deletes it (live captures): tombstone at block 1
    put(strategy, key, node);
    setStoredBlockNumber(0L);
    final SegmentedKeyValueStorageTransaction removal = storage.startTransaction();
    strategy.removeTrieNode(storage, removal, key);
    strategy.onBeforeBlockCommit(storage, removal, 1L);
    removal.commit();
    assertThat(historyStore.getLatestBefore(naturalKey, 1L).orElseThrow().codecEntry().isDeletion())
        .isTrue();

    final SegmentedKeyValueStorage roller = registerRollerAt(strategy, 1L);
    plantStaleNode(roller, key, node);
    rollerPut(strategy, roller, key, node, 2L);

    final var entry = historyStore.getLatestBefore(naturalKey, 2L);
    assertThat(entry).isPresent();
    assertThat(entry.get().block()).isEqualTo(2L);
    assertThat(new ArchiveHistoryReader(historyStore).nodeAt(naturalKey, 2L)).contains(node);
  }

  @Test
  void rollerSkipsWriteEqualToArchiveStateEvenWhenScratchNodeDiffers() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x53);
    final Bytes node = Bytes.fromHexString("0x5353");
    put(strategy, location, node); // genesis: archive holds node at L

    final SegmentedKeyValueStorage roller = registerRollerAt(strategy, 0L);
    plantStaleNode(roller, location, Bytes.fromHexString("0x9999"));
    rollerPut(strategy, roller, location, node, 1L);

    assertThat(
            historyStore
                .getLatestBefore(ArchiveNodeKey.account(location), 1L)
                .orElseThrow()
                .block())
        .as("no-op from the archive's view: no entry at block 1")
        .isEqualTo(0L);
    assertThat(coverageTracker.lastIndexedBlock()).hasValue(1L);
  }

  @Test
  void rollerDiffIsEncodedAgainstArchiveStateNotStaleScratchNode() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x54);
    final Bytes naturalKey = ArchiveNodeKey.account(location);
    final Bytes archived = bigNode(1, 0); // A
    final Bytes stale = bigNode(2, 9); // Y: equals X at byte 10, differs at byte 50
    final Bytes written = bigNode(2, 0); // X
    put(strategy, location, archived); // genesis: FULL A

    final SegmentedKeyValueStorage roller = registerRollerAt(strategy, 0L);
    plantStaleNode(roller, location, stale);
    // a diff taken against Y would only patch byte 50, so A + diff(Y, X) would not be X
    rollerPut(strategy, roller, location, written, 1L);

    final var entry = historyStore.getLatestBefore(naturalKey, 1L).orElseThrow();
    assertThat(entry.block()).isEqualTo(1L);
    assertThat(entry.codecEntry().isFull()).as("small change is stored as a DIFF").isFalse();
    assertThat(new ArchiveHistoryReader(historyStore).nodeAt(naturalKey, 1L)).contains(written);
  }
}
