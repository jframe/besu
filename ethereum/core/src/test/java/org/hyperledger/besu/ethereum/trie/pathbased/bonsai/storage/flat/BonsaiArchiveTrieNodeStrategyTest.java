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
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeChangeIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeDiffCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the trie-node differential-index write hook added to {@link
 * BonsaiArchiveTrieNodeStrategy} in Task 3.3.
 *
 * <p>The tests focus on verifying that when the trie-node index flag is enabled, {@code
 * putFlatAccountTrieNode} and {@code putFlatStorageTrieNode} write correct entries to the {@code
 * TRIE_NODE_HISTORY_ARCHIVE} column family and update the change-block index, and that with the
 * flag disabled neither write occurs.
 */
class BonsaiArchiveTrieNodeStrategyTest {

  /** A trie node location (nibble path bytes, 5 bytes = depth 10 nibbles). */
  private static final Bytes LOCATION_DEEP = Bytes.fromHexString("0x0102030405");

  /**
   * An upper-trie location with only 1 nibble byte (depth 1) — exercises {@code
   * SHALLOW_CHECKPOINT_INTERVAL} (checkpoint every 32nd mutation).
   */
  private static final Bytes LOCATION_SHALLOW = Bytes.fromHexString("0x01");

  /** The trie root: an empty nibble-path location (depth 0) → always FULL. */
  private static final Bytes LOCATION_ROOT = Bytes.EMPTY;

  /** A dummy node hash (does not have to be the real hash of the RLP in unit tests). */
  private static final Bytes32 NODE_HASH =
      Bytes32.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  /** A realistic short-node RLP (2-item list: compact path + value). */
  private static final Bytes SHORT_NODE_V1 =
      RLP.encode(
          out -> {
            out.startList();
            out.writeBytes(Bytes.fromHexString("0x20")); // compact leaf path: 1 nibble
            out.writeBytes(Bytes.fromHexString("0xaabb")); // leaf value
            out.endList();
          });

  /** A second short-node RLP with the same structure but different value. */
  private static final Bytes SHORT_NODE_V2 =
      RLP.encode(
          out -> {
            out.startList();
            out.writeBytes(Bytes.fromHexString("0x20")); // same path
            out.writeBytes(Bytes.fromHexString("0xccdd")); // different value
            out.endList();
          });

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;
  private TrieNodeChangeIndex changeIndex;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
    changeIndex = new TrieNodeChangeIndex(storage, ArchiveNodeKey.RANGE_SIZE);
  }

  // ---------------------------------------------------------------------------
  // Factory helpers
  // ---------------------------------------------------------------------------

  /** Strategy with trie-node index ENABLED. */
  private BonsaiArchiveTrieNodeStrategy strategyWithIndex() {
    // Include a real progress object so advanceIndexProgress() (called from writeAtBlock/
    // writeStorageAtBlock) actually clears the block-number cache between blocks.
    return new BonsaiArchiveTrieNodeStrategy(
        null, // no trieLoader
        new BonsaiTrieNodeStrategy(),
        true, // trieNodeIndexEnabled
        historyStore,
        changeIndex,
        new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE));
  }

  /** Strategy with trie-node index ENABLED and progress tracking wired. */
  private BonsaiArchiveTrieNodeStrategy strategyWithIndexAndProgress(
      final TrieNodeIndexProgress progress) {
    return new BonsaiArchiveTrieNodeStrategy(
        null, // no trieLoader
        new BonsaiTrieNodeStrategy(),
        true, // trieNodeIndexEnabled
        historyStore,
        changeIndex,
        progress);
  }

  /** Strategy with trie-node index DISABLED. */
  private BonsaiArchiveTrieNodeStrategy strategyWithoutIndex() {
    return new BonsaiArchiveTrieNodeStrategy();
  }

  /**
   * Sets ARCHIVE_PROOF_BLOCK_NUMBER_KEY to {@code blockNumber} in committed storage.
   *
   * <p>Used together with the {@code advanceIndexProgress} call at the end of {@link
   * #writeAtBlock}/{@link #writeStorageAtBlock} to seed the correct block number for the next
   * block's first trie-node write (when the cache is cold after being cleared by {@code
   * advanceIndexProgress}).
   */
  private void setArchiveProofBlockNumber(final long blockNumber) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        ARCHIVE_PROOF_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }

  /** Writes {@code node} via the strategy at block {@code targetBlock}. */
  private void writeAtBlock(
      final BonsaiArchiveTrieNodeStrategy strategy,
      final Bytes location,
      final Bytes node,
      final long targetBlock) {
    setArchiveProofBlockNumber(targetBlock);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, NODE_HASH, node);
    tx.commit();
    // Advance index progress to clear the block-number cache, matching production semantics
    // (flushIndexIfEnabled → advanceIndexProgress runs at the end of each block's persist).
    final SegmentedKeyValueStorageTransaction progressTx = storage.startTransaction();
    strategy.advanceIndexProgress(progressTx, storage);
    progressTx.commit();
  }

  /** Writes a storage trie node via the strategy at block {@code targetBlock}. */
  private void writeStorageAtBlock(
      final BonsaiArchiveTrieNodeStrategy strategy,
      final Hash accountHash,
      final Bytes location,
      final Bytes node,
      final long targetBlock) {
    setArchiveProofBlockNumber(targetBlock);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatStorageTrieNode(storage, tx, accountHash, location, NODE_HASH, node);
    tx.commit();
    // Advance index progress to clear the block-number cache.
    final SegmentedKeyValueStorageTransaction progressTx = storage.startTransaction();
    strategy.advanceIndexProgress(progressTx, storage);
    progressTx.commit();
  }

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

  // ---------------------------------------------------------------------------
  // Account-trie tests (flag ON)
  // ---------------------------------------------------------------------------

  @Test
  void flagEnabled_firstWrite_producesFullCreationEntry() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();

    writeAtBlock(strategy, LOCATION_DEEP, SHORT_NODE_V1, 100L);

    final Bytes naturalKey = ArchiveNodeKey.account(LOCATION_DEEP);
    final java.util.Optional<Bytes> entryOpt = historyStore.get(naturalKey, 100L);
    assertThat(entryOpt).isPresent();

    final TrieNodeDiffCodec.Decoded decoded = TrieNodeDiffCodec.decode(entryOpt.get());
    // First write is a creation → FULL | CREATION.
    assertThat(decoded.isFull()).isTrue();
    assertThat(decoded.isCreation()).isTrue();
    assertThat(decoded.fullNode()).isEqualTo(SHORT_NODE_V1);
  }

  @Test
  void flagEnabled_secondWrite_producesCorrectDiffEntry() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();

    writeAtBlock(strategy, LOCATION_DEEP, SHORT_NODE_V1, 100L);
    writeAtBlock(strategy, LOCATION_DEEP, SHORT_NODE_V2, 101L);

    final Bytes naturalKey = ArchiveNodeKey.account(LOCATION_DEEP);

    // Block 100 should still be FULL | CREATION.
    final java.util.Optional<Bytes> entry100 = historyStore.get(naturalKey, 100L);
    assertThat(entry100).isPresent();
    assertThat(TrieNodeDiffCodec.decode(entry100.get()).isFull()).isTrue();
    assertThat(TrieNodeDiffCodec.decode(entry100.get()).isCreation()).isTrue();

    // Block 101 should be a DIFF (mutation 1, not a checkpoint, not upper-trie, not creation).
    final java.util.Optional<Bytes> entry101 = historyStore.get(naturalKey, 101L);
    assertThat(entry101).isPresent();
    final TrieNodeDiffCodec.Decoded decoded101 = TrieNodeDiffCodec.decode(entry101.get());
    assertThat(decoded101.isFull()).isFalse();
    assertThat(decoded101.isDeletion()).isFalse();
    // Short-node DIFF: isShortNodeDiff() true.
    assertThat(decoded101.isShortNodeDiff()).isTrue();
  }

  @Test
  void flagEnabled_changeIndex_recordsBothBlocks() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();

    writeAtBlock(strategy, LOCATION_DEEP, SHORT_NODE_V1, 100L);
    writeAtBlock(strategy, LOCATION_DEEP, SHORT_NODE_V2, 101L);

    final Bytes naturalKey = ArchiveNodeKey.account(LOCATION_DEEP);

    // latestChangeBlock(key, 101) should return 101.
    assertThat(changeIndex.latestChangeBlock(naturalKey, 101L)).contains(101L);
    // latestChangeBlock(key, 100) should return 100.
    assertThat(changeIndex.latestChangeBlock(naturalKey, 100L)).contains(100L);
  }

  @Test
  void flagEnabled_shallowLocation_checkpointsAtInterval32() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();

    // LOCATION_SHALLOW has size 1 (depth 1) → SHALLOW_CHECKPOINT_INTERVAL (32), no longer forced
    // FULL.
    // Block 100 = creation (FULL). Block 101 = mutation 1 → DIFF (not a checkpoint).
    writeAtBlock(strategy, LOCATION_SHALLOW, SHORT_NODE_V1, 100L);
    writeAtBlock(strategy, LOCATION_SHALLOW, SHORT_NODE_V2, 101L);

    final Bytes naturalKey = ArchiveNodeKey.account(LOCATION_SHALLOW);

    final java.util.Optional<Bytes> entry100 = historyStore.get(naturalKey, 100L);
    assertThat(entry100).isPresent();
    assertThat(TrieNodeDiffCodec.decode(entry100.get()).isFull()).isTrue();
    assertThat(TrieNodeDiffCodec.decode(entry100.get()).isCreation()).isTrue();

    // Block 101 is now a DIFF (previously this location was forced FULL).
    final java.util.Optional<Bytes> entry101 = historyStore.get(naturalKey, 101L);
    assertThat(entry101).isPresent();
    assertThat(TrieNodeDiffCodec.decode(entry101.get()).isFull()).isFalse();
    assertThat(TrieNodeDiffCodec.decode(entry101.get()).isShortNodeDiff()).isTrue();
  }

  @Test
  void flagEnabled_rootLocation_alwaysWritesFull() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();

    // Root (empty location, depth 0) → interval 1 → FULL at every block.
    writeAtBlock(strategy, LOCATION_ROOT, SHORT_NODE_V1, 100L);
    writeAtBlock(strategy, LOCATION_ROOT, SHORT_NODE_V2, 101L);
    writeAtBlock(strategy, LOCATION_ROOT, SHORT_NODE_V1, 102L);

    final Bytes naturalKey = ArchiveNodeKey.account(LOCATION_ROOT);

    for (final long block : new long[] {100L, 101L, 102L}) {
      final java.util.Optional<Bytes> entry = historyStore.get(naturalKey, block);
      assertThat(entry).as("root entry at block %s", block).isPresent();
      assertThat(TrieNodeDiffCodec.decode(entry.get()).isFull())
          .as("root entry at block %s must be FULL", block)
          .isTrue();
    }
    // Root stays indexed in this plan (skip-root is deferred): the index records every change.
    assertThat(changeIndex.latestChangeBlock(naturalKey, 102L)).contains(102L);
  }

  @Test
  void flagEnabled_deepNode_every16thMutationIsFull() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();
    final Bytes naturalKey = ArchiveNodeKey.account(LOCATION_DEEP);

    writeAtBlock(strategy, LOCATION_DEEP, SHORT_NODE_V1, 100L);

    for (int i = 1; i <= BonsaiArchiveTrieNodeStrategy.DEEP_CHECKPOINT_INTERVAL; i++) {
      final Bytes node = (i % 2 == 0) ? SHORT_NODE_V1 : SHORT_NODE_V2;
      writeAtBlock(strategy, LOCATION_DEEP, node, 100L + i);
    }

    final long checkpointBlock = 100L + BonsaiArchiveTrieNodeStrategy.DEEP_CHECKPOINT_INTERVAL;
    final java.util.Optional<Bytes> entryAtCheckpoint =
        historyStore.get(naturalKey, checkpointBlock);
    assertThat(entryAtCheckpoint).isPresent();
    assertThat(TrieNodeDiffCodec.decode(entryAtCheckpoint.get()).isFull()).isTrue();

    final java.util.Optional<Bytes> entryBeforeCheckpoint =
        historyStore.get(naturalKey, checkpointBlock - 1);
    assertThat(entryBeforeCheckpoint).isPresent();
    assertThat(TrieNodeDiffCodec.decode(entryBeforeCheckpoint.get()).isFull()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Account-trie test (flag OFF)
  // ---------------------------------------------------------------------------

  @Test
  void flagDisabled_noHistoryOrIndexWritten() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithoutIndex();

    writeAtBlock(strategy, LOCATION_DEEP, SHORT_NODE_V1, 100L);

    final Bytes naturalKey = ArchiveNodeKey.account(LOCATION_DEEP);

    // History store should have no entry.
    assertThat(historyStore.get(naturalKey, 100L)).isEmpty();

    // Change index should have no entry.
    assertThat(changeIndex.latestChangeBlock(naturalKey, 100L)).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Storage-trie tests (flag ON)
  // ---------------------------------------------------------------------------

  @Test
  void storageNode_flagEnabled_firstWrite_producesFullCreationEntry() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000001").addressHash();

    writeStorageAtBlock(strategy, accountHash, LOCATION_DEEP, SHORT_NODE_V1, 100L);

    final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), LOCATION_DEEP);
    final java.util.Optional<Bytes> entryOpt = historyStore.get(naturalKey, 100L);
    assertThat(entryOpt).isPresent();

    final TrieNodeDiffCodec.Decoded decoded = TrieNodeDiffCodec.decode(entryOpt.get());
    assertThat(decoded.isFull()).isTrue();
    assertThat(decoded.isCreation()).isTrue();
    assertThat(decoded.fullNode()).isEqualTo(SHORT_NODE_V1);
  }

  @Test
  void storageNode_flagEnabled_secondWrite_producesDiffEntry() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000002").addressHash();

    writeStorageAtBlock(strategy, accountHash, LOCATION_DEEP, SHORT_NODE_V1, 100L);
    writeStorageAtBlock(strategy, accountHash, LOCATION_DEEP, SHORT_NODE_V2, 101L);

    final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), LOCATION_DEEP);

    // Block 100: FULL | CREATION.
    assertThat(historyStore.get(naturalKey, 100L)).isPresent();
    assertThat(TrieNodeDiffCodec.decode(historyStore.get(naturalKey, 100L).get()).isFull())
        .isTrue();

    // Block 101: DIFF (short-node diff).
    final java.util.Optional<Bytes> entry101 = historyStore.get(naturalKey, 101L);
    assertThat(entry101).isPresent();
    assertThat(TrieNodeDiffCodec.decode(entry101.get()).isFull()).isFalse();
    assertThat(TrieNodeDiffCodec.decode(entry101.get()).isShortNodeDiff()).isTrue();

    // Change index records both blocks.
    assertThat(changeIndex.latestChangeBlock(naturalKey, 101L)).contains(101L);
    assertThat(changeIndex.latestChangeBlock(naturalKey, 100L)).contains(100L);
  }

  @Test
  void storageNode_flagDisabled_noHistoryOrIndexWritten() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithoutIndex();
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000003").addressHash();

    writeStorageAtBlock(strategy, accountHash, LOCATION_DEEP, SHORT_NODE_V1, 100L);

    final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), LOCATION_DEEP);
    assertThat(historyStore.get(naturalKey, 100L)).isEmpty();
    assertThat(changeIndex.latestChangeBlock(naturalKey, 100L)).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Storage-trie depth-tiered interval uses location, not naturalKey (issue 3)
  // ---------------------------------------------------------------------------

  @Test
  void storageNode_flagEnabled_shallowLocation_checkpointsAtInterval32() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000004").addressHash();

    // Storage-trie node at a shallow location (size 1, depth 1): the depth is taken from the
    // nibble-path `location`, not the 33-byte naturalKey, so it too moves to interval 32.
    writeStorageAtBlock(strategy, accountHash, LOCATION_SHALLOW, SHORT_NODE_V1, 100L);
    writeStorageAtBlock(strategy, accountHash, LOCATION_SHALLOW, SHORT_NODE_V2, 101L);

    final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), LOCATION_SHALLOW);

    final java.util.Optional<Bytes> entry100 = historyStore.get(naturalKey, 100L);
    assertThat(entry100).isPresent();
    assertThat(TrieNodeDiffCodec.decode(entry100.get()).isFull()).isTrue();
    assertThat(TrieNodeDiffCodec.decode(entry100.get()).isCreation()).isTrue();

    final java.util.Optional<Bytes> entry101 = historyStore.get(naturalKey, 101L);
    assertThat(entry101).isPresent();
    assertThat(TrieNodeDiffCodec.decode(entry101.get()).isFull()).isFalse();
    assertThat(TrieNodeDiffCodec.decode(entry101.get()).isShortNodeDiff()).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Task 3.4: Coverage-progress advancement on block flush
  // ---------------------------------------------------------------------------

  /**
   * After writing a node at block N and flushing with storage context, {@code
   * progress.lastIndexedBlock()} must equal N.
   */
  @Test
  void progress_flushAdvancesLastIndexedBlock() {
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndexAndProgress(progress);

    final long targetBlock = 100L;
    setArchiveProofBlockNumber(targetBlock);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, LOCATION_DEEP, NODE_HASH, SHORT_NODE_V1);
    strategy.advanceIndexProgress(tx, storage);
    tx.commit();

    assertThat(progress.lastIndexedBlock()).isEqualTo(targetBlock);
  }

  /**
   * At the range boundary (block = rangeSize - 1), {@code progress.lastIndexedBlock()} must equal
   * the boundary block and {@code indexStartBlock()} must be the range start.
   */
  @Test
  void progress_atRangeBoundary_advancesLastIndexedBlock() {
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndexAndProgress(progress);

    final long lastBlockInRange0 = ArchiveNodeKey.RANGE_SIZE - 1;
    setArchiveProofBlockNumber(lastBlockInRange0);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, LOCATION_DEEP, NODE_HASH, SHORT_NODE_V1);
    strategy.advanceIndexProgress(tx, storage);
    tx.commit();

    assertThat(progress.lastIndexedBlock()).isEqualTo(lastBlockInRange0);
    assertThat(progress.indexStartBlock()).isEqualTo(0L);
  }

  /**
   * After indexing a mid-range block, {@code covers(block)} returns true for that block (window
   * semantics: any block in [indexStartBlock, lastIndexedBlock] is covered), but returns false for
   * blocks beyond lastIndexedBlock.
   */
  @Test
  void progress_midRangeBlock_doesNotMarkRangeComplete() {
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndexAndProgress(progress);

    // Block 500_000 is mid-range (range 0 spans [0, 999_999]).
    final long midBlock = 500_000L;
    setArchiveProofBlockNumber(midBlock);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, LOCATION_DEEP, NODE_HASH, SHORT_NODE_V1);
    strategy.advanceIndexProgress(tx, storage);
    tx.commit();

    assertThat(progress.lastIndexedBlock()).isEqualTo(midBlock);
    // Window semantics: block is in [indexStartBlock=0, lastIndexedBlock=500_000] → covered.
    assertThat(progress.covers(midBlock)).isTrue();
    // Block beyond the window is not covered.
    assertThat(progress.covers(midBlock + 1)).isFalse();
  }

  /**
   * {@code TrieNodeIndexProgress.load(storage, rangeSize)} round-trips: after saving via {@code
   * advanceIndexProgress}, loading from the same storage returns an equivalent record.
   */
  @Test
  void progress_loadRoundTrip() {
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndexAndProgress(progress);

    final long lastBlockInRange0 = ArchiveNodeKey.RANGE_SIZE - 1;
    setArchiveProofBlockNumber(lastBlockInRange0);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, LOCATION_DEEP, NODE_HASH, SHORT_NODE_V1);
    strategy.advanceIndexProgress(tx, storage);
    tx.commit();

    // Now load from the same storage.
    final TrieNodeIndexProgress loaded =
        TrieNodeIndexProgress.load(storage, ArchiveNodeKey.RANGE_SIZE);
    assertThat(loaded.lastIndexedBlock()).isEqualTo(lastBlockInRange0);
    assertThat(loaded.indexStartBlock()).isEqualTo(0L);
  }

  @Test
  void advanceIndexProgress_setsLastIndexedBlock() {
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(1_000_000L);
    final BonsaiArchiveTrieNodeStrategy strat =
        new BonsaiArchiveTrieNodeStrategy(
            null, new BonsaiTrieNodeStrategy(), true, historyStore, changeIndex, progress);
    setArchiveProofBlockNumber(42L);

    final var tx = storage.startTransaction();
    strat.advanceIndexProgress(tx, storage); // renamed method
    tx.commit();

    assertThat(progress.lastIndexedBlock()).isEqualTo(42L);
    assertThat(progress.indexStartBlock()).isEqualTo(0L); // start of range 0
  }

  /**
   * When the trie-node index is disabled, {@code advanceIndexProgress(tx, storage)} is a no-op —
   * coverage never advances.
   */
  @Test
  void progress_flagDisabled_noProgressWritten() {
    // When trieNodeIndexEnabled=false, advanceIndexProgress(tx, storage) must NOT persist any
    // progress bytes (it simply skips the progress block).
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);
    // Construct WITH progress, but index disabled — the index-disabled code path never calls
    // progress.setLastIndexedBlock, so lastIndexedBlock stays UNSET.
    final BonsaiArchiveTrieNodeStrategy strategy =
        new BonsaiArchiveTrieNodeStrategy(
            null,
            new BonsaiTrieNodeStrategy(),
            false, // trieNodeIndexEnabled = false
            historyStore,
            changeIndex,
            progress);

    final long targetBlock = 100L;
    setArchiveProofBlockNumber(targetBlock);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, LOCATION_DEEP, NODE_HASH, SHORT_NODE_V1);
    strategy.advanceIndexProgress(tx, storage);
    tx.commit();

    // Index is disabled: progress must not have advanced.
    assertThat(progress.lastIndexedBlock()).isEqualTo(TrieNodeIndexProgress.UNSET_LAST_INDEXED);
  }

  @Test
  void checkpointIntervalForDepth_mapsDepthToInterval() {
    // Root (empty location, 0 bytes) is always FULL → interval 1.
    assertThat(BonsaiArchiveTrieNodeStrategy.checkpointIntervalForDepth(0)).isEqualTo(1);
    // Shallow non-root nodes (1–2 location bytes = trie levels 1–4) → interval 32.
    assertThat(BonsaiArchiveTrieNodeStrategy.checkpointIntervalForDepth(1)).isEqualTo(32);
    assertThat(BonsaiArchiveTrieNodeStrategy.checkpointIntervalForDepth(2)).isEqualTo(32);
    // Deep nodes (>= 3 location bytes) → interval 16.
    assertThat(BonsaiArchiveTrieNodeStrategy.checkpointIntervalForDepth(3)).isEqualTo(16);
    assertThat(BonsaiArchiveTrieNodeStrategy.checkpointIntervalForDepth(5)).isEqualTo(16);
    assertThat(BonsaiArchiveTrieNodeStrategy.checkpointIntervalForDepth(32)).isEqualTo(16);
  }

  @Test
  void maxCheckpointInterval_fitsReconstructWindow() {
    final int maxInterval =
        Math.max(
            BonsaiArchiveTrieNodeStrategy.ROOT_CHECKPOINT_INTERVAL,
            Math.max(
                BonsaiArchiveTrieNodeStrategy.SHALLOW_CHECKPOINT_INTERVAL,
                BonsaiArchiveTrieNodeStrategy.DEEP_CHECKPOINT_INTERVAL));
    // The reader scans only the trailing RECONSTRUCT_WINDOW change-blocks for a FULL. If the
    // largest checkpoint interval exceeded that, a DIFF target could have no FULL in the window and
    // reconstruction would return empty → eth_getProof would fail. Keep them coupled.
    assertThat(maxInterval).isLessThanOrEqualTo(TrieNodeHistoryReader.RECONSTRUCT_WINDOW);
    assertThat(maxInterval).isLessThanOrEqualTo(TrieNodeHistoryReader.MAX_BACKWARD_WALK_STEPS);
  }

  @Test
  void shallowNode_reconstructsMidDiffChain_withinReconstructWindow() {
    final BonsaiArchiveTrieNodeStrategy strategy = strategyWithIndex();
    // Depth-1 (shallow) node → interval 32. Write 40 mutations so the chain spans past one
    // checkpoint and the target sits inside a diff run whose FULL base is up to 32 entries back —
    // well within RECONSTRUCT_WINDOW (64).
    final long baseBlock = 1_000L;
    final int mutations = 40;
    final java.util.Map<Long, Bytes> writtenByBlock = new java.util.HashMap<>();
    for (int i = 0; i <= mutations; i++) {
      final Bytes node = branchWith(i % 16, 1 + i); // distinct node each block
      final long block = baseBlock + i;
      writeAtBlock(strategy, LOCATION_SHALLOW, node, block);
      writtenByBlock.put(block, node);
    }

    final Bytes naturalKey = ArchiveNodeKey.account(LOCATION_SHALLOW);
    final TrieNodeHistoryReader reader = new TrieNodeHistoryReader(historyStore, changeIndex);

    // Reconstruct at a block that is a DIFF (mutation 20, not a multiple of 32) and confirm it
    // equals exactly what was written there.
    final long targetBlock = baseBlock + 20;
    assertThat(TrieNodeDiffCodec.decode(historyStore.get(naturalKey, targetBlock).get()).isFull())
        .as("mutation 20 should be a DIFF, not a checkpoint")
        .isFalse();

    final java.util.Optional<Bytes> reconstructed = reader.nodeAt(naturalKey, targetBlock);
    assertThat(reconstructed).isPresent();
    assertThat(reconstructed.get()).isEqualTo(writtenByBlock.get(targetBlock));

    // Also reconstruct at the checkpoint block (mutation 32 = FULL) and at the last mutation.
    assertThat(reader.nodeAt(naturalKey, baseBlock + 32))
        .hasValue(writtenByBlock.get(baseBlock + 32));
    assertThat(reader.nodeAt(naturalKey, baseBlock + mutations))
        .hasValue(writtenByBlock.get(baseBlock + mutations));
  }
}
