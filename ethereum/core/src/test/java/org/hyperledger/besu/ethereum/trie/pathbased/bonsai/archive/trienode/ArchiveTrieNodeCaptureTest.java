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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeCaptureTest {

  private static final Bytes LOCATION = Bytes.of(0x01);
  private static final Bytes ROOT_LOCATION = Bytes.EMPTY;

  private static Bytes branchNode(final int seed) {
    final byte[] childRef = new byte[33];
    childRef[0] = (byte) 0xa0;
    for (int i = 1; i < 33; i++) {
      childRef[i] = (byte) (i + seed);
    }
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(Bytes.wrap(childRef));
    for (int i = 1; i < 16; i++) {
      out.writeNull();
    }
    out.writeBytes(Bytes.EMPTY);
    out.endList();
    return out.encoded();
  }

  private static Bytes32 hashOf(final Bytes node) {
    return Bytes32.wrap(Hash.hash(node).getBytes());
  }

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveCoverageTracker coverageTracker;
  private ArchiveTrieNodeCapture capture;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    coverageTracker = new ArchiveCoverageTracker(storage);
    capture =
        new ArchiveTrieNodeCapture(historyStore, coverageTracker, Executors.newFixedThreadPool(2));
  }

  private void enqueueAndCommit(
      final long block,
      final Bytes naturalKey,
      final Bytes location,
      final Bytes newNode,
      final Bytes priorNode) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    capture.enqueue(naturalKey, location, block, null, Bytes32.ZERO, newNode, priorNode, tx);
    capture.onBeforeCommit(tx);
    tx.commit();
  }

  @Test
  void creationIsStoredAsFull() {
    final Bytes node = branchNode(0);
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, node, null);

    final var entry = historyStore.getLatestBefore(nk, 0L).orElseThrow();
    assertThat(entry.codecEntry().isFull()).isTrue();
    assertThat(entry.counter()).isZero();
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void updateOnContiguousChainIsDiff() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, branchNode(0), null);
    enqueueAndCommit(1L, nk, LOCATION, branchNode(1), branchNode(0));

    final var diff = historyStore.getLatestBefore(nk, 1L).orElseThrow();
    assertThat(diff.block()).isEqualTo(1L);
    assertThat(diff.counter()).isEqualTo(1);
    assertThat(diff.codecEntry().isFull()).isFalse();
    assertThat(diff.codecEntry().isDeletion()).isFalse();
  }

  @Test
  void deletionWithPriorStoresTombstone() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, branchNode(0), null);
    enqueueAndCommit(1L, nk, LOCATION, null, branchNode(0));

    final var tombstone = historyStore.getLatestBefore(nk, 1L).orElseThrow();
    assertThat(tombstone.block()).isEqualTo(1L);
    assertThat(tombstone.codecEntry().isDeletion()).isTrue();
  }

  @Test
  void nonContiguousUpdateForcesFullCheckpoint() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, branchNode(0), null);
    // block 5 does not follow block 0 — chain is not contiguous
    enqueueAndCommit(5L, nk, LOCATION, branchNode(5), branchNode(4));

    final var entry = historyStore.getLatestBefore(nk, 5L).orElseThrow();
    assertThat(entry.block()).isEqualTo(5L);
    assertThat(entry.codecEntry().isFull()).isTrue();
    assertThat(entry.counter()).isZero();
  }

  @Test
  void rootSparseChange_producesDiffBetweenCheckpoints() {
    // Root now participates in checkpoint+diff. branchNode(0)->branchNode(1) changes one 32-byte
    // child slot of a ~50-byte node, so the diff is smaller than the node -> DIFF (was FULL).
    final Bytes nk = ArchiveNodeKey.account(ROOT_LOCATION);
    enqueueAndCommit(0L, nk, ROOT_LOCATION, branchNode(0), null);
    enqueueAndCommit(1L, nk, ROOT_LOCATION, branchNode(1), branchNode(0));

    final var creation = historyStore.getLatestBefore(nk, 0L).orElseThrow();
    assertThat(creation.codecEntry().isFull()).isTrue();
    assertThat(creation.counter()).isZero();

    final var diff = historyStore.getLatestBefore(nk, 1L).orElseThrow();
    assertThat(diff.codecEntry().isFull()).isFalse();
    assertThat(diff.codecEntry().isDeletion()).isFalse();
    assertThat(diff.counter()).isEqualTo(1);
  }

  @Test
  void rootDrasticChange_staysFullEveryBlock() {
    // When each block's root shares nothing with the prior, encodeDiff falls back to FULL every
    // block -> byte-identical to the old always-FULL behaviour (the mainnet case).
    final Bytes nk = ArchiveNodeKey.account(ROOT_LOCATION);
    enqueueAndCommit(0L, nk, ROOT_LOCATION, disjointNode(0), null);
    enqueueAndCommit(1L, nk, ROOT_LOCATION, disjointNode(1), disjointNode(0));
    enqueueAndCommit(2L, nk, ROOT_LOCATION, disjointNode(2), disjointNode(1));

    for (long block = 0; block <= 2; block++) {
      assertThat(historyStore.getLatestBefore(nk, block).orElseThrow().codecEntry().isFull())
          .as("drastic root change at block %s must fall back to FULL", block)
          .isTrue();
    }
  }

  @Test
  void storageRootSparseChange_producesDiff() {
    // Storage-trie root: natural key = accountHash(32) ‖ [len:0]. Same self-tuning behaviour.
    final Bytes accountHash = Bytes32.leftPad(Bytes.of(0x04));
    final Bytes nk = ArchiveNodeKey.storage(accountHash, ROOT_LOCATION);
    enqueueAndCommit(0L, nk, ROOT_LOCATION, branchNode(0), null);
    enqueueAndCommit(1L, nk, ROOT_LOCATION, branchNode(1), branchNode(0));

    assertThat(historyStore.getLatestBefore(nk, 0L).orElseThrow().codecEntry().isFull()).isTrue();
    assertThat(historyStore.getLatestBefore(nk, 1L).orElseThrow().codecEntry().isFull()).isFalse();
  }

  @Test
  void rollbackDiscardsCaptureBuffer() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    capture.enqueue(nk, LOCATION, 0L, null, Bytes32.ZERO, branchNode(0), null, tx);
    capture.onRollback(tx);
    tx.rollback();

    assertThat(historyStore.getLatestBefore(nk, 0L)).isEmpty();
  }

  @Test
  void foreignTransactionIgnoredOnRollback() {
    // Simulates commitTrieLogOnly() mid-block: a different updater's onRollback arrives while
    // the owning updater's buffer is still live. The buffer must not be discarded.
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    capture.enqueue(nk, LOCATION, 0L, null, Bytes32.ZERO, branchNode(0), null, tx);

    final SegmentedKeyValueStorageTransaction foreignTx = storage.startTransaction();
    capture.onRollback(foreignTx); // must be ignored
    foreignTx.rollback();

    // Buffer is still intact — flush with the owning tx.
    capture.onBeforeCommit(tx);
    tx.commit();

    assertThat(historyStore.getLatestBefore(nk, 0L)).isPresent();
  }

  @Test
  void checkpointIntervalForDepth_mapsDepthToInterval() {
    // Root (depth 0) now maps to ROOT_CHECKPOINT_INTERVAL instead of being excluded.
    assertThat(ArchiveTrieNodeCapture.checkpointIntervalForDepth(0))
        .isEqualTo(ArchiveTrieNodeCapture.ROOT_CHECKPOINT_INTERVAL);
    assertThat(ArchiveTrieNodeCapture.checkpointIntervalForDepth(1))
        .isEqualTo(ArchiveTrieNodeCapture.SHALLOW_CHECKPOINT_INTERVAL);
    assertThat(ArchiveTrieNodeCapture.checkpointIntervalForDepth(2))
        .isEqualTo(ArchiveTrieNodeCapture.SHALLOW_CHECKPOINT_INTERVAL);
    assertThat(ArchiveTrieNodeCapture.checkpointIntervalForDepth(3))
        .isEqualTo(ArchiveTrieNodeCapture.DEEP_CHECKPOINT_INTERVAL);
    assertThat(ArchiveTrieNodeCapture.checkpointIntervalForDepth(5))
        .isEqualTo(ArchiveTrieNodeCapture.DEEP_CHECKPOINT_INTERVAL);
    assertThat(ArchiveTrieNodeCapture.checkpointIntervalForDepth(32))
        .isEqualTo(ArchiveTrieNodeCapture.DEEP_CHECKPOINT_INTERVAL);
  }

  @Test
  void maxCheckpointInterval_fitsReconstructWindow() {
    final int maxInterval =
        Math.max(
            ArchiveTrieNodeCapture.ROOT_CHECKPOINT_INTERVAL,
            Math.max(
                ArchiveTrieNodeCapture.SHALLOW_CHECKPOINT_INTERVAL,
                ArchiveTrieNodeCapture.DEEP_CHECKPOINT_INTERVAL));
    // The reader scans only the trailing MAX_BACKWARD_WALK_STEPS change-blocks for a FULL. If the
    // largest checkpoint interval (now including the root) exceeded that, a DIFF target could have
    // no FULL in the window and reconstruction would return empty -> eth_getProof would fail.
    assertThat(maxInterval).isLessThanOrEqualTo(ArchiveHistoryReader.MAX_BACKWARD_WALK_STEPS);
  }

  @Test
  void shallowNode_checksAtInterval32_notInterval16() {
    // Depth-1 node (LOCATION has size 1): SHALLOW_CHECKPOINT_INTERVAL = 32.
    // With the old single-interval-16 rule, mutation 16 would produce a FULL; with 32 it must DIFF.
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, branchNode(0), null);
    for (int block = 1; block <= ArchiveHistoryReader.CHECKPOINT_INTERVAL; block++) {
      enqueueAndCommit((long) block, nk, LOCATION, branchNode(block), branchNode(block - 1));
    }
    // Block 16 (= old CHECKPOINT_INTERVAL) should be a DIFF under the new tiered policy.
    final var entry16 =
        historyStore.getLatestBefore(nk, (long) ArchiveHistoryReader.CHECKPOINT_INTERVAL);
    assertThat(entry16).isPresent();
    assertThat(entry16.get().codecEntry().isFull())
        .as("block 16 should be DIFF, not FULL, for a shallow node (interval 32)")
        .isFalse();
  }

  // ~530-byte node in which exactly one byte changes per block — a small, diff-friendly per-block
  // delta typical of a real (deep) trie node's churn, where the wider interval genuinely wins.
  private static Bytes smallDeltaNode(final int block) {
    final byte[] b = new byte[530];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i * 31);
    }
    b[7] = (byte) block;
    return Bytes.wrap(b);
  }

  // ~530-byte node in which every byte differs from the previous block — the patch is >= the full
  // node, so encodeDiff falls back to a FULL every block regardless of interval. This is the tiny
  // local-QBFT case: shallow nodes cover a large fraction of the small keyspace and change
  // drastically each block, so raising the checkpoint interval stores nothing extra AND saves
  // nothing.
  private static Bytes drasticNode(final int block) {
    final byte[] b = new byte[530];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i * 31 + block * 131 + 1);
    }
    return Bytes.wrap(b);
  }

  // 64-byte node fully determined by seed; consecutive seeds share no bytes, so encodeDiff's
  // patch is >= the node and it falls back to FULL. Models dense (mainnet-shaped) root churn.
  private static Bytes disjointNode(final int seed) {
    final byte[] b = new byte[64];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i * 31 + seed * 131 + 1);
    }
    return Bytes.wrap(b);
  }

  /**
   * Replays an identical mutation sequence for a node at {@code location} on a fresh capture
   * instance, then sums the on-disk stored-value bytes across all blocks. Stored value = 1 counter
   * byte + the codec entry.
   */
  private long replayAndSumStoredBytes(
      final Bytes location, final int blocks, final IntFunction<Bytes> nodeAt) {
    final Bytes nk = ArchiveNodeKey.account(location);
    final ExecutorService pool = Executors.newFixedThreadPool(2);
    final ArchiveTrieNodeCapture localCapture =
        new ArchiveTrieNodeCapture(historyStore, coverageTracker, pool);
    Bytes prior = null;
    for (int block = 0; block < blocks; block++) {
      final Bytes node = nodeAt.apply(block);
      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      localCapture.enqueue(nk, location, block, null, Bytes32.ZERO, node, prior, tx);
      localCapture.onBeforeCommit(tx);
      tx.commit();
      prior = node;
    }
    pool.shutdown();
    long total = 0;
    for (int block = 0; block < blocks; block++) {
      total +=
          1 + historyStore.getLatestBefore(nk, (long) block).orElseThrow().rawEntryBytes().size();
    }
    return total;
  }

  @Test
  void widerShallowIntervalReducesStoredBytes_forSmallPerBlockDeltas() {
    // Shallow location (size 1 -> interval 32) vs deep location (size 3 -> interval 16), same
    // 128-block sequence with a 1-byte-per-block delta on a ~530-byte node.
    final int blocks = 128;
    final long shallowBytes =
        replayAndSumStoredBytes(Bytes.of(0x0a), blocks, ArchiveTrieNodeCaptureTest::smallDeltaNode);
    final long deepBytes =
        replayAndSumStoredBytes(
            Bytes.of(0x0b, 0x0c, 0x0d), blocks, ArchiveTrieNodeCaptureTest::smallDeltaNode);

    // Monotonic invariant: a wider interval can never store MORE (encodeDiff never exceeds a FULL).
    assertThat(shallowBytes)
        .as("interval-32 (%s B) must never exceed interval-16 (%s B)", shallowBytes, deepBytes)
        .isLessThanOrEqualTo(deepBytes);
    // With small per-block deltas the wider interval genuinely wins: fewer forced FULL checkpoints.
    assertThat(shallowBytes)
        .as(
            "interval-32 (%s B) should be strictly smaller than interval-16 (%s B) for small deltas",
            shallowBytes, deepBytes)
        .isLessThan(deepBytes);
  }

  @Test
  void intervalHasNoEffectOnStoredBytes_whenEveryBlockChangesDrastically() {
    // When each block's node shares nothing with the previous, encodeDiff falls back to FULL every
    // block, so interval-32 and interval-16 store byte-for-byte identically. This is why the local
    // QBFT network (tiny trie, drastically-churning shallow nodes) shows no logical benefit — and
    // why a small on-disk *increase* there is RocksDB blob/compaction noise, not this change.
    final int blocks = 128;
    final long shallowBytes =
        replayAndSumStoredBytes(Bytes.of(0x0a), blocks, ArchiveTrieNodeCaptureTest::drasticNode);
    final long deepBytes =
        replayAndSumStoredBytes(
            Bytes.of(0x0b, 0x0c, 0x0d), blocks, ArchiveTrieNodeCaptureTest::drasticNode);

    assertThat(shallowBytes)
        .as("drastic churn: interval-32 (%s B) == interval-16 (%s B)", shallowBytes, deepBytes)
        .isEqualTo(deepBytes);
  }

  @Test
  void chunkBoundaryFlushesAllNodes() {
    // 65 creations in one block: the first 64 are auto-submitted as a chunk during enqueue, the
    // 65th is submitted in onBeforeCommit. All 65 must appear in the archive.
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    for (int i = 0; i < 65; i++) {
      final Bytes loc = Bytes.of((byte) (i + 2)); // +2 avoids ROOT_LOCATION and LOCATION
      capture.enqueue(
          ArchiveNodeKey.account(loc),
          loc,
          0L,
          null,
          hashOf(branchNode(i)),
          branchNode(i),
          null,
          tx);
    }
    capture.onBeforeCommit(tx);
    tx.commit();

    for (int i = 0; i < 65; i++) {
      final Bytes loc = Bytes.of((byte) (i + 2));
      assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(loc), 0L)).isPresent();
    }
  }
}
