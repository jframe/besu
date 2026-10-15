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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TrieNodeStrategy} that archives every trie-node write into {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE} so historical {@code eth_getProof} requests don't need trie-log
 * replay.
 *
 * <p>Each put delegates to {@code base} (live trie) first, then — if the capture rule admits the
 * transaction — reads the prior node value from storage (where it is typically hot in the top few
 * in-memory layers) and calls {@link ArchiveTrieNodeWriter#capture} to queue an async history-entry
 * write. Workers compute the history entry ({@link ArchiveNodeHistoryStore#getLatestBefore} +
 * encode FULL/DIFF) off the import thread. Results are joined and applied to the transaction in
 * {@link #onBeforeCommit}, which runs immediately before commit.
 *
 * <p>Reading the prior node on the calling thread — rather than inside a worker — avoids disk I/O
 * in the common case (the prior value is hot in the in-memory layered chain) and eliminates any
 * need for worker threads to access the layered storage.
 *
 * <p>Capture rule: a block {@code b} is captured iff the storage is the live storage or the
 * registered roller storage, {@code b} directly follows the last indexed block (or {@code b == 0}
 * when nothing is indexed), and {@code b == 0 || b <= frontier}. Live-storage captures commit
 * inline with the import transaction; roller-storage captures commit detached into canonical
 * storage.
 */
public class ArchiveTrieNodeStrategy implements TrieNodeStrategy, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveTrieNodeStrategy.class);

  /**
   * The capture decision for one transaction; block number and capture decision are constant within
   * it.
   */
  private record TxCapture(long block, boolean capture, ArchiveTrieNodeWriter.CommitMode mode) {}

  private final TrieNodeStrategy base;
  private final ArchiveTrieNodeWriter trieNodeWriter;
  private final SegmentedKeyValueStorage liveStorage;
  private final ArchiveCoverageTracker coverageTracker;
  private final AtomicReference<LongSupplier> frontier = new AtomicReference<>();
  private final AtomicReference<SegmentedKeyValueStorage> rollerStorage = new AtomicReference<>();

  private final Cache<SegmentedKeyValueStorageTransaction, TxCapture> txCaptureCache =
      Caffeine.newBuilder().weakKeys().build();

  /**
   * Builds an archiving strategy over {@code liveStorage}, owning a worker pool for async
   * history-entry computation.
   *
   * @param liveStorage the live world-state storage to archive writes from
   * @param trieCapturePool the worker pool, owned and shut down by the returned strategy
   * @param shallowCheckpointInterval FULL-checkpoint interval for shallow nodes
   * @param deepCheckpointInterval FULL-checkpoint interval for deep nodes
   * @return an archiving {@link TrieNodeStrategy} ready to install via {@code setTrieNodeStrategy}
   */
  public static ArchiveTrieNodeStrategy createArchiveStrategy(
      final SegmentedKeyValueStorage liveStorage,
      final ExecutorService trieCapturePool,
      final int shallowCheckpointInterval,
      final int deepCheckpointInterval) {
    final ArchiveCoverageTracker coverageTracker = new ArchiveCoverageTracker(liveStorage);
    return new ArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(),
        new ArchiveTrieNodeWriter(
            new ArchiveNodeHistoryStore(liveStorage),
            coverageTracker,
            trieCapturePool,
            shallowCheckpointInterval,
            deepCheckpointInterval),
        liveStorage,
        coverageTracker);
  }

  /**
   * @param base the delegate strategy for the live trie
   * @param trieNodeWriter the writer that persists archived history entries
   * @param liveStorage the canonical head storage, whose commits are captured inline
   * @param coverageTracker the tracker of indexed blocks
   */
  @VisibleForTesting
  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveTrieNodeWriter trieNodeWriter,
      final SegmentedKeyValueStorage liveStorage,
      final ArchiveCoverageTracker coverageTracker) {
    this.base = Objects.requireNonNull(base);
    this.trieNodeWriter = Objects.requireNonNull(trieNodeWriter);
    this.liveStorage = Objects.requireNonNull(liveStorage);
    this.coverageTracker = Objects.requireNonNull(coverageTracker);
  }

  /** Arms the capture frontier; until then any non-genesis capture decision fails loudly. */
  public void armFrontier(final LongSupplier frontier) {
    this.frontier.set(Objects.requireNonNull(frontier));
  }

  /**
   * Marks {@code storage} as the roller's scratch storage: persists on it are captured and their
   * history is committed to canonical storage, detached from the scratch transaction.
   *
   * @param storage the roller's scratch storage
   * @throws IllegalStateException if a roller storage is already registered
   */
  public void registerRollerStorage(final SegmentedKeyValueStorage storage) {
    if (!rollerStorage.compareAndSet(null, Objects.requireNonNull(storage))) {
      throw new IllegalStateException("a roller storage is already registered");
    }
  }

  /**
   * Unregisters the roller's scratch storage, if it is the registered one.
   *
   * @param storage the storage to unregister
   */
  public void unregisterRollerStorage(final SegmentedKeyValueStorage storage) {
    if (!rollerStorage.compareAndSet(storage, null)) {
      LOG.warn("unregisterRollerStorage called with a storage that is not the registered one");
    }
  }

  /**
   * Returns the coverage tracker.
   *
   * @return the coverage tracker
   */
  public ArchiveCoverageTracker getCoverageTracker() {
    return coverageTracker;
  }

  // Capture rule: only the canonical head storage or the registered roller storage, only the block
  // directly after the last indexed one (genesis when nothing is indexed), and only at or below
  // the frontier. Everything else (RPC and cache layers, reorg rolls) is never captured.
  //
  // Invariant: the live and roller paths never target the same block. The roller's target is at or
  // below the local head, while a live capture requires last == head, so the decision cached at
  // the first put stays valid even though record() re-reads coverage at commit.
  private TxCapture decide(final SegmentedKeyValueStorage storage) {
    final long block = readBlockNumber(storage);
    final boolean isRoller = storage == rollerStorage.get();
    if (!isRoller && storage != liveStorage) {
      return new TxCapture(block, false, ArchiveTrieNodeWriter.CommitMode.INLINE);
    }
    final OptionalLong last = coverageTracker.lastIndexedBlock();
    final boolean isNext = last.isPresent() ? block == last.getAsLong() + 1L : block == 0L;
    final boolean capture = isNext && (block == 0L || block <= frontier());
    return new TxCapture(
        block,
        capture,
        isRoller
            ? ArchiveTrieNodeWriter.CommitMode.DETACHED
            : ArchiveTrieNodeWriter.CommitMode.INLINE);
  }

  private long frontier() {
    return Objects.requireNonNull(frontier.get(), "archive frontier consulted before arming")
        .getAsLong();
  }

  private static long readBlockNumber(final SegmentedKeyValueStorage storage) {
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(b -> Bytes.wrap(b).toLong() + 1L)
        .orElse(0L);
  }

  // Bonsai re-commits unchanged nodes along a touched path; skip archiving them to avoid
  // inflating the diff-chain counter. Creations (prior == null) are never no-ops.
  private boolean isNoOpRewrite(final Bytes prior, final Bytes node) {
    return prior != null && prior.equals(node);
  }

  @Override
  public Optional<Bytes> getTrieNode(
      final SegmentedKeyValueStorage storage, final Bytes key, final Bytes32 nodeHash) {
    return base.getTrieNode(storage, key, nodeHash);
  }

  @Override
  public void putTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes key,
      final Bytes32 nodeHash,
      final Bytes node) {
    final TxCapture txCapture = txCaptureCache.get(transaction, ignored -> decide(storage));
    base.putTrieNode(storage, transaction, key, nodeHash, node);
    if (txCapture.capture()) {
      final Bytes prior = readPriorNode(storage, key, txCapture.block());
      if (isNoOpRewrite(prior, node)) {
        return;
      }
      capture(key, txCapture, node, prior, transaction);
    }
  }

  @Override
  public void removeTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes key) {
    final TxCapture txCapture = txCaptureCache.get(transaction, ignored -> decide(storage));
    base.removeTrieNode(storage, transaction, key);
    if (txCapture.capture()) {
      final Bytes prior = readPriorNode(storage, key, txCapture.block());
      // Removing a node that doesn't exist is a no-op for the archive.
      if (prior != null) {
        capture(key, txCapture, null, prior, transaction);
      }
    }
  }

  // Genesis (block 0) has no prior node by definition.
  private Bytes readPriorNode(
      final SegmentedKeyValueStorage storage, final Bytes key, final long block) {
    return block == 0L ? null : base.getTrieNode(storage, key, Bytes32.ZERO).orElse(null);
  }

  private void capture(
      final Bytes key,
      final TxCapture txCapture,
      final Bytes node,
      final Bytes prior,
      final SegmentedKeyValueStorageTransaction transaction) {
    trieNodeWriter.capture(
        ArchiveNodeKey.of(key),
        TrieNodeKey.location(key),
        txCapture.block(),
        node,
        prior,
        transaction,
        txCapture.mode());
  }

  @Override
  public void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    txCaptureCache.invalidate(transaction);
    trieNodeWriter.onBeforeCommit(transaction);
  }

  @Override
  public void onBeforeBlockCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final long blockNumber) {
    // A block persist records coverage even if it wrote no captured nodes (empty blocks, no-op
    // rewrites), otherwise coverage would freeze at the first such block.
    final TxCapture txCapture = txCaptureCache.get(transaction, ignored -> decide(storage));
    txCaptureCache.invalidate(transaction);
    if (txCapture.capture() && txCapture.block() == blockNumber) {
      trieNodeWriter.beginBlock(transaction, txCapture.block(), txCapture.mode());
    } else if (txCapture.capture()) {
      // e.g. a rollFullWorldStateToBlockHash persist merging rollbacks and roll-forwards: the write
      // set is not the content of any single block, so it must never be archived.
      LOG.debug(
          "archive decided block {} but persisting block {}; not archiving",
          txCapture.block(),
          blockNumber);
      trieNodeWriter.onRollback(transaction);
      return;
    }
    trieNodeWriter.onBeforeCommit(transaction);
  }

  @Override
  public void onRollback(final SegmentedKeyValueStorageTransaction transaction) {
    txCaptureCache.invalidate(transaction);
    trieNodeWriter.onRollback(transaction);
  }

  @Override
  public void close() {
    trieNodeWriter.close();
  }
}
