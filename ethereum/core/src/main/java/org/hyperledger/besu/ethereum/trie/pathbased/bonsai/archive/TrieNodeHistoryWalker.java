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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.common.StateRootMismatchException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.Closeable;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A trailing walker that processes historical blocks to populate the trie-node history archive.
 *
 * <p>The walker subscribes to new canonical head events and chases the chain head, staying outside
 * the reorg window ({@code chainHead - maxLayersToLoad}). For each block M in the safe window it:
 *
 * <ol>
 *   <li>Sets the archive strategy for block M on the walker world state
 *   <li>Rolls forward the trie log for block M into the accumulator
 *   <li>Persists the world state with the block header (verifying the state root)
 *   <li>Updates and durably saves the history progress to storage
 * </ol>
 *
 * <p>On {@link StateRootMismatchException}, the walker halts and logs at ERROR level. This is the
 * compensating control for the removed flat-DB validation — continuing after a mismatch would write
 * corrupt history.
 *
 * <p>The lifecycle mirrors {@link BonsaiFlatDbToArchiveMigrator}'s ongoing-migration half: {@code
 * start()} registers a blockchain observer; each canonical-head event updates the target and
 * schedules a single-flight catch-up task; {@code close()} deregisters the observer and shuts down
 * the executor.
 */
public class TrieNodeHistoryWalker implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TrieNodeHistoryWalker.class);
  private static final int LOG_INTERVAL_SECONDS = 60;

  private final TrieNodeHistoryWalkerWorldState walkerWorldState;
  private final TrieLogManager realTrieLogManager;
  private final Blockchain blockchain;
  private final TrieNodeHistoryProgress historyProgress;
  private final SegmentedKeyValueStorage composedWorldStateStorage;
  private final ExecutorService executorService;
  private final GenesisState genesisState;

  protected final AtomicLong walkedBlockNumber =
      new AtomicLong(TrieNodeHistoryProgress.UNSET_LAST_INDEXED);
  protected final AtomicLong ongoingTarget = new AtomicLong(0);
  protected final AtomicBoolean catchUpRunning = new AtomicBoolean(false);
  protected final AtomicBoolean halted = new AtomicBoolean(false);
  private final AtomicBoolean shouldLogCatchUp = new AtomicBoolean(true);
  protected volatile OptionalLong blockObserverId = OptionalLong.empty();
  private boolean closed = false;

  /**
   * Creates a new {@code TrieNodeHistoryWalker}.
   *
   * @param walkerWorldState the isolated world state used to replay trie logs (Task 4)
   * @param realTrieLogManager the node's real trie-log manager, used to read each block's trie log
   * @param blockchain the blockchain, used to look up block headers and observe new heads
   * @param historyProgress persisted progress tracker; updated atomically with each block's history
   *     data
   * @param composedWorldStateStorage the node's live composed storage; used only to durably persist
   *     the progress record alongside each block's history writes
   * @param executorService the executor on which catch-up tasks run; caller owns its lifecycle
   * @param genesisState the genesis state, used to bootstrap history for non-empty genesis chains
   */
  public TrieNodeHistoryWalker(
      final TrieNodeHistoryWalkerWorldState walkerWorldState,
      final TrieLogManager realTrieLogManager,
      final Blockchain blockchain,
      final TrieNodeHistoryProgress historyProgress,
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final ExecutorService executorService,
      final GenesisState genesisState) {
    this.walkerWorldState = walkerWorldState;
    this.realTrieLogManager = realTrieLogManager;
    this.blockchain = blockchain;
    this.historyProgress = historyProgress;
    this.composedWorldStateStorage = composedWorldStateStorage;
    this.executorService = executorService;
    this.genesisState = genesisState;
  }

  /**
   * Starts the walker. Registers a canonical-head observer on the blockchain and schedules an
   * initial catch-up if there are already blocks outside the reorg window that have not been
   * processed.
   *
   * <p>Idempotent: calling {@code start()} a second time while already running is a no-op.
   */
  public synchronized void start() {
    if (closed) {
      LOG.debug("TrieNodeHistoryWalker.start called after close; skipping");
      return;
    }
    if (blockObserverId.isPresent()) {
      LOG.debug("TrieNodeHistoryWalker.start called while already running; skipping");
      return;
    }

    // Initialise from persisted progress; UNSET_LAST_INDEXED (-1) means nothing walked yet.
    walkedBlockNumber.set(historyProgress.lastIndexedBlock());

    // Align the walker world-state's starting root hash.
    //
    // BonsaiWorldState reads its initial worldStateRootHash from storage at construction time.
    // The walker's storage is HistoryOnlyWriteStorage, which forwards reads to the live composed
    // storage — so without this reset the walker would inherit the current chain-head state root
    // (e.g. block 3) instead of the correct starting root (genesis or last indexed block).
    // resetWorldStateTo() overwrites the in-memory field without touching storage.
    final long startBlockForRoot =
        (walkedBlockNumber.get() == TrieNodeHistoryProgress.UNSET_LAST_INDEXED)
            ? 0L
            : walkedBlockNumber.get();
    blockchain
        .getBlockHeader(startBlockForRoot)
        .ifPresent(walkerWorldState.getWorldState()::resetWorldStateTo);

    blockObserverId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (!event.isNewCanonicalHead()) {
                    return;
                  }
                  final long newTarget = walkerTarget(event.getHeader().getNumber());
                  if (newTarget <= 0) {
                    return;
                  }
                  ongoingTarget.accumulateAndGet(newTarget, Math::max);
                  scheduleCatchUpIfNeeded();
                }));

    // Schedule initial catch-up for blocks already beyond the reorg window.
    final long currentHead = blockchain.getChainHeadBlockNumber();
    final long initialTarget = walkerTarget(currentHead);
    if (initialTarget > walkedBlockNumber.get()) {
      ongoingTarget.accumulateAndGet(initialTarget, Math::max);
      scheduleCatchUpIfNeeded();
    }
  }

  private void scheduleCatchUpIfNeeded() {
    if (halted.get()) {
      return;
    }
    if (!catchUpRunning.compareAndSet(false, true)) {
      return;
    }
    try {
      executorService.submit(this::catchUp);
    } catch (final RejectedExecutionException e) {
      catchUpRunning.set(false);
      LOG.debug(
          "Trie node history walker executor shut down; skipping walk up to block {}",
          ongoingTarget.get());
    }
  }

  private void catchUp() {
    try {
      final long startBlock = walkedBlockNumber.get() + 1;
      final long targetSnapshot = ongoingTarget.get();
      if (startBlock > targetSnapshot) {
        return;
      }
      LogUtil.throttledLog(
          () ->
              LOG.info(
                  "Trie node history walker catch-up: blocks {} to {}", startBlock, targetSnapshot),
          shouldLogCatchUp,
          LOG_INTERVAL_SECONDS);
      for (long blockNumber = startBlock; blockNumber <= ongoingTarget.get(); blockNumber++) {
        if (halted.get()) {
          return;
        }
        try {
          processBlock(blockNumber);
          final long bn = blockNumber;
          final long target = ongoingTarget.get();
          LogUtil.throttledLog(
              () -> LOG.info("Trie node history walker progress: block {}/{}", bn, target),
              shouldLogCatchUp,
              LOG_INTERVAL_SECONDS);
        } catch (final StateRootMismatchException e) {
          LOG.error(
              "Trie node history walker halted at block {}: state root mismatch "
                  + "(expected {}, got {}). History is invalid for blocks beyond {}.",
              blockNumber,
              e.getExpectedRoot(),
              e.getActualRoot(),
              walkedBlockNumber.get());
          halted.set(true);
          return;
        } catch (final IllegalStateException e) {
          LOG.error(
              "Trie node history walker halted at block {} due to unrecoverable error",
              blockNumber,
              e);
          halted.set(true);
          return;
        } catch (final Exception e) {
          LOG.error(
              "Trie node history walker halted at block {} due to unexpected error",
              blockNumber,
              e);
          halted.set(true);
          return;
        }
      }
    } finally {
      catchUpRunning.set(false);
      if (!halted.get() && walkedBlockNumber.get() < ongoingTarget.get()) {
        scheduleCatchUpIfNeeded();
      }
    }
  }

  private void processBlock(final long blockNumber) {
    final BlockHeader header =
        blockchain
            .getBlockHeader(blockNumber)
            .orElseThrow(
                () -> new IllegalStateException("No block header for block " + blockNumber));

    final Optional<TrieLog> maybeTrieLog = realTrieLogManager.getTrieLogLayer(header.getHash());
    if (maybeTrieLog.isEmpty()) {
      if (blockNumber > 0) {
        throw new IllegalStateException("No trie log for block " + blockNumber);
      }
      // Block 0 (genesis) may have no trie log — there is no prior state to diff against.
      // For non-empty genesis, bootstrap all genesis trie nodes into history at block 0 so
      // that block 1's priorVersionOf(key, 0) reads return correct genesis values.
      if (!Hash.EMPTY_TRIE_HASH.equals(header.getStateRoot())) {
        bootstrapGenesis();
      }
      // Persist progress so a restart does not re-walk genesis in a tight loop.
      historyProgress.setLastIndexedBlock(0);
      historyProgress.setIndexStartBlock(0);
      final SegmentedKeyValueStorageTransaction genesisTx =
          composedWorldStateStorage.startTransaction();
      historyProgress.save(genesisTx);
      genesisTx.commit();
      walkedBlockNumber.set(0);
      return;
    }
    final TrieLog trieLog = maybeTrieLog.get();

    walkerWorldState.setStrategyForBlock(blockNumber);
    // Block 0's genesis trie nodes don't exist in live storage (superseded by chain head).
    // Start from an empty trie so persist() builds it from scratch.
    if (blockNumber == 0) {
      walkerWorldState.getWorldState().resetWorldStateRootHash(Hash.EMPTY_TRIE_HASH);
    }
    ((PathBasedWorldStateUpdateAccumulator<?>) walkerWorldState.getWorldState().updater())
        .rollForward(trieLog);
    walkerWorldState
        .getWorldState()
        .persist(header); // throws StateRootMismatchException on divergence

    historyProgress.setLastIndexedBlock(blockNumber);
    historyProgress.setIndexStartBlock(blockNumber);
    final SegmentedKeyValueStorageTransaction tx = composedWorldStateStorage.startTransaction();
    historyProgress.save(tx);
    tx.commit();

    walkedBlockNumber.set(blockNumber);
  }

  /**
   * Bootstraps genesis trie nodes into the history archive at block 0.
   *
   * <p>Genesis has no trie log, so the walker cannot use its normal rollForward path. Instead this
   * method drives the genesis state directly through the walker world state:
   *
   * <ol>
   *   <li>Installs the block-0 {@link BonsaiArchiveTrieNodeStrategy}, which returns {@code
   *       Optional.empty()} for all prior-value reads (there is no prior state before genesis).
   *   <li>Resets the accumulator so no stale chain-head data contaminates genesis derivation.
   *   <li>Calls {@link GenesisState#writeStateTo} on the walker world state. {@code writeStateTo}
   *       creates all genesis accounts in the accumulator then calls {@code
   *       persist(genesisHeader)}, which builds the genesis MPT from scratch and — via the block-0
   *       strategy — captures every genesis trie node as a CREATION history entry in {@code
   *       TRIE_NODE_HISTORY_ARCHIVE}.
   *   <li>Resets the accumulator again so block 1's {@code rollForward} starts clean.
   * </ol>
   *
   * <p>If {@code persist()} throws {@link StateRootMismatchException}, the genesis derivation
   * produced the wrong state root; the exception propagates and the walker halts via the normal
   * catch in {@link #catchUp}.
   */
  private void bootstrapGenesis() {
    LOG.info("Bootstrapping genesis trie nodes into history archive (one-time initialization)…");
    walkerWorldState.setStrategyForBlock(0L);
    final PathBasedWorldStateUpdateAccumulator<?> accumulator =
        (PathBasedWorldStateUpdateAccumulator<?>) walkerWorldState.getWorldState().updater();
    accumulator.reset();
    // The walker world state's root was reset to the genesis block's state root in start(), but
    // persist() needs to BUILD the genesis trie from scratch (those nodes no longer exist in live
    // storage — that's the whole point of this archive). Starting from EMPTY_TRIE_HASH tells
    // BonsaiWorldState to construct the MPT fresh rather than trying to load non-existent nodes.
    walkerWorldState.getWorldState().resetWorldStateRootHash(Hash.EMPTY_TRIE_HASH);
    genesisState.writeStateTo(walkerWorldState.getWorldState());
    // Reset so block 1's rollForward starts from a clean accumulator.
    accumulator.reset();
    LOG.info("Genesis trie node bootstrap complete.");
  }

  private long walkerTarget(final long blockNumber) {
    return Math.max(0L, blockNumber - realTrieLogManager.getMaxLayersToLoad());
  }

  /**
   * Returns the block number of the most recently successfully processed block, or {@link
   * TrieNodeHistoryProgress#UNSET_LAST_INDEXED} if no blocks have been processed yet.
   *
   * @return the highest block number walked so far
   */
  public long getWalkedBlockNumber() {
    return walkedBlockNumber.get();
  }

  @Override
  public synchronized void close() {
    closed = true;
    blockObserverId.ifPresent(blockchain::removeObserver);
    blockObserverId = OptionalLong.empty();
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Trie node history walker executor did not terminate within 10 seconds");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
