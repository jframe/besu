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
package org.hyperledger.besu.cli.subcommands.storage;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.NoOpBonsaiCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.CalibrationResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.RecordingTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.CacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Produces a calibration file measuring the <em>real</em> per-depth FULL/DIFF entry sizes of the
 * archive trie-node write path, by replaying a recent slice of the chain through {@link
 * RecordingTrieNodeStrategy}. The resulting {@link CalibrationResult} feeds {@code
 * x-trie-node-history-estimate} in place of its embedded hoodi defaults.
 *
 * <p><strong>This subcommand mutates the datadir it is pointed at.</strong> It rolls the head world
 * state backward {@code --blocks} blocks, then forward again. Point {@code --data-path} at a
 * <em>disposable copy</em> of a stopped node's datadir — never a live node's datadir. Running
 * against a datadir still held by a live besu process fails when opening RocksDB (the node holds
 * the write lock).
 *
 * <p>The replay proceeds in three phases: (1) roll the real head world state backward to {@code
 * head - blocks} one block at a time via trie logs; (2) verify the resulting state root matches the
 * historical header at that block, aborting on divergence; (3) roll forward again through a
 * recording strategy, capturing every trie-node write's FULL and DIFF encoded size.
 */
@Command(
    name = "x-trie-node-history-calibrate",
    description =
        "Measure real per-depth archive trie-node entry sizes by replaying a recent chain slice on"
            + " a DISPOSABLE COPY of a stopped node's datadir, writing a calibration file for"
            + " x-trie-node-history-estimate",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class TrieNodeHistoryCalibrateSubCommand implements Runnable {

  // Below this many recorded writes at a depth, the per-depth mean is statistically shaky and the
  // operator is warned that the calibration slice may be too short to trust for that depth.
  private static final long MIN_SAMPLES_PER_DEPTH = 100L;

  @SuppressWarnings("unused")
  @ParentCommand
  private StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Option(
      names = {"--blocks"},
      required = true,
      description =
          "Number of recent blocks to roll back and replay (the calibration slice length). Larger"
              + " slices give more per-depth samples but take longer.")
  private long blocks;

  @Option(
      names = {"--output"},
      required = true,
      description = "Path to write the calibration JSON")
  private Path output;

  @Option(
      names = {"--log-interval-seconds"},
      description =
          "Print a progress line roughly every N seconds; 0 disables progress (default:"
              + " ${DEFAULT-VALUE})")
  private long logIntervalSeconds = 60;

  /** Default constructor. */
  public TrieNodeHistoryCalibrateSubCommand() {}

  @Override
  public void run() {
    final java.io.PrintWriter out = spec.commandLine().getOut();

    final BesuController controller;
    try {
      controller = parentCommand.besuCommand.buildController();
    } catch (final RuntimeException e) {
      throw new IllegalStateException(
          "Failed to open the datadir for calibration. This subcommand MUTATES the datadir, so"
              + " point --data-path at a disposable COPY of a stopped node's datadir, not a live"
              + " node's datadir (a running besu holds the RocksDB write lock). Underlying error: "
              + e.getMessage(),
          e);
    }

    final WorldStateArchive worldStateArchive =
        controller.getProtocolContext().getWorldStateArchive();
    if (!(worldStateArchive instanceof BonsaiWorldStateProvider archive)) {
      throw new IllegalStateException(
          "Calibration requires a Bonsai (path-based) datadir, but found "
              + worldStateArchive.getClass().getSimpleName());
    }
    final MutableBlockchain blockchain = controller.getProtocolContext().getBlockchain();
    final TrieLogManager trieLogManager = archive.getTrieLogManager();

    final long head = blockchain.getChainHeadBlockNumber();
    final long target = head - blocks;
    if (blocks <= 0) {
      throw new IllegalArgumentException("--blocks must be positive, was " + blocks);
    }
    if (target < 1) {
      throw new IllegalArgumentException(
          "--blocks="
              + blocks
              + " exceeds available history: chain head is block "
              + head
              + ", so the earliest replayable slice length is "
              + (head - 1)
              + " (block 0 is genesis and has no trie log)");
    }

    out.printf(
        "Calibrating on a %,d-block slice: rolling head (block %,d) back to block %,d, then"
            + " replaying forward through the recording strategy...%n",
        blocks, head, target);
    out.println(
        "NOTE: this mutates the datadir in place; it must be a disposable copy of a stopped node.");
    out.flush();

    final PathBasedWorldState headWorldState = (PathBasedWorldState) archive.getWorldState();
    final AtomicLong rollbackProgress = new AtomicLong();
    final Thread rollbackReporter =
        startProgressReporter(out, "rollback", rollbackProgress, blocks);
    try {
      rollBackTo(headWorldState, blockchain, trieLogManager, head, target, rollbackProgress);
    } finally {
      stopReporter(rollbackReporter);
    }
    verifyStateRoot(headWorldState, blockchain, target);
    out.printf("Rolled back and verified state root at block %,d.%n", target);
    out.flush();

    final RecordingTrieNodeStrategy recorder =
        new RecordingTrieNodeStrategy(new BonsaiTrieNodeStrategy());
    final BonsaiWorldState recordingWorldState =
        buildRecordingWorldState(archive, new NoOpMetricsSystem(), recorder);
    final AtomicLong forwardProgress = new AtomicLong();
    final Thread forwardReporter =
        startProgressReporter(out, "forward replay", forwardProgress, blocks);
    final CalibrationResult result;
    try {
      result =
          replayForward(
              recordingWorldState,
              blockchain,
              trieLogManager,
              target,
              head,
              recorder,
              forwardProgress);
    } finally {
      stopReporter(forwardReporter);
    }

    warnOnSparseCoverage(out, result);

    result.writeTo(output);
    out.println("Wrote calibration to " + output);
    out.flush();
  }

  /**
   * Rolls {@code worldState} backward from block {@code head} to block {@code target} one block at
   * a time, applying each block's trie log in reverse and persisting against the parent header.
   * Package-private for testing. {@code persist} self-verifies each intermediate root against the
   * historical header, so a trie-log/flat-DB divergence surfaces here rather than silently.
   */
  static void rollBackTo(
      final PathBasedWorldState worldState,
      final MutableBlockchain blockchain,
      final TrieLogManager trieLogManager,
      final long head,
      final long target,
      final AtomicLong progress) {
    for (long n = head; n > target; n--) {
      final BlockHeader header = headerOrThrow(blockchain, n);
      final TrieLog trieLog =
          trieLogManager
              .getTrieLogLayer(header.getHash())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "No trie log stored for block "
                              + header.getNumber()
                              + "; cannot roll back. The datadir may have pruned trie logs below the"
                              + " historical block limit — reduce --blocks or use a datadir with more"
                              + " retained trie logs."));
      final BlockHeader parent = headerOrThrow(blockchain, n - 1);
      ((PathBasedWorldStateUpdateAccumulator<?>) worldState.updater()).rollBack(trieLog);
      worldState.persist(parent);
      progress.incrementAndGet();
    }
  }

  /**
   * Confirms {@code worldState}'s current root hash equals the historical state root of block
   * {@code target}, throwing {@link IllegalStateException} on mismatch. Package-private for
   * testing.
   */
  static void verifyStateRoot(
      final PathBasedWorldState worldState, final MutableBlockchain blockchain, final long target) {
    final Hash expected = headerOrThrow(blockchain, target).getStateRoot();
    final Hash actual = worldState.rootHash();
    if (!actual.equals(expected)) {
      throw new IllegalStateException(
          "State root mismatch after rolling back to block "
              + target
              + ": expected "
              + expected
              + " from the block header but the rolled-back world state is at "
              + actual
              + ". The trie logs and flat database have diverged; aborting calibration to avoid"
              + " producing garbage measurements.");
    }
  }

  /**
   * Rolls {@code worldState} forward from block {@code from} (exclusive) to block {@code to}
   * (inclusive) one block at a time, applying each block's trie log and persisting against that
   * block's header. Every trie-node write is measured by {@code recorder}. Returns the accumulated
   * {@link CalibrationResult}. Package-private for testing.
   */
  static CalibrationResult replayForward(
      final BonsaiWorldState worldState,
      final MutableBlockchain blockchain,
      final TrieLogManager trieLogManager,
      final long from,
      final long to,
      final RecordingTrieNodeStrategy recorder,
      final AtomicLong progress) {
    for (long n = from + 1; n <= to; n++) {
      final BlockHeader header = headerOrThrow(blockchain, n);
      final TrieLog trieLog =
          trieLogManager
              .getTrieLogLayer(header.getHash())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "No trie log stored for block "
                              + header.getNumber()
                              + "; cannot replay forward."));
      ((PathBasedWorldStateUpdateAccumulator<?>) worldState.updater()).rollForward(trieLog);
      worldState.persist(header);
      progress.incrementAndGet();
    }
    return recorder.result();
  }

  /**
   * Builds a {@link BonsaiWorldState} over the archive's existing composed storage but with its
   * trie-node writes routed through {@code recorder}. Mirrors the migrator's dedicated-world-state
   * construction: a fresh {@link BonsaiWorldStateKeyValueStorage} over the same flat DB, wired with
   * NoOp cache / trie-log managers so the replay neither caches layers nor rewrites trie logs.
   * Package-private for testing.
   */
  static BonsaiWorldState buildRecordingWorldState(
      final BonsaiWorldStateProvider archive,
      final MetricsSystem metricsSystem,
      final RecordingTrieNodeStrategy recorder) {
    final PathBasedWorldStateKeyValueStorage realStorage = archive.getWorldStateKeyValueStorage();
    final SegmentedKeyValueStorage composedStorage = realStorage.getComposedWorldStateStorage();
    final KeyValueStorage trieLogStorage = realStorage.getTrieLogStorage();

    final DataStorageConfiguration dataStorageConfiguration =
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;
    final BonsaiFlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    flatDbStrategyProvider.loadFlatDbStrategy(composedStorage);

    final BonsaiWorldStateKeyValueStorage recordingStorage =
        new BonsaiWorldStateKeyValueStorage(
            flatDbStrategyProvider,
            composedStorage,
            trieLogStorage,
            CacheManager.NO_OP_CACHE,
            0L,
            recorder);

    final CodeCache codeCache = new CodeCache();
    return new BonsaiWorldState(
        recordingStorage,
        new NoopBonsaiCachedMerkleTrieLoader(),
        new NoOpBonsaiCachedWorldStorageManager(
            recordingStorage, EvmConfiguration.DEFAULT, codeCache),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        WorldStateConfig.newBuilder(WorldStateConfig.createStatefulConfigWithTrie())
            .parallelStateRootComputationEnabled(false)
            .build(),
        codeCache);
  }

  private void warnOnSparseCoverage(final java.io.PrintWriter out, final CalibrationResult result) {
    final long[] writesByDepth = result.writesByDepth();
    final double[] fullBranch = result.fullBranchBytesByDepth();
    final double[] fullShort = result.fullShortBytesByDepth();
    int maxObservedDepth = -1;
    long totalWrites = 0;
    for (int d = 0; d < writesByDepth.length; d++) {
      totalWrites += writesByDepth[d];
      if (writesByDepth[d] > 0) {
        maxObservedDepth = d;
      }
    }
    if (totalWrites == 0) {
      out.println(
          "WARNING: no trie-node writes were recorded during calibration. The slice contained no"
              + " state changes; the calibration file will be all zeros and must not be used.");
      return;
    }
    for (int d = 0; d <= maxObservedDepth; d++) {
      if (writesByDepth[d] == 0) {
        out.printf(
            "WARNING: depth %d had no observed writes within the calibration slice; its entry-size"
                + " estimate will fall back to 0. Consider a larger --blocks slice.%n",
            d);
      } else if (writesByDepth[d] < MIN_SAMPLES_PER_DEPTH) {
        out.printf(
            "WARNING: depth %d has only %,d observed writes (< %,d); its mean entry size"
                + " (FULL branch=%.1f, FULL short=%.1f bytes) may be unreliable. Consider a larger"
                + " --blocks slice.%n",
            d, writesByDepth[d], MIN_SAMPLES_PER_DEPTH, fullBranch[d], fullShort[d]);
      }
    }
    out.flush();
  }

  private Thread startProgressReporter(
      final java.io.PrintWriter out,
      final String phaseName,
      final AtomicLong processed,
      final long total) {
    if (logIntervalSeconds <= 0) {
      return null;
    }
    final long startNanos = System.nanoTime();
    final Thread t =
        new Thread(
            () -> {
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  Thread.sleep(logIntervalSeconds * 1000L);
                  final long done = processed.get();
                  final double elapsedSec = (System.nanoTime() - startNanos) / 1e9;
                  final double rate = elapsedSec > 0 ? done / elapsedSec : 0;
                  out.printf(
                      "  %s: %,d / %,d blocks (%.1f%%), %,.0f blocks/s%n",
                      phaseName, done, total, total > 0 ? 100.0 * done / total : 0.0, rate);
                  out.flush();
                }
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            },
            "history-calibrate-progress");
    t.setDaemon(true);
    t.start();
    return t;
  }

  private static void stopReporter(final Thread reporter) {
    if (reporter != null) {
      reporter.interrupt();
    }
  }

  private static BlockHeader headerOrThrow(
      final MutableBlockchain blockchain, final long blockNumber) {
    return blockchain
        .getBlockHeader(blockNumber)
        .orElseThrow(() -> new IllegalStateException("No block header for block " + blockNumber));
  }
}
