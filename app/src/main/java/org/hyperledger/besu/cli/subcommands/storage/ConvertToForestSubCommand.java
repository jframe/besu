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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.forest.migration.BonsaiTrieLogToForestConverter;
import org.hyperledger.besu.ethereum.trie.forest.migration.ForestConversionResume;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.OptimisticRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * The {@code x-convert-to-forest} storage subcommand. Converts an existing Bonsai full-sync
 * database into a Forest database by replaying its trie logs block-by-block, reconstructing the
 * Forest world-state trie nodes, and flipping the database metadata to the Forest format.
 *
 * <p>The conversion uses a single storage handle. The Forest {@code WORLD_STATE} column family does
 * not exist in a BONSAI database, so it is first created by briefly opening a raw RocksDB store
 * that lists {@code WORLD_STATE} among its (non-ignorable) segments; the columnar storage creates
 * missing column families on open. That raw store is closed immediately (a raw columnar {@code
 * close()} releases the RocksDB lock). A single {@link BesuController} is then built: because
 * {@code WORLD_STATE} now physically exists, the format-gated factory opens it as a cross-format
 * (ignorable) column family even though the metadata still says BONSAI. Everything needed for the
 * conversion (protocol schedule, genesis state, blockchain, trie logs, and the Forest world-state
 * writer) is obtained from that one controller, which is only closed once the conversion finishes.
 */
@Command(
    name = "x-convert-to-forest",
    description =
        "EXPERIMENTAL: convert a Bonsai full-sync database to a Forest database by replaying trie logs",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class ConvertToForestSubCommand implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ConvertToForestSubCommand.class);

  private static final int LOG_INTERVAL_SECONDS = 60;

  // Progress "blocks/s" is reported as a trailing average over this window rather than over the
  // whole run, so the figure reflects current throughput (and drops visibly during stalls) instead
  // of being smoothed away by the entire run's history.
  private static final long RATE_WINDOW_MILLIS = 5 * 60 * 1000L;

  private final AtomicBoolean shouldLogProgress = new AtomicBoolean(true);

  @CommandLine.Option(
      names = {"--Xx-convert-cache-size-mb"},
      description =
          "EXPERIMENTAL: on-heap size (MB) of the cross-block trie-node cache used during conversion; 0 disables it (default: ${DEFAULT-VALUE})",
      hidden = true)
  private long convertCacheSizeMb = 1024;

  @CommandLine.Option(
      names = {"--Xx-convert-prefetch-threads"},
      description =
          "EXPERIMENTAL: number of parallel reader threads used to warm the node cache ahead of replay, raising disk queue depth; 0 disables prefetch (default: ${DEFAULT-VALUE})",
      hidden = true)
  private int convertPrefetchThreads = 32;

  @CommandLine.Option(
      names = {"--Xx-convert-prefetch-window"},
      description =
          "EXPERIMENTAL: number of upcoming blocks whose changed keys are prefetched together before replay (default: ${DEFAULT-VALUE})",
      hidden = true)
  private int convertPrefetchWindow = 64;

  @CommandLine.Option(
      names = {"--Xx-convert-prefetch-lookahead"},
      description =
          "EXPERIMENTAL: number of windows to prefetch ahead of the apply phase; deeper lookahead keeps prefetch threads busy when apply takes longer than a single prefetch window (default: ${DEFAULT-VALUE})",
      hidden = true)
  private int convertPrefetchLookahead = 4;

  @SuppressWarnings("unused")
  @ParentCommand
  private StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec; // Picocli injects reference to command spec

  /** Default Constructor. */
  ConvertToForestSubCommand() {}

  private BesuController createBesuController() {
    final DataStorageConfiguration config = parentCommand.besuCommand.getDataStorageConfiguration();
    // disable limit trie logs so the full unpruned trie-log set is visible during conversion
    return parentCommand
        .besuCommand
        .setupControllerBuilder()
        .dataStorageConfiguration(
            ImmutableDataStorageConfiguration.copyOf(config)
                .withPathBasedExtraStorageConfiguration(
                    ImmutablePathBasedExtraStorageConfiguration.copyOf(
                            config.getPathBasedExtraStorageConfiguration())
                        .withLimitTrieLogsEnabled(false)))
        .build();
  }

  @Override
  public void run() {
    final Path dataDir = parentCommand.besuCommand.dataDir();
    final Path databaseDir = dataDir.resolve(BesuController.DATABASE_PATH);

    // Step 1: ensure the Forest WORLD_STATE column family physically exists on disk. A BONSAI
    // database does not have it, so the controller's factory would otherwise have nothing to open
    // for it. Briefly open a raw RocksDB store that lists WORLD_STATE among its (non-ignorable)
    // segments; the columnar storage creates missing column families on open. Close it immediately
    // (a raw columnar close() releases the RocksDB lock, unlike controller.close()).
    preCreateWorldStateColumnFamily(databaseDir);

    // Step 2: build the single controller and drive the entire conversion from it. WORLD_STATE now
    // exists, so the cross-format factory augmentation opens it even though the metadata says
    // BONSAI. The controller is held open until the conversion finishes.
    final BesuController controller = createBesuController();
    try {
      final DataStorageConfiguration config = controller.getDataStorageConfiguration();
      checkArgument(
          config.getDataStorageFormat() == DataStorageFormat.BONSAI,
          "x-convert-to-forest only supports source data-storage-format=BONSAI");

      final ProtocolSchedule protocolSchedule = controller.getProtocolSchedule();
      final GenesisState genesisState =
          GenesisState.fromConfig(
              config,
              parentCommand.besuCommand.getGenesisConfig(),
              protocolSchedule,
              new CodeCache());

      final Blockchain blockchain = controller.getProtocolContext().getBlockchain();

      final BonsaiWorldStateKeyValueStorage bonsai =
          (BonsaiWorldStateKeyValueStorage)
              controller.getStorageProvider().createWorldStateStorage(config);

      final ForestWorldStateKeyValueStorage forest =
          new ForestWorldStateKeyValueStorage(
              controller
                  .getStorageProvider()
                  .getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.WORLD_STATE));

      final BonsaiTrieLogToForestConverter converter =
          new BonsaiTrieLogToForestConverter(
              forest, convertCacheSizeMb * 1024L * 1024L, convertPrefetchThreads);
      final TrieLogFactoryImpl trieLogFactory = new TrieLogFactoryImpl();

      try {
        // Seed genesis so block-1 replay starts from the correct base state. Slot preimages are not
        // needed for correctness, so the controller's preimage storage is sufficient.
        final WorldStatePreimageStorage preimage =
            controller.getStorageProvider().createWorldStatePreimageStorage();
        converter.seedGenesis(genesisState, preimage, EvmConfiguration.DEFAULT);
        LOG.info("Seeded genesis state (root={})", converter.currentRootHash());

        final long head = blockchain.getChainHeadBlockNumber();

        final Hash genesisStateRoot = genesisState.getBlock().getHeader().getStateRoot();
        final LongFunction<Hash> stateRootByBlock =
            number ->
                number == 0
                    ? genesisStateRoot
                    : blockchain
                        .getBlockHeader(
                            blockchain
                                .getBlockHashByNumber(number)
                                .orElseThrow(
                                    () ->
                                        new IllegalStateException(
                                            "Missing block hash for block " + number)))
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    "Missing block header for block " + number))
                        .getStateRoot();

        final long resumeBlock =
            ForestConversionResume.findResumeBlock(
                head,
                stateRootByBlock,
                root -> forest.isWorldStateAvailable(Bytes32.wrap(root.getBytes())));
        if (resumeBlock > 0) {
          final Hash resumeRoot = stateRootByBlock.apply(resumeBlock);
          converter.resumeFrom(resumeRoot);
          LOG.info("Resuming conversion from block {} (root={})", resumeBlock, resumeRoot);
        }
        if (resumeBlock >= head) {
          LOG.info("Conversion already complete to head {}", head);
          flipMetadataToForest(dataDir);
          LOG.info("Flipped database metadata to FOREST format");
          return;
        }

        final long startMillis = System.currentTimeMillis();
        final long loopStartBlock = resumeBlock;

        // Trailing-window samples of (timestampMillis, blockNumber) backing the rolling blocks/s
        // rate. Seeded with the run's start so early logs (before the window fills) report the
        // since-start average, then transition smoothly to the trailing window.
        final Deque<long[]> rateSamples = new ArrayDeque<>();
        rateSamples.addLast(new long[] {startMillis, loopStartBlock});

        // Process the chain in windows, pipelining cache-warming of upcoming windows with the
        // single-threaded apply so the disk stays busy continuously. With convertPrefetchLookahead
        // windows in flight simultaneously, the prefetch threads stay active even when apply takes
        // much longer than a single prefetch window (the "pipeline inversion" problem).
        WindowData current =
            gatherWindow(
                blockchain,
                bonsai,
                trieLogFactory,
                resumeBlock + 1,
                Math.min(resumeBlock + convertPrefetchWindow, head));
        // Warm the first window synchronously from the (on-disk) resume root before replaying it.
        converter.prefetch(current.layers());

        // Seed the lookahead queue: submit prefetch for the next convertPrefetchLookahead windows
        // so the prefetch threads stay busy throughout the entire apply phase.
        final Deque<WindowData> windowQueue = new ArrayDeque<>();
        final Deque<Future<?>> prefetchQueue = new ArrayDeque<>();
        long nextToGather = current.startBlock() + current.layers().size();
        while (prefetchQueue.size() < convertPrefetchLookahead && nextToGather <= head) {
          final long end = Math.min(nextToGather + convertPrefetchWindow - 1, head);
          final WindowData upcoming =
              gatherWindow(blockchain, bonsai, trieLogFactory, nextToGather, end);
          windowQueue.addLast(upcoming);
          prefetchQueue.addLast(
              converter.prefetchAsync(upcoming.layers(), converter.currentRootHash()));
          nextToGather = upcoming.startBlock() + upcoming.layers().size();
        }

        while (current != null) {
          for (int i = 0; i < current.layers().size(); i++) {
            final long blockNumber = current.startBlock() + i;
            converter.applyTrieLog(current.layers().get(i), current.expectedRoots().get(i));

            LogUtil.throttledLog(
                () -> {
                  final long now = System.currentTimeMillis();
                  rateSamples.addLast(new long[] {now, blockNumber});
                  // Evict samples that have aged out of the trailing window, leaving the oldest
                  // sample still within RATE_WINDOW_MILLIS of now as the window's start point.
                  final long windowStart = now - RATE_WINDOW_MILLIS;
                  while (rateSamples.size() > 1 && rateSamples.peekFirst()[0] < windowStart) {
                    rateSamples.pollFirst();
                  }
                  final long[] windowOldest = rateSamples.peekFirst();
                  final double elapsedSeconds = Math.max((now - windowOldest[0]) / 1000.0, 0.001);
                  final double blocksPerSecond = (blockNumber - windowOldest[1]) / elapsedSeconds;
                  final double percentComplete = head > 0 ? (blockNumber * 100.0 / head) : 100.0;
                  final String eta =
                      blocksPerSecond > 0
                          ? formatDuration((long) ((head - blockNumber) / blocksPerSecond))
                          : "unknown";
                  LOG.info(
                      "Converted {} / {} blocks ({}%), {} blocks/s, ETA {}, cache hit-rate {} ({} entries)",
                      blockNumber,
                      head,
                      String.format("%.1f", percentComplete),
                      String.format("%.2f", blocksPerSecond),
                      eta,
                      converter.cacheHitRate() < 0
                          ? "disabled"
                          : String.format("%.1f%%", converter.cacheHitRate() * 100),
                      converter.cacheEstimatedSize());
                },
                shouldLogProgress,
                LOG_INTERVAL_SECONDS);
          }

          // Keep the lookahead queue full: submit one more window as we consume one.
          if (nextToGather <= head) {
            final long end = Math.min(nextToGather + convertPrefetchWindow - 1, head);
            final WindowData upcoming =
                gatherWindow(blockchain, bonsai, trieLogFactory, nextToGather, end);
            windowQueue.addLast(upcoming);
            prefetchQueue.addLast(
                converter.prefetchAsync(upcoming.layers(), converter.currentRootHash()));
            nextToGather = upcoming.startBlock() + upcoming.layers().size();
          }

          // Wait for the oldest in-flight prefetch to complete before advancing to the next window.
          if (!prefetchQueue.isEmpty()) {
            try {
              prefetchQueue.pollFirst().get();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (final ExecutionException e) {
              // Warming is best-effort; replay re-reads the authoritative node by hash on a miss.
              LOG.debug("Prefetch of upcoming window failed", e);
            }
          }

          current = windowQueue.pollFirst();
        }
        LOG.info("Conversion complete to head {} (root={})", head, converter.currentRootHash());

        flipMetadataToForest(dataDir);
        LOG.info("Flipped database metadata to FOREST format");
      } finally {
        converter.close();
      }
    } finally {
      controller.close();
    }

    spec.commandLine().getOut().println("x-convert-to-forest finished successfully.");
  }

  /** A window of consecutive blocks' deserialized trie logs and their expected post-state roots. */
  private record WindowData(long startBlock, List<TrieLog> layers, List<Hash> expectedRoots) {}

  /**
   * Reads and deserializes the trie logs for blocks {@code [start, end]} (inclusive), along with
   * their expected post-state roots, into an in-memory {@link WindowData}.
   *
   * @param blockchain the source blockchain
   * @param bonsai the Bonsai storage holding the trie logs
   * @param trieLogFactory factory used to deserialize trie logs
   * @param start the first block number in the window
   * @param end the last block number in the window
   * @return the gathered window
   */
  private WindowData gatherWindow(
      final Blockchain blockchain,
      final BonsaiWorldStateKeyValueStorage bonsai,
      final TrieLogFactoryImpl trieLogFactory,
      final long start,
      final long end) {
    final List<TrieLog> layers = new ArrayList<>();
    final List<Hash> expectedRoots = new ArrayList<>();
    for (long number = start; number <= end; number++) {
      final long blockNumber = number;
      final Hash blockHash =
          blockchain
              .getBlockHashByNumber(number)
              .orElseThrow(
                  () -> new IllegalStateException("Missing block hash for block " + blockNumber));
      final BlockHeader header =
          blockchain
              .getBlockHeader(blockHash)
              .orElseThrow(
                  () -> new IllegalStateException("Missing block header for block " + blockNumber));
      final byte[] raw =
          bonsai
              .getTrieLog(blockHash)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Missing trie log for block "
                              + blockNumber
                              + "; trie-log pruning must be disabled for conversion"));
      layers.add(trieLogFactory.deserialize(raw));
      expectedRoots.add(header.getStateRoot());
    }
    return new WindowData(start, layers, expectedRoots);
  }

  /**
   * Opens a raw RocksDB store over the database directory that lists the Forest {@code WORLD_STATE}
   * column family among its segments (alongside every BONSAI segment), then closes it again. The
   * columnar storage is configured to create missing column families, so this single open is enough
   * to materialize {@code WORLD_STATE} on disk so a subsequently-built controller can open it.
   *
   * @param databaseDir the RocksDB database directory
   */
  private void preCreateWorldStateColumnFamily(final Path databaseDir) {
    final RocksDBConfiguration rocksDBConfiguration =
        new RocksDBConfigurationBuilder().databaseDir(databaseDir).build();

    final List<SegmentIdentifier> segments = new ArrayList<>();
    for (final KeyValueSegmentIdentifier segment : KeyValueSegmentIdentifier.values()) {
      if (segment.includeInDatabaseFormat(DataStorageFormat.BONSAI)) {
        segments.add(segment);
      }
    }
    // WORLD_STATE is a FOREST-only column family; listing it here (not as ignorable) creates it.
    segments.add(KeyValueSegmentIdentifier.WORLD_STATE);

    final OptimisticRocksDBColumnarKeyValueStorage store =
        new OptimisticRocksDBColumnarKeyValueStorage(
            rocksDBConfiguration,
            segments,
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
    store.close();
  }

  /**
   * Rewrites {@code DATABASE_METADATA.json} in the given data directory so that it is read back as
   * the current Forest versioned storage format. Mirrors the exact serialized shape and {@link
   * com.fasterxml.jackson.databind.ObjectMapper} configuration used by {@link DatabaseMetadata}.
   *
   * @param dataDir the Besu data directory containing the metadata file
   */
  void flipMetadataToForest(final Path dataDir) {
    final DatabaseMetadata forestMetadata =
        new DatabaseMetadata(BaseVersionedStorageFormat.FOREST_WITH_RECEIPT_COMPACTION);
    try {
      forestMetadata.writeToDirectory(dataDir);
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to write FOREST database metadata to " + dataDir, e);
    }
  }

  private static String formatDuration(final long totalSeconds) {
    if (totalSeconds < 0) {
      return "unknown";
    }
    final long days = totalSeconds / 86400;
    final long hours = (totalSeconds % 86400) / 3600;
    final long minutes = (totalSeconds % 3600) / 60;
    final long seconds = totalSeconds % 60;
    if (days > 0) {
      return String.format("%dd %dh %dm %ds", days, hours, minutes, seconds);
    }
    if (hours > 0) {
      return String.format("%dh %dm %ds", hours, minutes, seconds);
    }
    if (minutes > 0) {
      return String.format("%dm %ds", minutes, seconds);
    }
    return String.format("%ds", seconds);
  }
}
