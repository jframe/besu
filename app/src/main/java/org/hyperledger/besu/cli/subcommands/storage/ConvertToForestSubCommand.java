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
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.migration.BonsaiTrieLogToForestConverter;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.OptimisticRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
 * <p>The conversion opens a single RocksDB store spanning the union of the Bonsai column families
 * (read from) and the Forest {@code WORLD_STATE} column family (written to). The controller's
 * format-gated storage provider cannot be used here because, while the database metadata still says
 * BONSAI, it only opens BONSAI column families and would never open {@code WORLD_STATE}.
 */
@Command(
    name = "x-convert-to-forest",
    description =
        "EXPERIMENTAL: convert a Bonsai full-sync database to a Forest database by replaying trie logs",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class ConvertToForestSubCommand implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ConvertToForestSubCommand.class);

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
    // Phase 1: build the controller only to capture the protocol schedule + genesis, then release
    // the RocksDB lock so we can re-open the database with our own union segment set.
    final ProtocolSchedule protocolSchedule;
    final GenesisState genesisState;
    final Path dataDir;
    final boolean receiptCompaction;

    final BesuController controller = createBesuController();
    try {
      final DataStorageConfiguration config = controller.getDataStorageConfiguration();
      checkArgument(
          config.getDataStorageFormat() == DataStorageFormat.BONSAI,
          "x-convert-to-forest only supports source data-storage-format=BONSAI");

      protocolSchedule = controller.getProtocolSchedule();
      genesisState =
          GenesisState.fromConfig(
              config,
              parentCommand.besuCommand.getGenesisConfig(),
              protocolSchedule,
              new CodeCache());
      dataDir = parentCommand.besuCommand.dataDir();
      receiptCompaction = config.getReceiptCompactionEnabled();
    } finally {
      controller.close();
    }

    // Phase 2: open one RocksDB store over the union of every Bonsai column family plus the Forest
    // WORLD_STATE column family, and drive the conversion against per-segment views of it.
    final Path databaseDir = dataDir.resolve(BesuController.DATABASE_PATH);
    final RocksDBConfiguration rocksDBConfiguration =
        new RocksDBConfigurationBuilder().databaseDir(databaseDir).build();

    final List<SegmentIdentifier> segments = new ArrayList<>();
    for (final KeyValueSegmentIdentifier segment : KeyValueSegmentIdentifier.values()) {
      if (segment.includeInDatabaseFormat(DataStorageFormat.BONSAI)) {
        segments.add(segment);
      }
    }
    // WORLD_STATE is a FOREST-only column family; opening it here creates+opens it for writing.
    segments.add(KeyValueSegmentIdentifier.WORLD_STATE);

    final OptimisticRocksDBColumnarKeyValueStorage unionStore =
        new OptimisticRocksDBColumnarKeyValueStorage(
            rocksDBConfiguration,
            segments,
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
    try {
      final ForestWorldStateKeyValueStorage forest =
          new ForestWorldStateKeyValueStorage(
              adapter(unionStore, KeyValueSegmentIdentifier.WORLD_STATE));

      final BlockchainStorage blockchainStorage =
          new KeyValueStoragePrefixedKeyBlockchainStorage(
              adapter(unionStore, KeyValueSegmentIdentifier.BLOCKCHAIN),
              new VariablesKeyValueStorage(
                  adapter(unionStore, KeyValueSegmentIdentifier.VARIABLES)),
              ScheduleBasedBlockHeaderFunctions.create(protocolSchedule),
              protocolSchedule,
              receiptCompaction);

      final KeyValueStorage trieLogStorage =
          adapter(unionStore, KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);

      final BonsaiTrieLogToForestConverter converter = new BonsaiTrieLogToForestConverter(forest);
      final TrieLogFactoryImpl trieLogFactory = new TrieLogFactoryImpl();

      // Seed genesis so block-1 replay starts from the correct base state. Slot preimages are not
      // needed for correctness, so an in-memory preimage store is sufficient.
      final WorldStatePreimageStorage preimage =
          new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage());
      converter.seedGenesis(genesisState, preimage, EvmConfiguration.DEFAULT);
      LOG.info("Seeded genesis state (root={})", converter.currentRootHash());

      final Hash headHash =
          blockchainStorage
              .getChainHead()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "No chain head found in database; nothing to convert"));
      final long head =
          blockchainStorage
              .getBlockHeader(headHash)
              .orElseThrow(
                  () -> new IllegalStateException("Chain head header missing for hash " + headHash))
              .getNumber();

      for (long number = 1; number <= head; number++) {
        final long blockNumber = number;
        final Hash blockHash =
            blockchainStorage
                .getBlockHash(number)
                .orElseThrow(
                    () -> new IllegalStateException("Missing block hash for block " + blockNumber));
        final BlockHeader header =
            blockchainStorage
                .getBlockHeader(blockHash)
                .orElseThrow(
                    () ->
                        new IllegalStateException("Missing block header for block " + blockNumber));
        final Optional<byte[]> rawTrieLog =
            trieLogStorage.get(blockHash.getBytes().toArrayUnsafe());
        final byte[] raw =
            rawTrieLog.orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing trie log for block "
                            + blockNumber
                            + "; trie-log pruning must be disabled for conversion"));
        final TrieLog layer = trieLogFactory.deserialize(raw);
        converter.applyTrieLog(layer, header.getStateRoot());
        if (number % 5000 == 0) {
          LOG.info(
              "Converted through block {} / {} (root={})",
              number,
              head,
              converter.currentRootHash());
        }
      }
      LOG.info("Conversion complete to head {} (root={})", head, converter.currentRootHash());
    } finally {
      try {
        unionStore.close();
      } catch (final RuntimeException e) {
        LOG.warn("Failed to close union RocksDB store", e);
      }
    }

    flipMetadataToForest(dataDir);
    LOG.info("Flipped database metadata to FOREST format");

    spec.commandLine().getOut().println("x-convert-to-forest finished successfully.");
  }

  private static KeyValueStorage adapter(
      final OptimisticRocksDBColumnarKeyValueStorage unionStore,
      final KeyValueSegmentIdentifier segment) {
    return new SegmentedKeyValueStorageAdapter(segment, unionStore);
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
}
