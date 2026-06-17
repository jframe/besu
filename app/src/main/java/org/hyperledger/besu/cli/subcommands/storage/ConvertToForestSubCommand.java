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
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.forest.migration.BonsaiTrieLogToForestConverter;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * The {@code x-convert-to-forest} storage subcommand. Converts an existing Bonsai full-sync
 * database into a Forest database by replaying its trie logs block-by-block, reconstructing the
 * Forest world-state trie nodes, and flipping the database metadata to the Forest format.
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

  @CommandLine.Option(
      names = "--prune-bonsai",
      description =
          "After conversion, clear the Bonsai-only column families to reclaim disk space (default: ${DEFAULT-VALUE}).")
  private boolean pruneBonsai = false;

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
    final BesuController controller = createBesuController();
    final DataStorageConfiguration config = controller.getDataStorageConfiguration();
    Preconditions.checkArgument(
        config.getDataStorageFormat() == DataStorageFormat.BONSAI,
        "x-convert-to-forest only supports source data-storage-format=BONSAI");

    final StorageProvider storageProvider = controller.getStorageProvider();
    final MutableBlockchain blockchain = controller.getProtocolContext().getBlockchain();
    final BonsaiWorldStateKeyValueStorage bonsai =
        (BonsaiWorldStateKeyValueStorage) storageProvider.createWorldStateStorage(config);
    final ForestWorldStateKeyValueStorage forest =
        new ForestWorldStateKeyValueStorage(
            storageProvider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.WORLD_STATE));
    final BonsaiTrieLogToForestConverter converter = new BonsaiTrieLogToForestConverter(forest);

    // Seed genesis so block-1 replay starts from the correct base state.
    final GenesisState genesisState =
        GenesisState.fromConfig(
            config,
            parentCommand.besuCommand.getGenesisConfig(),
            controller.getProtocolSchedule(),
            new CodeCache());
    final WorldStatePreimageStorage preimage = storageProvider.createWorldStatePreimageStorage();
    converter.seedGenesis(genesisState, preimage, EvmConfiguration.DEFAULT);
    LOG.info("Seeded genesis state (root={})", converter.currentRootHash());

    final long head = blockchain.getChainHeadBlockNumber();
    final TrieLogFactoryImpl trieLogFactory = new TrieLogFactoryImpl();
    for (long number = 1; number <= head; number++) {
      final BlockHeader header =
          blockchain
              .getBlockHeader(blockchain.getBlockHashByNumber(number).orElseThrow())
              .orElseThrow();
      final long blockNumber = number;
      final byte[] raw =
          bonsai
              .getTrieLog(header.getBlockHash())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Missing trie log for block "
                              + blockNumber
                              + "; trie-log pruning must be disabled for conversion"));
      final TrieLog layer = trieLogFactory.deserialize(raw);
      converter.applyTrieLog(layer, header.getStateRoot());
      if (number % 5000 == 0) {
        LOG.info(
            "Converted through block {} / {} (root={})", number, head, converter.currentRootHash());
      }
    }
    LOG.info("Conversion complete to head {} (root={})", head, converter.currentRootHash());

    flipMetadataToForest(parentCommand.besuCommand.dataDir());
    LOG.info("Flipped database metadata to FOREST format");

    if (pruneBonsai) {
      for (final KeyValueSegmentIdentifier seg :
          new KeyValueSegmentIdentifier[] {
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE,
            KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
            KeyValueSegmentIdentifier.CODE_STORAGE,
            KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            KeyValueSegmentIdentifier.TRIE_LOG_STORAGE
          }) {
        try {
          storageProvider.getStorageBySegmentIdentifier(seg).clear();
          LOG.info("Cleared Bonsai-only segment {}", seg);
        } catch (final RuntimeException e) {
          throw new IllegalStateException("Failed to clear Bonsai segment " + seg, e);
        }
      }
    }

    spec.commandLine().getOut().println("x-convert-to-forest finished successfully.");
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
