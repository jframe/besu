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
import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/** The check-flat-db subcommand. */
@Command(
    name = "check-flat-db",
    description = "Verify flat DB consistency against trielog data",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class CheckFlatDbSubCommand implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(CheckFlatDbSubCommand.class);

  @SuppressWarnings("unused")
  @ParentCommand
  private StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(
      names = {"--start-block"},
      description = "The starting block number (inclusive). Default: 0",
      defaultValue = "0")
  private long startBlock;

  @CommandLine.Option(
      names = {"--end-block"},
      description =
          "The ending block number (inclusive). If not specified, checks up to the chain head.")
  private Long endBlock;

  @CommandLine.Option(
      names = {"--output-file"},
      description = "Path to the output CSV file. Default: inconsistencies.csv",
      defaultValue = "inconsistencies.csv")
  private String outputFile;

  @Override
  public void run() {
    checkNotNull(parentCommand);

    try {
      // Create Besu controller
      final BesuController besuController = createBesuController();
      final DataStorageConfiguration config = besuController.getDataStorageConfiguration();

      // Validate Bonsai format
      checkArgument(
          config.getDataStorageFormat().isBonsaiFormat(),
          "check-flat-db only works with data-storage-format=BONSAI or X_BONSAI_ARCHIVE");

      // Get storage and blockchain
      final StorageProvider storageProvider = besuController.getStorageProvider();
      final BonsaiWorldStateKeyValueStorage worldStateStorage =
          (BonsaiWorldStateKeyValueStorage) storageProvider.createWorldStateStorage(config);
      final Blockchain blockchain = besuController.getProtocolContext().getBlockchain();

      // Validate archive mode
      FlatDbMode flatDbMode = worldStateStorage.getFlatDbMode();
      LOG.info("Flat DB mode: {}", flatDbMode);

      if (flatDbMode != FlatDbMode.ARCHIVE) {
        LOG.warn(
            "WARNING: Flat DB is in {} mode, not ARCHIVE mode. "
                + "Consistency checks are most accurate for archive nodes. "
                + "Proceeding anyway...",
            flatDbMode);
      }

      // Determine end block
      final long finalEndBlock;
      if (endBlock == null) {
        finalEndBlock = blockchain.getChainHeadBlockNumber();
        LOG.info("No end block specified, checking up to chain head: {}", finalEndBlock);
      } else {
        finalEndBlock = endBlock;
      }

      // Validate block range
      checkArgument(startBlock >= 0, "Start block must be >= 0");
      checkArgument(
          finalEndBlock >= startBlock, "End block must be >= start block");
      checkArgument(
          finalEndBlock <= blockchain.getChainHeadBlockNumber(),
          "End block must be <= chain head (" + blockchain.getChainHeadBlockNumber() + ")");

      // Resolve output file path
      final Path outputFilePath = Paths.get(outputFile);
      if (!outputFilePath.isAbsolute()) {
        final Path resolvedPath =
            parentCommand.besuCommand.dataDir().resolve(outputFilePath).toAbsolutePath();
        LOG.info("Output will be written to: {}", resolvedPath);
      } else {
        LOG.info("Output will be written to: {}", outputFilePath);
      }

      // Create reporter
      try (InconsistencyReporter reporter = new InconsistencyReporter(outputFilePath)) {

        // Get storage references
        final SegmentedKeyValueStorage composedStorage =
            worldStateStorage.getComposedWorldStateStorage();
        final SegmentedKeyValueStorage trieLogStorage =
            (SegmentedKeyValueStorage) storageProvider.getStorageBySegmentIdentifier(
                org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                    .TRIE_LOG_STORAGE);

        // Create and run checker
        final FlatDbConsistencyChecker checker =
            new FlatDbConsistencyChecker(blockchain, composedStorage, trieLogStorage, reporter);

        LOG.info(
            "Starting flat DB consistency check from block {} to {} (total: {} blocks)",
            startBlock,
            finalEndBlock,
            (finalEndBlock - startBlock + 1));

        checker.check(startBlock, finalEndBlock);

        LOG.info("Consistency check complete.");

      } catch (IOException e) {
        LOG.error("Failed to create inconsistency reporter", e);
        throw new RuntimeException("Failed to create output file: " + outputFilePath, e);
      }

    } catch (Exception e) {
      LOG.error("Error during flat DB consistency check", e);
      throw new RuntimeException("Consistency check failed", e);
    }
  }

  /**
   * Creates a BesuController with trie log limits disabled.
   *
   * @return the Besu controller
   */
  private BesuController createBesuController() {
    final DataStorageConfiguration config =
        parentCommand.besuCommand.getDataStorageConfiguration();
    // Disable limit trie logs to avoid preloading during subcommand execution
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
}
