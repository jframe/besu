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
package org.hyperledger.besu.ethereum.chain;

import static org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture.MAINNET;
import static org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;

import org.hyperledger.besu.cli.config.NetworkName;
import org.hyperledger.besu.config.GenesisConfigFile;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProviderBuilder;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.MetricsSystemFactory;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBKeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.services.BesuConfigurationImpl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ImportBlocksTaskPerformanceTest {
  private static final List<BlockWithReceipts> blocks = new ArrayList<>();
  private static DefaultBlockchain blockchain;

  private static final int BLOCK_LIMIT = 501; // includes genesis block
  private static final String DB_EXPORT_FILE = "/Users/jframe/db_export_10k.rlp";
  private static final NetworkName NETWORK = NetworkName.MAINNET;
  private static final Difficulty DIFFICULTY = Difficulty.fromHexOrDecimalString("58750003716598352816469");

  @BeforeAll
  public static void setup(final @TempDir Path tempDir) {
    final MainnetBlockHeaderFunctions blockHeaderFunctions = new MainnetBlockHeaderFunctions();
    try {
      final Path file = Path.of(DB_EXPORT_FILE);
      final RawBlockWithReceiptsIterator rawBlockWithReceiptsIterator =
          new RawBlockWithReceiptsIterator(file, blockHeaderFunctions);

      while (rawBlockWithReceiptsIterator.hasNext() && blocks.size() < BLOCK_LIMIT) {
        BlockWithReceipts blockWithReceipts = rawBlockWithReceiptsIterator.next();
        if (blockWithReceipts.getNumber() % 100 == 0) {
          System.out.println("Loaded block " + blockWithReceipts.getNumber());
        }
        blocks.add(blockWithReceipts);
      }

      System.out.println("Loaded " + blocks.size() + " blocks.");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
    public void setupBlockchain(final @TempDir Path tempDir) {
    final StorageProvider storageProvider =
            createKeyValueStorageProvider(tempDir, tempDir.resolve("database"));
    VariablesStorage variablesStorage = storageProvider.createVariablesStorage();
    ObservableMetricsSystem metricsSystem =
            MetricsSystemFactory.create(MetricsConfiguration.builder().enabled(true).build());
    BlockchainStorage blockchainStorage =
            storageProvider.createBlockchainStorage(MAINNET, variablesStorage, DEFAULT_BONSAI_CONFIG);
    GenesisConfigFile genesisConfigFile = GenesisConfigFile.fromResource(NETWORK.getGenesisFile());
    GenesisState genesisState = GenesisState.fromConfig(genesisConfigFile, MAINNET);
    Block genesisBlock = genesisState.getBlock();
    blockchain = (DefaultBlockchain) DefaultBlockchain.createMutable(genesisBlock, blockchainStorage, metricsSystem, 0);

    BlockWithReceipts firstBlockWithReceipts = blocks.getFirst();
    if (!genesisBlock.equals(firstBlockWithReceipts.getBlock())) {
      blockchain.unsafeImportBlock(
              firstBlockWithReceipts.getBlock(),
              firstBlockWithReceipts.getReceipts(),
              Optional.of(DIFFICULTY));
      blockchain.unsafeSetChainHead(firstBlockWithReceipts.getBlock().getHeader(), DIFFICULTY);
    }
  }

  @ParameterizedTest
  @MethodSource("blockImportValues")
  public void blockImportUsingLoop(final int blocksToImport) {
    long start = System.nanoTime();
    for (int i = 1; i <= blocksToImport; i++) {
      blockchain.appendBlock(blocks.get(i).getBlock(), blocks.get(i).getReceipts());
    }
    long end = System.nanoTime();
    long totalTime = end - start;
    System.out.println("Imported " + blocksToImport + " in " + (double) totalTime / 1_000_000 + "ms");
    System.out.println(
        "Imported time per block " + (double) totalTime / blocksToImport / 1_000_000 + "ms");
  }

  @ParameterizedTest
  @MethodSource("blockImportValues")
  public void ImportUsingLoopStoreOnly(final int blocksToImport) {
    long start = System.nanoTime();
    for (int i = 1; i <= blocksToImport; i++) {
      blockchain.storeBlock(blocks.get(i).getBlock(), blocks.get(i).getReceipts());
    }
    long end = System.nanoTime();
    long totalTime = end - start;
    System.out.println("Imported blocks in " + (double) totalTime / 1_000_000 + "ms");
    System.out.println(
        "Imported time per block " + (double) totalTime / blocksToImport / 1_000_000 + "ms");
  }

  @ParameterizedTest
  @MethodSource("blockImportValues")
  public void unsafeBlockImportUsingLoop(final int blocksToImport) {
    long start = System.nanoTime();
    for (int i = 1; i <= blocksToImport; i++) {
      blockchain.unsafeImportBlock(
          blocks.get(i).getBlock(), blocks.get(i).getReceipts(), Optional.empty());
    }
    long end = System.nanoTime();
    long totalTime = end - start;
    System.out.println("Imported blocks in " + (double) totalTime / 1_000_000 + "ms");
    System.out.println(
        "Imported time per block " + (double) totalTime / blocksToImport / 1_000_000 + "ms");
  }

  @ParameterizedTest
  @MethodSource("blockImportValues")
  public void unsafeBlockImportWithoutTxIndexing(final int blocksToImport) {
    long start = System.nanoTime();
    for (int i = 1; i <= blocksToImport; i++) {
      blockchain.unsafeImportBlockWithoutTxIndexing(
              blocks.get(i).getBlock(), blocks.get(i).getReceipts(), Optional.empty());
    }
    long end = System.nanoTime();
    long totalTime = end - start;
    System.out.println("Imported blocks in " + (double) totalTime / 1_000_000 + "ms");
    System.out.println(
            "Imported time per block " + (double) totalTime / blocksToImport / 1_000_000 + "ms");
  }

  @ParameterizedTest
  @MethodSource("blockImportValues")
  public void storeBlockForSyncing(final int blocksToImport) {
    long start = System.nanoTime();
    for (int i = 1; i <= blocksToImport; i++) {
      blockchain.storeBlockForSyncing(
              blocks.get(i).getBlock(), blocks.get(i).getReceipts());
    }
    long end = System.nanoTime();
    long totalTime = end - start;
    System.out.println("Imported blocks in " + (double) totalTime / 1_000_000 + "ms");
    System.out.println(
            "Imported time per block " + (double) totalTime / blocksToImport / 1_000_000 + "ms");
  }

  @ParameterizedTest
  @MethodSource("blockImportValues")
  public void unsafeBlockImportWithoutTxIndexingOneTx(final int blocksToImport) {
    long start = System.nanoTime();
    blockchain.unsafeImportBlockWithoutTxIndexingOneTx(blocks.subList(1, blocksToImport + 1));
    long end = System.nanoTime();
    long totalTime = end - start;
    System.out.println("Imported blocks in " + (double) totalTime / 1_000_000 + "ms");
    System.out.println(
            "Imported time per block " + (double) totalTime / blocksToImport / 1_000_000 + "ms");
  }

  @ParameterizedTest
  @MethodSource("blockImportValues")
  public void unsafeBlockImportWithoutTxIndexingParallelTxs(final int blocksToImport) {
    long start = System.nanoTime();
    blockchain.unsafeImportBlockWithoutTxIndexingMultipleParallelTxs(blocks.subList(1, blocksToImport + 1));
    long end = System.nanoTime();
    long totalTime = end - start;
    System.out.println("Imported blocks in " + (double) totalTime / 1_000_000 + "ms");
    System.out.println(
            "Imported time per block " + (double) totalTime / blocksToImport / 1_000_000 + "ms");
  }

  @ParameterizedTest
  @MethodSource("blockImportValues")
  public void unsafeBlockImportParallel(final int blocksToImport) {
    long start = System.nanoTime();
    blockchain.unsafeImportBlocks(blocks.subList(1, blocksToImport + 1));
    long end = System.nanoTime();
    long totalTime = end - start;
    System.out.println("Imported blocks in " + (double) totalTime / 1_000_000 + "ms");
    System.out.println(
        "Imported time per block " + (double) totalTime / blocksToImport / 1_000_000 + "ms");
  }

  private static StorageProvider createKeyValueStorageProvider(
      final Path dataDir, final Path dbDir) {
    final var besuConfiguration = new BesuConfigurationImpl();
    besuConfiguration.init(dataDir, dbDir, DataStorageConfiguration.DEFAULT_CONFIG);
    return new KeyValueStorageProviderBuilder()
        .withStorageFactory(
            new RocksDBKeyValueStorageFactory(
                () ->
                    new RocksDBFactoryConfiguration(
                        RocksDBCLIOptions.DEFAULT_MAX_OPEN_FILES,
                        RocksDBCLIOptions.DEFAULT_BACKGROUND_THREAD_COUNT,
                        RocksDBCLIOptions.DEFAULT_CACHE_CAPACITY,
                        RocksDBCLIOptions.DEFAULT_IS_HIGH_SPEC),
                Arrays.asList(KeyValueSegmentIdentifier.values()),
                RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS))
        .withCommonConfiguration(besuConfiguration)
        .withMetricsSystem(new NoOpMetricsSystem())
        .build();
  }

  static Stream<Integer> blockImportValues() {
    return Stream.of(200);
  }
}
