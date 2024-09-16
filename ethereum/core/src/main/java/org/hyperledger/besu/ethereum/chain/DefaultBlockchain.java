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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage.Updater;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.PrometheusMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.util.InvalidConfigurationException;
import org.hyperledger.besu.util.Subscribers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.prometheus.client.guava.cache.CacheMetricsCollector;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBlockchain implements MutableBlockchain {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockchain.class);

  private final Comparator<BlockHeader> heaviestChainBlockChoiceRule =
      Comparator.comparing(this::calculateTotalDifficulty);

  protected final BlockchainStorage blockchainStorage;

  private final Subscribers<BlockAddedObserver> blockAddedObservers = Subscribers.create();
  private final Subscribers<ChainReorgObserver> blockReorgObservers = Subscribers.create();
  private final long reorgLoggingThreshold;

  private volatile BlockHeader chainHeader;
  private volatile Difficulty totalDifficulty;
  private volatile int chainHeadTransactionCount;
  private volatile int chainHeadOmmerCount;

  private Comparator<BlockHeader> blockChoiceRule;

  private final int numberOfBlocksToCache;
  private final Optional<Cache<Hash, BlockHeader>> blockHeadersCache;
  private final Optional<Cache<Hash, BlockBody>> blockBodiesCache;
  private final Optional<Cache<Hash, List<TransactionReceipt>>> transactionReceiptsCache;
  private final Optional<Cache<Hash, Difficulty>> totalDifficultyCache;

  private Counter gasUsedCounter = NoOpMetricsSystem.NO_OP_COUNTER;
  private Counter numberOfTransactionsCounter = NoOpMetricsSystem.NO_OP_COUNTER;

  private DefaultBlockchain(
      final Optional<Block> genesisBlock,
      final BlockchainStorage blockchainStorage,
      final MetricsSystem metricsSystem,
      final long reorgLoggingThreshold) {
    this(genesisBlock, blockchainStorage, metricsSystem, reorgLoggingThreshold, null, 0);
  }

  private DefaultBlockchain(
      final Optional<Block> genesisBlock,
      final BlockchainStorage blockchainStorage,
      final MetricsSystem metricsSystem,
      final long reorgLoggingThreshold,
      final String dataDirectory,
      final int numberOfBlocksToCache) {
    checkNotNull(genesisBlock);
    checkNotNull(blockchainStorage);
    checkNotNull(metricsSystem);

    this.blockchainStorage = blockchainStorage;
    genesisBlock.ifPresent(block -> this.setGenesis(block, dataDirectory));

    final Hash chainHead = blockchainStorage.getChainHead().get();
    chainHeader = blockchainStorage.getBlockHeader(chainHead).get();
    totalDifficulty = blockchainStorage.getTotalDifficulty(chainHead).get();
    final BlockBody chainHeadBody = blockchainStorage.getBlockBody(chainHead).get();
    chainHeadTransactionCount = chainHeadBody.getTransactions().size();
    chainHeadOmmerCount = chainHeadBody.getOmmers().size();

    this.reorgLoggingThreshold = reorgLoggingThreshold;
    this.blockChoiceRule = heaviestChainBlockChoiceRule;
    this.numberOfBlocksToCache = numberOfBlocksToCache;

    if (numberOfBlocksToCache != 0) {
      blockHeadersCache =
          Optional.of(
              CacheBuilder.newBuilder().recordStats().maximumSize(numberOfBlocksToCache).build());
      blockBodiesCache =
          Optional.of(
              CacheBuilder.newBuilder().recordStats().maximumSize(numberOfBlocksToCache).build());
      transactionReceiptsCache =
          Optional.of(
              CacheBuilder.newBuilder().recordStats().maximumSize(numberOfBlocksToCache).build());
      totalDifficultyCache =
          Optional.of(
              CacheBuilder.newBuilder().recordStats().maximumSize(numberOfBlocksToCache).build());
      CacheMetricsCollector cacheMetrics = new CacheMetricsCollector();
      cacheMetrics.addCache("blockHeaders", blockHeadersCache.get());
      cacheMetrics.addCache("blockBodies", blockBodiesCache.get());
      cacheMetrics.addCache("transactionReceipts", transactionReceiptsCache.get());
      cacheMetrics.addCache("totalDifficulty", totalDifficultyCache.get());
      if (metricsSystem instanceof PrometheusMetricsSystem prometheusMetricsSystem)
        prometheusMetricsSystem.addCollector(BesuMetricCategory.BLOCKCHAIN, () -> cacheMetrics);
    } else {
      blockHeadersCache = Optional.empty();
      blockBodiesCache = Optional.empty();
      transactionReceiptsCache = Optional.empty();
      totalDifficultyCache = Optional.empty();
    }

    createCounters(metricsSystem);
    createGauges(metricsSystem);
  }

  private void createCounters(final MetricsSystem metricsSystem) {
    gasUsedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN, "chain_head_gas_used_counter", "Counter for Gas used");

    numberOfTransactionsCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "chain_head_transaction_count_counter",
            "Counter for the number of transactions");
  }

  private void createGauges(final MetricsSystem metricsSystem) {
    metricsSystem.createLongGauge(
        BesuMetricCategory.ETHEREUM,
        "blockchain_height",
        "The current height of the canonical chain",
        this::getChainHeadBlockNumber);

    metricsSystem.createLongGauge(
        BesuMetricCategory.ETHEREUM,
        "blockchain_finalized_block",
        "The current finalized block number",
        this::getFinalizedBlockNumber);

    metricsSystem.createLongGauge(
        BesuMetricCategory.ETHEREUM,
        "blockchain_safe_block",
        "The current safe block number",
        this::getSafeBlockNumber);

    metricsSystem.createGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "difficulty_total",
        "Total difficulty of the chainhead",
        () -> this.getChainHead().getTotalDifficulty().toBigInteger().doubleValue());

    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "chain_head_timestamp",
        "Timestamp from the current chain head",
        () -> getChainHeadHeader().getTimestamp());

    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "chain_head_gas_used",
        "Gas used by the current chain head block",
        () -> getChainHeadHeader().getGasUsed());

    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "chain_head_gas_limit",
        "Block gas limit of the current chain head block",
        () -> getChainHeadHeader().getGasLimit());

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "chain_head_transaction_count",
        "Number of transactions in the current chain head block",
        () -> chainHeadTransactionCount);

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "chain_head_ommer_count",
        "Number of ommers in the current chain head block",
        () -> chainHeadOmmerCount);
  }

  public static MutableBlockchain createMutable(
      final Block genesisBlock,
      final BlockchainStorage blockchainStorage,
      final MetricsSystem metricsSystem,
      final long reorgLoggingThreshold) {
    checkNotNull(genesisBlock);
    return new DefaultBlockchain(
        Optional.of(genesisBlock),
        blockchainStorage,
        metricsSystem,
        reorgLoggingThreshold,
        null,
        0);
  }

  public static MutableBlockchain createMutable(
      final Block genesisBlock,
      final BlockchainStorage blockchainStorage,
      final MetricsSystem metricsSystem,
      final long reorgLoggingThreshold,
      final String dataDirectory) {
    checkNotNull(genesisBlock);
    return new DefaultBlockchain(
        Optional.of(genesisBlock),
        blockchainStorage,
        metricsSystem,
        reorgLoggingThreshold,
        dataDirectory,
        0);
  }

  public static MutableBlockchain createMutable(
      final Block genesisBlock,
      final BlockchainStorage blockchainStorage,
      final MetricsSystem metricsSystem,
      final long reorgLoggingThreshold,
      final String dataDirectory,
      final int numberOfBlocksToCache) {
    checkNotNull(genesisBlock);
    return new DefaultBlockchain(
        Optional.of(genesisBlock),
        blockchainStorage,
        metricsSystem,
        reorgLoggingThreshold,
        dataDirectory,
        numberOfBlocksToCache);
  }

  public static Blockchain create(
      final BlockchainStorage blockchainStorage,
      final MetricsSystem metricsSystem,
      final long reorgLoggingThreshold) {
    checkArgument(
        validateStorageNonEmpty(blockchainStorage), "Cannot create Blockchain from empty storage");
    return new DefaultBlockchain(
        Optional.empty(), blockchainStorage, metricsSystem, reorgLoggingThreshold);
  }

  private static boolean validateStorageNonEmpty(final BlockchainStorage blockchainStorage) {
    // Run a few basic checks to make sure data looks available and consistent
    return blockchainStorage
            .getChainHead()
            .flatMap(blockchainStorage::getTotalDifficulty)
            .isPresent()
        && blockchainStorage.getBlockHash(BlockHeader.GENESIS_BLOCK_NUMBER).isPresent();
  }

  @Override
  public ChainHead getChainHead() {
    return new ChainHead(chainHeader, totalDifficulty, chainHeader.getNumber());
  }

  @Override
  public Optional<Hash> getFinalized() {
    return blockchainStorage.getFinalized();
  }

  @Override
  public Optional<Hash> getSafeBlock() {
    return blockchainStorage.getSafeBlock();
  }

  @Override
  public Hash getChainHeadHash() {
    return chainHeader.getHash();
  }

  @Override
  public long getChainHeadBlockNumber() {
    return chainHeader.getNumber();
  }

  @Override
  public BlockHeader getChainHeadHeader() {
    return chainHeader;
  }

  @Override
  public Block getChainHeadBlock() {
    return new Block(
        chainHeader,
        getBlockBody(chainHeader.getHash())
            .orElseGet(() -> getBlockBodySafe(chainHeader.getHash()).get()));
  }

  @Override
  public Optional<BlockHeader> getBlockHeader(final long blockNumber) {
    return blockchainStorage.getBlockHash(blockNumber).flatMap(this::getBlockHeader);
  }

  @Override
  public Optional<BlockHeader> getBlockHeader(final Hash blockHeaderHash) {
    return blockHeadersCache
        .map(
            cache ->
                Optional.ofNullable(cache.getIfPresent(blockHeaderHash))
                    .or(() -> blockchainStorage.getBlockHeader(blockHeaderHash)))
        .orElseGet(() -> blockchainStorage.getBlockHeader(blockHeaderHash));
  }

  @Override
  public synchronized Optional<BlockHeader> getBlockHeaderSafe(final Hash blockHeaderHash) {
    return blockHeadersCache
        .map(
            cache ->
                Optional.ofNullable(cache.getIfPresent(blockHeaderHash))
                    .or(() -> blockchainStorage.getBlockHeader(blockHeaderHash)))
        .orElseGet(() -> blockchainStorage.getBlockHeader(blockHeaderHash));
  }

  @Override
  public Optional<BlockBody> getBlockBody(final Hash blockHeaderHash) {
    return blockBodiesCache
        .map(
            cache ->
                Optional.ofNullable(cache.getIfPresent(blockHeaderHash))
                    .or(() -> blockchainStorage.getBlockBody(blockHeaderHash)))
        .orElseGet(() -> blockchainStorage.getBlockBody(blockHeaderHash));
  }

  @Override
  public synchronized Optional<BlockBody> getBlockBodySafe(final Hash blockHeaderHash) {
    return getBlockBody(blockHeaderHash);
  }

  @Override
  public Optional<List<TransactionReceipt>> getTxReceipts(final Hash blockHeaderHash) {
    return transactionReceiptsCache
        .map(
            cache ->
                Optional.ofNullable(cache.getIfPresent(blockHeaderHash))
                    .or(() -> blockchainStorage.getTransactionReceipts(blockHeaderHash)))
        .orElseGet(() -> blockchainStorage.getTransactionReceipts(blockHeaderHash));
  }

  @Override
  public Optional<Hash> getBlockHashByNumber(final long number) {
    return blockchainStorage.getBlockHash(number);
  }

  @Override
  public Optional<Difficulty> getTotalDifficultyByHash(final Hash blockHeaderHash) {
    return totalDifficultyCache
        .map(
            cache ->
                Optional.ofNullable(cache.getIfPresent(blockHeaderHash))
                    .or(() -> blockchainStorage.getTotalDifficulty(blockHeaderHash)))
        .orElseGet(() -> blockchainStorage.getTotalDifficulty(blockHeaderHash));
  }

  @Override
  public Optional<Transaction> getTransactionByHash(final Hash transactionHash) {
    return blockchainStorage
        .getTransactionLocation(transactionHash)
        .flatMap(
            l ->
                blockchainStorage
                    .getBlockBody(l.getBlockHash())
                    .map(b -> b.getTransactions().get(l.getTransactionIndex())));
  }

  @Override
  public Optional<TransactionLocation> getTransactionLocation(final Hash transactionHash) {
    return blockchainStorage.getTransactionLocation(transactionHash);
  }

  @Override
  public Comparator<BlockHeader> getBlockChoiceRule() {
    return blockChoiceRule;
  }

  @Override
  public void setBlockChoiceRule(final Comparator<BlockHeader> blockChoiceRule) {
    this.blockChoiceRule = blockChoiceRule;
  }

  @Override
  public synchronized void appendBlock(final Block block, final List<TransactionReceipt> receipts) {
    if (numberOfBlocksToCache != 0) cacheBlockData(block, receipts);
    appendBlockHelper(new BlockWithReceipts(block, receipts), false);
  }

  @Override
  public synchronized void storeBlock(final Block block, final List<TransactionReceipt> receipts) {
    if (numberOfBlocksToCache != 0) cacheBlockData(block, receipts);
    appendBlockHelper(new BlockWithReceipts(block, receipts), true);
  }

  private void cacheBlockData(final Block block, final List<TransactionReceipt> receipts) {
    blockHeadersCache.ifPresent(cache -> cache.put(block.getHash(), block.getHeader()));
    blockBodiesCache.ifPresent(cache -> cache.put(block.getHash(), block.getBody()));
    transactionReceiptsCache.ifPresent(cache -> cache.put(block.getHash(), receipts));
    totalDifficultyCache.ifPresent(
        cache -> cache.put(block.getHash(), block.getHeader().getDifficulty()));
  }

  private boolean blockShouldBeProcessed(
      final Block block, final List<TransactionReceipt> receipts) {
    checkArgument(
        block.getBody().getTransactions().size() == receipts.size(),
        "Supplied receipts do not match block transactions.");
    if (blockIsAlreadyTracked(block)) {
      return false;
    }
    checkArgument(blockIsConnected(block), "Attempt to append non-connected block.");
    return true;
  }

  private void appendBlockHelper(
      final BlockWithReceipts blockWithReceipts, final boolean storeOnly) {

    if (!blockShouldBeProcessed(blockWithReceipts.getBlock(), blockWithReceipts.getReceipts())) {
      return;
    }

    final Block block = blockWithReceipts.getBlock();
    final List<TransactionReceipt> receipts = blockWithReceipts.getReceipts();
    final Hash hash = block.getHash();
    final Difficulty td = calculateTotalDifficulty(block.getHeader());

    final BlockchainStorage.Updater updater = blockchainStorage.updater();

    updater.putBlockHeader(hash, block.getHeader());
    updater.putBlockBody(hash, block.getBody());
    updater.putTransactionReceipts(hash, receipts);
    updater.putTotalDifficulty(hash, td);

    final BlockAddedEvent blockAddedEvent;
    if (storeOnly) {
      blockAddedEvent = handleStoreOnly(blockWithReceipts);
    } else {
      blockAddedEvent = updateCanonicalChainData(updater, blockWithReceipts);
      if (blockAddedEvent.isNewCanonicalHead()) {
        updateCacheForNewCanonicalHead(block, td);
      }
    }

    updater.commit();
    blockAddedObservers.forEach(observer -> observer.onBlockAdded(blockAddedEvent));
  }

  @Override
  public synchronized void unsafeImportBlock(
      final Block block,
      final List<TransactionReceipt> transactionReceipts,
      final Optional<Difficulty> maybeTotalDifficulty) {
    final BlockchainStorage.Updater updater = blockchainStorage.updater();
    final Hash hash = block.getHash();
    updater.putBlockHeader(hash, block.getHeader());
    updater.putBlockHash(block.getHeader().getNumber(), hash);
    updater.putBlockBody(hash, block.getBody());
    final int nbTrx = block.getBody().getTransactions().size();
    for (int i = 0; i < nbTrx; i++) {
      final Hash transactionHash = block.getBody().getTransactions().get(i).getHash();
      updater.putTransactionLocation(transactionHash, new TransactionLocation(transactionHash, i));
    }
    updater.putTransactionReceipts(hash, transactionReceipts);
    maybeTotalDifficulty.ifPresent(
        totalDifficulty -> updater.putTotalDifficulty(hash, totalDifficulty));
    updater.commit();
  }

  @Override
  public void unsafeImportBlocks(final List<BlockWithReceipts> blocks) {
    final List<Pair<Bytes, Bytes>> updates = new ArrayList<>();
    blocks.parallelStream()
        .forEach(
            blockWithReceipts -> {
              Block block = blockWithReceipts.getBlock();
              Hash hash = blockWithReceipts.getHash();
              updates.add(
                  Pair.of(
                      Bytes.concatenate(
                          KeyValueStoragePrefixedKeyBlockchainStorage.BLOCK_HEADER_PREFIX, hash),
                      RLP.encode(blockWithReceipts.getBlock().getHeader()::writeTo)));
              updates.add(
                  Pair.of(
                      Bytes.concatenate(
                          KeyValueStoragePrefixedKeyBlockchainStorage.BLOCK_HASH_PREFIX,
                          UInt256.valueOf(blockWithReceipts.getBlock().getHeader().getNumber())),
                      hash));
              updates.add(
                  Pair.of(
                      Bytes.concatenate(
                          KeyValueStoragePrefixedKeyBlockchainStorage.BLOCK_BODY_PREFIX, hash),
                      RLP.encode(blockWithReceipts.getBlock().getBody()::writeTo)));
              final int nbTrx = block.getBody().getTransactions().size();
              IntStream.range(0, nbTrx)
                  .parallel()
                  .forEach(
                      i -> {
                        final Hash transactionHash =
                            block.getBody().getTransactions().get(i).getHash();
                        updates.add(
                            Pair.of(
                                Bytes.concatenate(
                                    KeyValueStoragePrefixedKeyBlockchainStorage
                                        .TRANSACTION_LOCATION_PREFIX,
                                    transactionHash),
                                RLP.encode(new TransactionLocation(transactionHash, i)::writeTo)));
                      });

              updates.add(
                  Pair.of(
                      Bytes.concatenate(
                          KeyValueStoragePrefixedKeyBlockchainStorage.TRANSACTION_RECEIPTS_PREFIX,
                          hash),
                      RLP.encode(
                          o ->
                              o.writeList(
                                  blockWithReceipts.getReceipts(),
                                  (r, rlpOutput) -> r.writeToForStorage(rlpOutput, true)))));
            });

    final BlockchainStorage.Updater updater = blockchainStorage.updater();
    updates.forEach(update -> updater.put(update.getLeft(), update.getRight()));
    updater.commit();
  }

  @Override
  public synchronized void unsafeSetChainHead(
      final BlockHeader blockHeader, final Difficulty totalDifficulty) {
    final BlockchainStorage.Updater updater = blockchainStorage.updater();
    this.chainHeader = blockHeader;
    this.totalDifficulty = totalDifficulty;
    updater.setChainHead(blockHeader.getBlockHash());
    updater.commit();
  }

  @Override
  public Difficulty calculateTotalDifficulty(final BlockHeader blockHeader) {
    if (blockHeader.getNumber() == BlockHeader.GENESIS_BLOCK_NUMBER) {
      return blockHeader.getDifficulty();
    }

    final Difficulty parentTotalDifficulty =
        blockchainStorage
            .getTotalDifficulty(blockHeader.getParentHash())
            .orElseThrow(
                () -> new IllegalStateException("Blockchain is missing total difficulty data."));
    return blockHeader.getDifficulty().add(parentTotalDifficulty);
  }

  private BlockAddedEvent updateCanonicalChainData(
      final BlockchainStorage.Updater updater, final BlockWithReceipts blockWithReceipts) {

    final Block newBlock = blockWithReceipts.getBlock();
    final Hash chainHead = blockchainStorage.getChainHead().orElse(null);

    if (newBlock.getHeader().getNumber() != BlockHeader.GENESIS_BLOCK_NUMBER && chainHead == null) {
      throw new IllegalStateException("Blockchain is missing chain head.");
    }

    try {
      if (newBlock.getHeader().getParentHash().equals(chainHead) || chainHead == null) {
        return handleNewHead(updater, blockWithReceipts);
      } else if (blockChoiceRule.compare(newBlock.getHeader(), chainHeader) > 0) {
        // New block represents a chain reorganization
        return handleChainReorg(updater, blockWithReceipts);
      } else {
        // New block represents a fork
        return handleFork(updater, newBlock);
      }
    } catch (final NoSuchElementException e) {
      // Any Optional.get() calls in this block should be present, missing data means data
      // corruption or a bug.
      updater.rollback();
      throw new IllegalStateException("Blockchain is missing data that should be present.", e);
    }
  }

  private BlockAddedEvent handleStoreOnly(final BlockWithReceipts blockWithReceipts) {
    return BlockAddedEvent.createForStoredOnly(blockWithReceipts.getBlock());
  }

  private BlockAddedEvent handleNewHead(
      final Updater updater, final BlockWithReceipts blockWithReceipts) {
    // This block advances the chain, update the chain head
    final Hash newBlockHash = blockWithReceipts.getHash();

    updater.putBlockHash(blockWithReceipts.getNumber(), newBlockHash);
    updater.setChainHead(newBlockHash);
    indexTransactionForBlock(
        updater, newBlockHash, blockWithReceipts.getBlock().getBody().getTransactions());
    gasUsedCounter.inc(blockWithReceipts.getHeader().getGasUsed());
    numberOfTransactionsCounter.inc(
        blockWithReceipts.getBlock().getBody().getTransactions().size());

    return BlockAddedEvent.createForHeadAdvancement(
        blockWithReceipts.getBlock(),
        LogWithMetadata.generate(
            blockWithReceipts.getBlock(), blockWithReceipts.getReceipts(), false),
        blockWithReceipts.getReceipts());
  }

  private BlockAddedEvent handleFork(final BlockchainStorage.Updater updater, final Block fork) {
    final Collection<Hash> forkHeads = blockchainStorage.getForkHeads();

    // Check to see if this block advances any existing fork.
    // This block will replace its parent
    forkHeads.stream()
        .filter(head -> head.equals(fork.getHeader().getParentHash()))
        .findAny()
        .ifPresent(forkHeads::remove);

    forkHeads.add(fork.getHash());

    updater.setForkHeads(forkHeads);
    return BlockAddedEvent.createForFork(fork);
  }

  private BlockAddedEvent handleChainReorg(
      final BlockchainStorage.Updater updater, final BlockWithReceipts newChainHeadWithReceipts) {
    final BlockWithReceipts oldChainWithReceipts = getBlockWithReceipts(chainHeader).get();
    BlockWithReceipts currentOldChainWithReceipts = oldChainWithReceipts;
    BlockWithReceipts currentNewChainWithReceipts = newChainHeadWithReceipts;

    // Update chain head
    updater.setChainHead(currentNewChainWithReceipts.getHeader().getHash());

    // Track transactions and logs to be added and removed
    final Map<Hash, List<Transaction>> newTransactions = new HashMap<>();
    final List<Transaction> removedTransactions = new ArrayList<>();
    final List<LogWithMetadata> addedLogsWithMetadata = new ArrayList<>();
    final List<LogWithMetadata> removedLogsWithMetadata = new ArrayList<>();

    while (currentNewChainWithReceipts.getNumber() > currentOldChainWithReceipts.getNumber()) {
      // If new chain is longer than old chain, walk back until we meet the old chain by number
      // adding indexing for new chain along the way.
      final Hash blockHash = currentNewChainWithReceipts.getHash();
      updater.putBlockHash(currentNewChainWithReceipts.getNumber(), blockHash);

      newTransactions.put(
          blockHash, currentNewChainWithReceipts.getBlock().getBody().getTransactions());
      addAddedLogsWithMetadata(addedLogsWithMetadata, currentNewChainWithReceipts);
      notifyChainReorgBlockAdded(currentNewChainWithReceipts);
      currentNewChainWithReceipts = getParentBlockWithReceipts(currentNewChainWithReceipts);
    }

    while (currentOldChainWithReceipts.getNumber() > currentNewChainWithReceipts.getNumber()) {
      // If oldChain is longer than new chain, walk back until we meet the new chain by number,
      // updating as we go.
      updater.removeBlockHash(currentOldChainWithReceipts.getNumber());

      removedTransactions.addAll(
          currentOldChainWithReceipts.getBlock().getBody().getTransactions());
      addRemovedLogsWithMetadata(removedLogsWithMetadata, currentOldChainWithReceipts);

      currentOldChainWithReceipts = getParentBlockWithReceipts(currentOldChainWithReceipts);
    }

    while (!currentOldChainWithReceipts.getHash().equals(currentNewChainWithReceipts.getHash())) {
      // Walk back until we meet the common ancestor between the two chains, updating as we go.
      final Hash newBlockHash = currentNewChainWithReceipts.getHash();
      updater.putBlockHash(currentNewChainWithReceipts.getNumber(), newBlockHash);

      newTransactions.put(
          newBlockHash, currentNewChainWithReceipts.getBlock().getBody().getTransactions());
      removedTransactions.addAll(
          currentOldChainWithReceipts.getBlock().getBody().getTransactions());
      addAddedLogsWithMetadata(addedLogsWithMetadata, currentNewChainWithReceipts);
      addRemovedLogsWithMetadata(removedLogsWithMetadata, currentOldChainWithReceipts);

      currentNewChainWithReceipts = getParentBlockWithReceipts(currentNewChainWithReceipts);
      currentOldChainWithReceipts = getParentBlockWithReceipts(currentOldChainWithReceipts);
    }
    final BlockWithReceipts commonAncestorWithReceipts = currentNewChainWithReceipts;

    // Update indexed transactions
    newTransactions.forEach(
        (blockHash, transactionsInBlock) -> {
          indexTransactionForBlock(updater, blockHash, transactionsInBlock);
          // Don't remove transactions that are being re-indexed.
          removedTransactions.removeAll(transactionsInBlock);
        });
    clearIndexedTransactionsForBlock(updater, removedTransactions);

    // Update tracked forks
    final Collection<Hash> forks = blockchainStorage.getForkHeads();
    // Old head is now a fork
    forks.add(oldChainWithReceipts.getHash());
    // Remove new chain head's parent if it was tracked as a fork
    final Optional<Hash> parentFork =
        forks.stream()
            .filter(f -> f.equals(newChainHeadWithReceipts.getHeader().getParentHash()))
            .findAny();
    parentFork.ifPresent(forks::remove);
    updater.setForkHeads(forks);

    maybeLogReorg(newChainHeadWithReceipts, oldChainWithReceipts, commonAncestorWithReceipts);

    return BlockAddedEvent.createForChainReorg(
        newChainHeadWithReceipts.getBlock(),
        newTransactions.values().stream().flatMap(Collection::stream).collect(toList()),
        removedTransactions,
        newChainHeadWithReceipts.getReceipts(),
        Stream.concat(removedLogsWithMetadata.stream(), addedLogsWithMetadata.stream())
            .collect(Collectors.toUnmodifiableList()),
        currentNewChainWithReceipts.getBlock().getHash());
  }

  private void maybeLogReorg(
      final BlockWithReceipts newChainHeadWithReceipts,
      final BlockWithReceipts oldChainWithReceipts,
      final BlockWithReceipts commonAncestorWithReceipts) {
    if ((newChainHeadWithReceipts.getNumber() - commonAncestorWithReceipts.getNumber()
                > reorgLoggingThreshold
            || oldChainWithReceipts.getNumber() - commonAncestorWithReceipts.getNumber()
                > reorgLoggingThreshold)
        && LOG.isWarnEnabled()) {
      LOG.warn(
          "Chain Reorganization +{} new / -{} old\n{}",
          newChainHeadWithReceipts.getNumber() - commonAncestorWithReceipts.getNumber(),
          oldChainWithReceipts.getNumber() - commonAncestorWithReceipts.getNumber(),
          Streams.zip(
                  Stream.of("Old", "New", "Ancestor"),
                  Stream.of(
                          oldChainWithReceipts,
                          newChainHeadWithReceipts,
                          commonAncestorWithReceipts)
                      .map(
                          blockWithReceipts ->
                              String.format(
                                  "hash: %s, height: %s",
                                  blockWithReceipts.getHash(), blockWithReceipts.getNumber())),
                  (label, values) -> String.format("%10s - %s", label, values))
              .collect(joining("\n")));
    }
  }

  @Override
  public boolean rewindToBlock(final long blockNumber) {
    return blockchainStorage.getBlockHash(blockNumber).map(this::rewindToBlock).orElse(false);
  }

  @Override
  public boolean rewindToBlock(final Hash blockHash) {
    final BlockchainStorage.Updater updater = blockchainStorage.updater();
    try {
      final BlockHeader oldBlockHeader = blockchainStorage.getBlockHeader(blockHash).get();
      final BlockWithReceipts blockWithReceipts = getBlockWithReceipts(oldBlockHeader).get();
      final Block block = blockWithReceipts.getBlock();

      var reorgEvent = handleChainReorg(updater, blockWithReceipts);
      updater.commit();
      blockAddedObservers.forEach(o -> o.onBlockAdded(reorgEvent));

      updateCacheForNewCanonicalHead(block, calculateTotalDifficulty(block.getHeader()));
      return true;
    } catch (final NoSuchElementException e) {
      // Any Optional.get() calls in this block should be present, missing data means data
      // corruption or a bug.
      updater.rollback();
      throw new IllegalStateException("Blockchain is missing data that should be present.", e);
    }
  }

  @Override
  public boolean forwardToBlock(final BlockHeader blockHeader) {
    checkArgument(
        chainHeader.getHash().equals(blockHeader.getParentHash()),
        "Supplied block header is not a child of the current chain head.");

    final BlockchainStorage.Updater updater = blockchainStorage.updater();

    try {
      final BlockWithReceipts blockWithReceipts = getBlockWithReceipts(blockHeader).get();

      BlockAddedEvent newHeadEvent = handleNewHead(updater, blockWithReceipts);
      updateCacheForNewCanonicalHead(
          blockWithReceipts.getBlock(), calculateTotalDifficulty(blockHeader));
      updater.commit();
      blockAddedObservers.forEach(observer -> observer.onBlockAdded(newHeadEvent));
      return true;
    } catch (final NoSuchElementException e) {
      // Any Optional.get() calls in this block should be present, missing data means data
      // corruption or a bug.
      updater.rollback();
      throw new IllegalStateException("Blockchain is missing data that should be present.", e);
    }
  }

  @Override
  public void setFinalized(final Hash blockHash) {
    final var updater = blockchainStorage.updater();
    updater.setFinalized(blockHash);
    updater.commit();
  }

  @Override
  public void setSafeBlock(final Hash blockHash) {
    final var updater = blockchainStorage.updater();
    updater.setSafeBlock(blockHash);
    updater.commit();
  }

  private long getFinalizedBlockNumber() {
    return this.getFinalized().flatMap(this::getBlockHeader).map(BlockHeader::getNumber).orElse(0L);
  }

  private long getSafeBlockNumber() {
    return this.getSafeBlock().flatMap(this::getBlockHeader).map(BlockHeader::getNumber).orElse(0L);
  }

  private void updateCacheForNewCanonicalHead(final Block block, final Difficulty uInt256) {
    chainHeader = block.getHeader();
    totalDifficulty = uInt256;
    chainHeadTransactionCount = block.getBody().getTransactions().size();
    chainHeadOmmerCount = block.getBody().getOmmers().size();
  }

  private static void indexTransactionForBlock(
      final BlockchainStorage.Updater updater, final Hash hash, final List<Transaction> txs) {
    for (int i = 0; i < txs.size(); i++) {
      final Hash txHash = txs.get(i).getHash();
      final TransactionLocation loc = new TransactionLocation(hash, i);
      updater.putTransactionLocation(txHash, loc);
    }
  }

  private static void clearIndexedTransactionsForBlock(
      final BlockchainStorage.Updater updater, final List<Transaction> txs) {
    for (final Transaction tx : txs) {
      updater.removeTransactionLocation(tx.getHash());
    }
  }

  @VisibleForTesting
  Set<Hash> getForks() {
    return new HashSet<>(blockchainStorage.getForkHeads());
  }

  private void setGenesis(final Block genesisBlock, final String dataDirectory) {
    checkArgument(
        genesisBlock.getHeader().getNumber() == BlockHeader.GENESIS_BLOCK_NUMBER,
        "Invalid genesis block.");
    final Optional<Hash> maybeHead = blockchainStorage.getChainHead();
    if (maybeHead.isEmpty()) {
      // Initialize blockchain store with genesis block.
      final BlockchainStorage.Updater updater = blockchainStorage.updater();
      final Hash hash = genesisBlock.getHash();
      updater.putBlockHeader(hash, genesisBlock.getHeader());
      updater.putBlockBody(hash, genesisBlock.getBody());
      updater.putTransactionReceipts(hash, emptyList());
      updater.putTotalDifficulty(hash, calculateTotalDifficulty(genesisBlock.getHeader()));
      updater.putBlockHash(genesisBlock.getHeader().getNumber(), hash);
      updater.setChainHead(hash);
      updater.commit();
    } else {
      // Verify genesis block is consistent with stored blockchain.
      final Optional<Hash> genesisHash = getBlockHashByNumber(BlockHeader.GENESIS_BLOCK_NUMBER);
      if (genesisHash.isEmpty()) {
        throw new IllegalStateException("Blockchain is missing genesis block data.");
      }
      if (!genesisHash.get().equals(genesisBlock.getHash())) {
        throw new InvalidConfigurationException(
            "Supplied genesis block does not match chain data stored in "
                + dataDirectory
                + ".\n"
                + "Please specify a different data directory with --data-path, specify the original genesis file with "
                + "--genesis-file or supply a testnet/mainnet option with --network.");
      }
    }
  }

  private boolean blockIsAlreadyTracked(final Block block) {
    if (block.getHeader().getParentHash().equals(chainHeader.getHash())) {
      // If this block builds on our chain head it would have a higher TD and be the chain head
      // but since it isn't we mustn't have imported it yet.
      // Saves a db read for the most common case
      return false;
    }
    return blockchainStorage.getBlockHeader(block.getHash()).isPresent();
  }

  private boolean blockIsConnected(final Block block) {
    return blockchainStorage.getBlockHeader(block.getHeader().getParentHash()).isPresent();
  }

  private void addAddedLogsWithMetadata(
      final List<LogWithMetadata> logsWithMetadata, final BlockWithReceipts blockWithReceipts) {
    logsWithMetadata.addAll(
        0,
        LogWithMetadata.generate(
            blockWithReceipts.getBlock(), blockWithReceipts.getReceipts(), false));
  }

  private void addRemovedLogsWithMetadata(
      final List<LogWithMetadata> logsWithMetadata, final BlockWithReceipts blockWithReceipts) {
    logsWithMetadata.addAll(
        Lists.reverse(
            LogWithMetadata.generate(
                blockWithReceipts.getBlock(), blockWithReceipts.getReceipts(), true)));
  }

  private Optional<BlockWithReceipts> getBlockWithReceipts(final BlockHeader blockHeader) {
    return blockchainStorage
        .getBlockBody(blockHeader.getHash())
        .map(body -> new Block(blockHeader, body))
        .flatMap(
            block ->
                blockchainStorage
                    .getTransactionReceipts(blockHeader.getHash())
                    .map(receipts -> new BlockWithReceipts(block, receipts)));
  }

  private BlockWithReceipts getParentBlockWithReceipts(final BlockWithReceipts blockWithReceipts) {
    return blockchainStorage
        .getBlockHeader(blockWithReceipts.getHeader().getParentHash())
        .flatMap(this::getBlockWithReceipts)
        .get();
  }

  @Override
  public long observeBlockAdded(final BlockAddedObserver observer) {
    checkNotNull(observer);
    return blockAddedObservers.subscribe(observer);
  }

  @Override
  public boolean removeObserver(final long observerId) {
    return blockAddedObservers.unsubscribe(observerId);
  }

  @Override
  public long observeChainReorg(final ChainReorgObserver observer) {
    checkNotNull(observer);
    return blockReorgObservers.subscribe(observer);
  }

  @Override
  public boolean removeChainReorgObserver(final long observerId) {
    return blockReorgObservers.unsubscribe(observerId);
  }

  @VisibleForTesting
  int observerCount() {
    return blockAddedObservers.getSubscriberCount();
  }

  private void notifyChainReorgBlockAdded(final BlockWithReceipts blockWithReceipts) {
    blockReorgObservers.forEach(observer -> observer.onBlockAdded(blockWithReceipts, this));
  }

  public Optional<Cache<Hash, BlockHeader>> getBlockHeadersCache() {
    return blockHeadersCache;
  }

  public Optional<Cache<Hash, BlockBody>> getBlockBodiesCache() {
    return blockBodiesCache;
  }

  public Optional<Cache<Hash, List<TransactionReceipt>>> getTransactionReceiptsCache() {
    return transactionReceiptsCache;
  }

  public Optional<Cache<Hash, Difficulty>> getTotalDifficultyCache() {
    return totalDifficultyCache;
  }
}
