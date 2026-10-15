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
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiArchiveWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeRollerIntegrationTest {

  private final AtomicLong frontier = new AtomicLong(Long.MAX_VALUE);
  private BlockchainSetupUtil chain;
  private BonsaiWorldStateKeyValueStorage storage;
  private BonsaiArchiveWorldStateProvider provider;
  private ArchiveTrieNodeStrategy strategy;
  private ArchiveTrieNodeRoller roller;
  private long maxLayersToLoad;

  @AfterEach
  void tearDown() {
    if (roller != null) {
      roller.close();
    }
    if (strategy != null) {
      strategy.close();
    }
  }

  private void setUpChain(final long maxLayersToLoad) {
    this.maxLayersToLoad = maxLayersToLoad;
    final DataStorageConfiguration config =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .extraStorageConfiguration(
                ImmutableExtraStorageConfiguration.builder()
                    .maxLayersToLoad(maxLayersToLoad)
                    .unstable(
                        ImmutableExtraStorageConfiguration.Unstable.builder()
                            .bonsaiArchiveStateProofsEnabled(true)
                            .build())
                    .build())
            .build();
    chain =
        BlockchainSetupUtil.forTesting(
            blockchain -> {
              storage =
                  new BonsaiWorldStateKeyValueStorage(
                      new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), config);
              provider =
                  new BonsaiArchiveWorldStateProvider(
                      storage,
                      blockchain,
                      config,
                      new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
                      null,
                      EvmConfiguration.DEFAULT,
                      new BonsaiCodeCache(),
                      new NoOpMetricsSystem());
              strategy =
                  ArchiveTrieNodeStrategy.createArchiveStrategy(
                      storage.getComposedWorldStateStorage(),
                      Executors.newFixedThreadPool(2),
                      DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
                      DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL);
              storage.setTrieNodeStrategy(strategy);
              strategy.armFrontier(frontier::get);
              return provider;
            });
  }

  private ArchiveTrieNodeRoller roller(final int maxBatch) {
    return roller(1, maxBatch);
  }

  private ArchiveTrieNodeRoller roller(final int minBatch, final int maxBatch) {
    return new ArchiveTrieNodeRoller(
        provider,
        chain.getBlockchain(),
        strategy,
        frontier::get,
        Executors.newSingleThreadScheduledExecutor(),
        new NoOpMetricsSystem(),
        maxLayersToLoad,
        minBatch,
        maxBatch);
  }

  private long lastIndexed() {
    return strategy.getCoverageTracker().lastIndexedBlock().orElse(-1L);
  }

  private long head() {
    return chain.getBlockchain().getChainHeadBlockNumber();
  }

  private BlockHeader header(final long number) {
    return chain.getBlockchain().getBlockHeader(number).orElseThrow();
  }

  /** Storage slots requested in every full proof comparison. */
  private static final List<UInt256> STORAGE_SLOTS =
      LongStream.rangeClosed(0, 7).mapToObj(UInt256::valueOf).toList();

  /**
   * Addresses touched by the test chain: block coinbases, transaction senders, recipients and
   * created contracts.
   */
  private Set<Address> touchedAddresses() {
    final Set<Address> addresses = new LinkedHashSet<>();
    for (final Block block : chain.getBlocks()) {
      addresses.add(block.getHeader().getCoinbase());
      for (final Transaction tx : block.getBody().getTransactions()) {
        addresses.add(tx.getSender());
        tx.getTo()
            .ifPresentOrElse(
                addresses::add,
                () -> addresses.add(Address.contractAddress(tx.getSender(), tx.getNonce())));
      }
    }
    return addresses;
  }

  /** Structural view of a {@link WorldStateProof}, which does not implement equals. */
  private record ProofView(
      List<Bytes> accountProof,
      Map<UInt256, List<Bytes>> storageProofs,
      Map<UInt256, UInt256> storageValues) {

    static ProofView of(final WorldStateProof proof) {
      final Map<UInt256, List<Bytes>> storageProofs = new LinkedHashMap<>();
      final Map<UInt256, UInt256> storageValues = new LinkedHashMap<>();
      for (final UInt256 slot : STORAGE_SLOTS) {
        storageProofs.put(slot, proof.getStorageProof(slot));
        storageValues.put(slot, proof.getStorageValue(slot));
      }
      return new ProofView(proof.getAccountProof(), storageProofs, storageValues);
    }

    boolean hasNonTrivialStorage() {
      return storageValues.values().stream().anyMatch(value -> !value.isZero())
          || storageProofs.values().stream().anyMatch(nodes -> nodes.size() > 1);
    }
  }

  private Optional<ProofView> archiveFullProof(final long block, final Address address) {
    return provider.getAccountProof(
        header(block), address, STORAGE_SLOTS, maybeProof -> maybeProof.map(ProofView::of));
  }

  /** Oracle: the full proof (account and storage slots) from a trie-log-rolled world state. */
  private ProofView rolledFullProof(final long block, final Address address) {
    final BlockHeader blockHeader = header(block);
    try (var worldState =
        provider.getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader)).orElseThrow()) {
      final BonsaiWorldStateKeyValueStorage rolledStorage =
          ((BonsaiWorldState) worldState).getWorldStateStorage();
      return ProofView.of(
          new WorldStateProofProvider(new WorldStateStorageCoordinator(rolledStorage))
              .getAccountProof(blockHeader.getStateRoot(), address, STORAGE_SLOTS)
              .orElseThrow());
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Asserts {@code block} is served from the archive (not the trie-log fallback) and that every
   * touched address's full proof matches the rolled oracle byte for byte.
   *
   * @return true if any compared proof carried non-trivial storage
   */
  private boolean assertArchiveProofsMatchRolled(final long block) {
    assertThat(strategy.getCoverageTracker().hasArchiveBlock(block))
        .as("archive coverage for block %d", block)
        .isTrue();
    boolean sawStorage = false;
    for (final Address address : touchedAddresses()) {
      final ProofView rolled = rolledFullProof(block, address);
      assertThat(archiveFullProof(block, address))
          .as("block %d address %s", block, address)
          .contains(rolled);
      sawStorage |= rolled.hasNonTrivialStorage();
    }
    return sawStorage;
  }

  private Optional<List<Bytes>> archiveAccountProof(final long block, final Address address) {
    return provider.getAccountProof(
        header(block),
        address,
        List.of(),
        maybeProof -> maybeProof.map(WorldStateProof::getAccountProof));
  }

  private Map<Bytes, Bytes> segmentContents(final SegmentIdentifier segment) {
    final Map<Bytes, Bytes> contents = new HashMap<>();
    storage.getComposedWorldStateStorage().stream(segment)
        .forEach(pair -> contents.put(Bytes.wrap(pair.getKey()), Bytes.wrap(pair.getValue())));
    return contents;
  }

  @Test
  void inlineCaptureTracksHeadWhileFrontierIsAhead() {
    setUpChain(512L);
    chain.importAllBlocks();
    assertThat(lastIndexed()).isEqualTo(head());
  }

  @Test
  void inlineCaptureStopsAtFrontierAndRollerCatchesUp() {
    setUpChain(512L);
    frontier.set(10L);
    chain.importAllBlocks();
    assertThat(lastIndexed()).isEqualTo(10L);

    frontier.set(Long.MAX_VALUE);
    roller = roller(5);
    roller.catchUp();

    assertThat(lastIndexed()).isEqualTo(head());
  }

  @Test
  void archiveProofsMatchRolledStateAcrossHandoffAndBatchBoundaries() {
    setUpChain(512L);
    frontier.set(10L);
    chain.importAllBlocks();
    frontier.set(Long.MAX_VALUE);
    roller = roller(5); // reseeds every 5 blocks: boundaries at 15/16, 20/21, ...
    roller.catchUp();
    assertThat(lastIndexed()).isEqualTo(head());

    boolean sawStorage = false;
    for (long block = 1; block <= head(); block++) {
      sawStorage |= assertArchiveProofsMatchRolled(block);
    }
    assertThat(sawStorage).as("at least one compared proof carries non-trivial storage").isTrue();
  }

  @Test
  void rollerLeavesHeadTrieStorageUntouched() {
    setUpChain(512L);
    frontier.set(10L);
    chain.importAllBlocks();
    final Map<Bytes, Bytes> headTrieBefore = segmentContents(TRIE_BRANCH_STORAGE);
    final Hash headRootBefore = provider.getWorldState().rootHash();

    frontier.set(Long.MAX_VALUE);
    roller = roller(5);
    roller.catchUp();

    assertThat(segmentContents(TRIE_BRANCH_STORAGE)).isEqualTo(headTrieBefore);
    assertThat(provider.getWorldState().rootHash()).isEqualTo(headRootBefore);
  }

  @Test
  void historicalQueriesDoNotCapture() throws Exception {
    setUpChain(512L);
    frontier.set(10L);
    chain.importAllBlocks();
    frontier.set(
        Long.MAX_VALUE); // gate would now accept block 11 — but only from live/roller storage
    final Map<Bytes, Bytes> archiveBefore = segmentContents(TRIE_BRANCH_STORAGE_ARCHIVE);

    for (final long block : new long[] {5L, 11L, 12L, head() - 1}) {
      final var worldState =
          provider.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header(block)));
      assertThat(worldState).as("historical world state at block %d", block).isPresent();
      worldState.get().close();
    }

    assertThat(segmentContents(TRIE_BRANCH_STORAGE_ARCHIVE)).isEqualTo(archiveBefore);
    assertThat(lastIndexed()).isEqualTo(10L);
  }

  @Test
  void restartedRollerResumesFromCoverage() {
    setUpChain(512L);
    frontier.set(10L);
    chain.importAllBlocks();

    frontier.set(15L);
    roller = roller(5);
    roller.catchUp();
    roller.close();
    assertThat(lastIndexed()).isEqualTo(15L);

    frontier.set(Long.MAX_VALUE);
    roller = roller(5);
    roller.catchUp();
    assertThat(lastIndexed()).isEqualTo(head());
    boolean sawStorage = false;
    for (long block = 13; block <= 18; block++) {
      sawStorage |= assertArchiveProofsMatchRolled(block);
    }
    assertThat(sawStorage).as("at least one compared proof carries non-trivial storage").isTrue();
  }

  /** Runs {@code action} and returns the roller's INFO messages. */
  @SuppressWarnings("BannedMethod")
  private List<String> rollerInfoLogs(final Runnable action) {
    final Logger logger = (Logger) LogManager.getLogger(ArchiveTrieNodeRoller.class);
    final List<LogEvent> events = new CopyOnWriteArrayList<>();
    final AbstractAppender appender =
        new AbstractAppender("roller-it-capture", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(final LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    try {
      action.run();
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
    return events.stream()
        .filter(e -> Level.INFO.equals(e.getLevel()))
        .map(e -> e.getMessage().getFormattedMessage())
        .toList();
  }

  @Test
  void completedCatchUpLogsIndexedRangeOnceAtInfo() {
    setUpChain(512L);
    frontier.set(10L);
    chain.importAllBlocks();

    frontier.set(Long.MAX_VALUE);
    roller = roller(5); // several batches, one catch-up run
    final List<String> infos = rollerInfoLogs(roller::catchUp);

    assertThat(infos)
        .containsExactly(
            "Bonsai archive proofs indexed blocks 11.."
                + head()
                + " (frontier "
                + Long.MAX_VALUE
                + ", head "
                + head()
                + ")");
  }

  @Test
  void stopsWhenGapExceedsHistoricalBlockLimit() {
    setUpChain(8L); // the roller's seed span is capped at --bonsai-historical-block-limit
    frontier.set(4L);
    chain.importAllBlocks();
    assertThat(head() - lastIndexed()).isGreaterThan(8L);
    final Map<Bytes, Bytes> archiveBefore = segmentContents(TRIE_BRANCH_STORAGE_ARCHIVE);

    frontier.set(Long.MAX_VALUE);
    roller = roller(256);
    roller.catchUp();

    assertThat(roller.isStopped()).isTrue();
    assertThat(lastIndexed()).isEqualTo(4L);
    assertThat(segmentContents(TRIE_BRANCH_STORAGE_ARCHIVE)).isEqualTo(archiveBefore);
  }

  @Test
  void catchesUpWhenGapIsWithinHistoricalBlockLimit() {
    setUpChain(8L); // historical queries refuse blocks more than 8 behind head
    frontier.set(chain.getBlock(chain.blockCount() - 1).getHeader().getNumber() - 8L);
    chain.importAllBlocks();
    final long start = lastIndexed();
    assertThat(head() - start).isEqualTo(8L);

    frontier.set(Long.MAX_VALUE);
    roller = roller(256);
    roller.catchUp();

    assertThat(lastIndexed()).isEqualTo(head());
    // archive proof roots must hash to the header state root for every recovered block
    for (long block = start + 1; block <= head(); block++) {
      for (final Address address : touchedAddresses()) {
        final List<Bytes> proof = archiveAccountProof(block, address).orElseThrow();
        assertThat(Hash.hash(proof.get(0))).isEqualTo(header(block).getStateRoot());
      }
    }
  }

  @Test
  void residualGapBelowMinBatchDoesNotReseed() {
    setUpChain(512L);
    frontier.set(10L);
    chain.importAllBlocks();

    frontier.set(18L); // one batch 11..15, then a residual gap of 3 < minBatch 4
    roller = roller(4, 5);
    roller.catchUp();

    assertThat(lastIndexed()).isEqualTo(15L);
  }

  @Test
  void missingNextTrieLogStopsTheRoller() {
    setUpChain(512L);
    frontier.set(10L);
    chain.importAllBlocks();
    final var trieLogTx = storage.getTrieLogStorage().startTransaction();
    trieLogTx.remove(header(11L).getHash().getBytes().toArrayUnsafe());
    trieLogTx.commit();
    final Map<Bytes, Bytes> archiveBefore = segmentContents(TRIE_BRANCH_STORAGE_ARCHIVE);

    frontier.set(Long.MAX_VALUE);
    roller = roller(256);
    roller.catchUp();

    assertThat(roller.isStopped()).isTrue();
    assertThat(lastIndexed()).isEqualTo(10L);
    assertThat(segmentContents(TRIE_BRANCH_STORAGE_ARCHIVE)).isEqualTo(archiveBefore);
  }

  @Test
  void missingTrieLogLeavesCoverageUnchanged() {
    setUpChain(512L);
    frontier.set(10L);
    chain.importAllBlocks();
    final var trieLogTx = storage.getTrieLogStorage().startTransaction();
    trieLogTx.remove(header(14L).getHash().getBytes().toArrayUnsafe());
    trieLogTx.commit();
    final Map<Bytes, Bytes> archiveBefore = segmentContents(TRIE_BRANCH_STORAGE_ARCHIVE);

    frontier.set(Long.MAX_VALUE);
    roller = roller(256);
    roller.catchUp(); // seeding needs block 14's trie log → fails, logged

    assertThat(lastIndexed()).isEqualTo(10L);
    assertThat(segmentContents(TRIE_BRANCH_STORAGE_ARCHIVE)).isEqualTo(archiveBefore);
  }
}
