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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ArchiveTrieNodeRollerTest {

  private final AtomicLong frontier = new AtomicLong();
  private Blockchain blockchain;
  private BonsaiWorldStateProvider provider;
  private ArchiveTrieNodeStrategy strategy;
  private ArchiveCoverageTracker coverage;
  private ScheduledExecutorService executor;
  private ArchiveTrieNodeRoller roller;
  private SegmentedKeyValueStorage storage;
  private TrieLogManager trieLogManager;
  private MetricsSystem metricsSystem;
  private final List<LogEvent> logEvents = new CopyOnWriteArrayList<>();
  private AbstractAppender appender;

  private static final long MAX_LAYERS = 512L;

  @BeforeEach
  void setUp() {
    appender =
        new AbstractAppender("roller-test-capture", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(final LogEvent event) {
            logEvents.add(event.toImmutable());
          }
        };
    appender.start();
    rollerLogger().addAppender(appender);

    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    coverage = new ArchiveCoverageTracker(storage);
    blockchain = mock(Blockchain.class);
    provider = mock(BonsaiWorldStateProvider.class);
    strategy = mock(ArchiveTrieNodeStrategy.class);
    when(strategy.getCoverageTracker()).thenReturn(coverage);
    trieLogManager = mock(TrieLogManager.class);
    when(provider.getTrieLogManager()).thenReturn(trieLogManager);
    executor = mock(ScheduledExecutorService.class);
    metricsSystem = mock(MetricsSystem.class);
    when(metricsSystem.createCounter(any(), anyString(), anyString()))
        .thenReturn(mock(Counter.class));
    roller =
        new ArchiveTrieNodeRoller(
            provider,
            blockchain,
            strategy,
            frontier::get,
            executor,
            metricsSystem,
            MAX_LAYERS,
            4,
            8,
            ArchiveTrieNodeRoller.SeedMode.HEAD_SNAPSHOT);
  }

  @AfterEach
  void tearDown() {
    rollerLogger().removeAppender(appender);
    appender.stop();
  }

  @SuppressWarnings("BannedMethod")
  private static Logger rollerLogger() {
    return (Logger) LogManager.getLogger(ArchiveTrieNodeRoller.class);
  }

  private long logCount(final Level level) {
    return logEvents.stream().filter(e -> level.equals(e.getLevel())).count();
  }

  /** Stubs a canonical header at {@code number}, with or without a stored trie log. */
  private BlockHeader headerAt(final long number, final boolean hasTrieLog) {
    final BlockHeader header = new BlockHeaderTestFixture().number(number).buildHeader();
    when(blockchain.getBlockHeader(number)).thenReturn(Optional.of(header));
    when(trieLogManager.getTrieLogLayer(header.getHash()))
        .thenReturn(hasTrieLog ? Optional.of(mock(TrieLog.class)) : Optional.empty());
    return header;
  }

  private void coverageAt(final long block) {
    for (long b = coverage.lastIndexedBlock().orElse(-1L) + 1L; b <= block; b++) {
      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      coverage.record(tx, b);
      tx.commit();
    }
  }

  @Test
  void doesNotScheduleWhenCoverageEmpty() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);
    frontier.set(100L);
    roller.scheduleCatchUpIfNeeded();
    verifyNoInteractions(executor);
  }

  @Test
  void doesNotScheduleBelowMinBatch() {
    coverageAt(10L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);
    frontier.set(13L); // gap 3 < minBatch 4
    roller.scheduleCatchUpIfNeeded();
    verify(executor, never()).execute(any());
  }

  @Test
  void targetIsCappedAtLocalHead() {
    coverageAt(10L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(12L);
    frontier.set(1_000L); // frontier ahead of local head: target = 12, gap 2 < 4
    roller.scheduleCatchUpIfNeeded();
    verify(executor, never()).execute(any());
  }

  @Test
  void schedulesOnceWhileACatchUpIsPending() {
    coverageAt(10L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);
    frontier.set(20L);
    roller.scheduleCatchUpIfNeeded();
    roller.scheduleCatchUpIfNeeded();
    verify(executor, times(1)).execute(any());
  }

  @Test
  void closeStopsFurtherCatchUp() {
    coverageAt(10L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);
    frontier.set(20L);
    roller.close();

    roller.scheduleCatchUpIfNeeded();
    roller.catchUp();

    verify(executor, never()).execute(any());
    verify(executor).shutdownNow();
    verifyNoInteractions(provider);
  }

  @Test
  void catchUpFailureIsContainedAndLeavesCoverage() {
    coverageAt(10L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);
    headerAt(11L, true);
    when(blockchain.getBlockHeader(10L)).thenReturn(Optional.empty());
    frontier.set(20L);

    roller.catchUp(); // missing cursor header → logged, not thrown

    assertThat(coverage.lastIndexedBlock()).hasValue(10L);
    assertThat(roller.isStopped()).isFalse();
  }

  @Test
  void missingNextTrieLogStopsTheRollerWithoutSeeding() {
    // snap-synced node: genesis indexed, no trie logs below the pivot
    coverageAt(0L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);
    headerAt(1L, false);
    frontier.set(100L);

    roller.catchUp();
    roller.catchUp();
    roller.scheduleCatchUpIfNeeded();

    verify(provider, never()).rollWorldStateToBlockHash(any(), any());
    verify(provider, never()).createUncachedWorldState(any());
    verify(executor, never()).execute(any());
    assertThat(roller.isStopped()).isTrue();
    assertThat(coverage.lastIndexedBlock()).hasValue(0L);
    assertThat(logCount(Level.WARN)).isEqualTo(1L);
    assertThat(logCount(Level.ERROR)).isZero();
  }

  @Test
  void missingNextHeaderStopsTheRollerWithoutSeeding() {
    coverageAt(10L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);
    when(blockchain.getBlockHeader(11L)).thenReturn(Optional.empty());
    frontier.set(100L);

    roller.catchUp();

    verify(provider, never()).rollWorldStateToBlockHash(any(), any());
    assertThat(roller.isStopped()).isTrue();
    assertThat(logCount(Level.WARN)).isEqualTo(1L);
  }

  @Test
  void gapBeyondHistoricalBlockLimitStopsTheRollerWithoutSeeding() {
    coverageAt(10L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(10L + MAX_LAYERS + 1L);
    headerAt(11L, true);
    frontier.set(10L + MAX_LAYERS);

    roller.catchUp();
    roller.catchUp();
    roller.scheduleCatchUpIfNeeded();

    verify(provider, never()).rollWorldStateToBlockHash(any(), any());
    verify(provider, never()).createUncachedWorldState(any());
    verify(executor, never()).execute(any());
    assertThat(roller.isStopped()).isTrue();
    assertThat(coverage.lastIndexedBlock()).hasValue(10L);
    assertThat(logCount(Level.WARN)).isEqualTo(1L);
    assertThat(logCount(Level.ERROR)).isZero();
  }

  @Test
  void gapAtHistoricalBlockLimitStillSeeds() {
    coverageAt(10L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(10L + MAX_LAYERS);
    headerAt(11L, true);
    when(blockchain.getBlockHeader(10L)).thenReturn(Optional.empty()); // fail right after the cap
    frontier.set(10L + MAX_LAYERS);

    roller.catchUp();

    verify(blockchain).getBlockHeader(10L); // reached the seed
    assertThat(roller.isStopped()).isFalse();
  }

  @Test
  void startRegistersCanonicalHeadObserver() {
    roller.start();
    verify(blockchain).observeBlockAdded(any());
  }

  /** Coverage at 10, head 100: every catch-up fails because the cursor header is missing. */
  private void failingCatchUpSetup() {
    coverageAt(10L);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);
    headerAt(11L, true);
    when(blockchain.getBlockHeader(10L)).thenReturn(Optional.empty());
    frontier.set(20L);
  }

  @Test
  void failedCatchUpBacksOffExponentiallyInHeadBlocks() {
    failingCatchUpSetup();

    roller.catchUp(); // failure 1 at head 100: retry once head reaches 101
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);
    roller.scheduleCatchUpIfNeeded();
    verify(executor, never()).execute(any());
    when(blockchain.getChainHeadBlockNumber()).thenReturn(101L);
    roller.scheduleCatchUpIfNeeded();
    verify(executor, times(1)).execute(any());

    roller.catchUp(); // failure 2 at head 101: retry once head reaches 103
    when(blockchain.getChainHeadBlockNumber()).thenReturn(102L);
    roller.scheduleCatchUpIfNeeded();
    verify(executor, times(1)).execute(any());
    when(blockchain.getChainHeadBlockNumber()).thenReturn(103L);
    roller.scheduleCatchUpIfNeeded();
    verify(executor, times(2)).execute(any());
  }

  @Test
  void backoffIsCappedAt1024Blocks() {
    failingCatchUpSetup();
    for (int i = 0; i < 15; i++) {
      roller.catchUp();
    }
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L + 1023L);
    roller.scheduleCatchUpIfNeeded();
    verify(executor, never()).execute(any());
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L + 1024L);
    roller.scheduleCatchUpIfNeeded();
    verify(executor, times(1)).execute(any());
  }

  @Test
  void successfulCatchUpResetsBackoff() {
    failingCatchUpSetup();
    roller.catchUp();
    roller.catchUp(); // two failures at head 100: retry at 102

    frontier.set(12L); // gap 2 < minBatch: a catch-up that completes without failing
    roller.catchUp();

    frontier.set(20L);
    roller.scheduleCatchUpIfNeeded(); // still head 100, but the streak is over
    verify(executor, times(1)).execute(any());
  }

  @Test
  void onlyTheFirstFailureOfAStreakLogsAnErrorWithStackTrace() {
    failingCatchUpSetup();

    roller.catchUp();
    roller.catchUp();
    roller.catchUp();

    assertThat(logEvents.stream().filter(e -> Level.ERROR.equals(e.getLevel())))
        .singleElement()
        .satisfies(e -> assertThat(e.getThrown()).isNotNull());
    assertThat(logEvents.stream().filter(e -> Level.WARN.equals(e.getLevel())))
        .hasSize(2)
        .allSatisfy(e -> assertThat(e.getThrown()).isNull());
  }

  @Test
  void registersLastIndexedBlockGauge() {
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<LongSupplier> gauge = ArgumentCaptor.forClass(LongSupplier.class);
    verify(metricsSystem)
        .createLongGauge(
            eq(BesuMetricCategory.BLOCKCHAIN),
            eq("bonsai_archive_proofs_last_indexed_block"),
            any(),
            gauge.capture());
    coverageAt(42L);
    assertThat(gauge.getValue().getAsLong()).isEqualTo(42L);
  }
}
