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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.ImmutableCheckpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.QueuedExecutor;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Runs the downloads of the chain on the test thread: the header download when the test runs its
 * executor, a download that failed starting again when it runs the resumes, and the window when it
 * runs the workers of the window. A peer answers from a chain of empty blocks.
 */
class SyncChainTest {

  private static final int HEAD = 30;
  // the first proof-of-stake block, the checkpoint of the tests that have one
  private static final int MERGE = 6;
  private static final int PIVOT = 20;
  private static final int WINDOW = 8;

  private final BlockDataGenerator gen = new BlockDataGenerator(7);
  private final List<Block> blocks = chain();
  private final MutableBlockchain source =
      InMemoryKeyValueStorageProvider.createInMemoryBlockchain(blocks.getFirst());
  private final MutableBlockchain blockchain =
      spy(InMemoryKeyValueStorageProvider.createInMemoryBlockchain(blocks.getFirst()));
  private final ChainSender sender = new ChainSender(source);
  private final QueuedExecutor executor = new QueuedExecutor();
  private final QueuedExecutor resumes = new QueuedExecutor();
  private final QueuedExecutor windowWorkers = new QueuedExecutor();
  private final List<Long> pauses = new ArrayList<>();

  @BeforeEach
  void serveTheChain() {
    blocks.subList(1, blocks.size()).forEach(block -> source.appendBlock(block, List.of()));
  }

  @Test
  void storesTheHeadersDownToGenesisAndLeavesTheHeadThere() {
    final SyncChain chain = chain(Optional.empty(), false);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertHeadersStored(1, PIVOT);
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void storesTheCheckpointThenTheHeadersDownToIt() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    // downloaded, then the head until the bodies above it are stored
    assertThat(blockchain.getChainHeadHeader()).isEqualTo(header(MERGE));
    assertHeadersStored(MERGE, PIVOT);
    assertThat(blockchain.getBlockHeader(MERGE - 1)).isEmpty();
    // above a proof-of-stake checkpoint, every header has the total difficulty of the checkpoint
    assertThat(blockchain.getTotalDifficultyByHash(header(PIVOT - 1).getHash()))
        .contains(totalDifficulty(MERGE));
  }

  @Test
  void storesTheProofOfWorkHeadersBelowTheCheckpointWithoutTotalDifficulty() {
    final SyncChain chain = chain(Optional.of(checkpoint()), false);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertHeadersStored(1, PIVOT);
    assertThat(blockchain.getTotalDifficultyByHash(header(MERGE - 2).getHash())).isEmpty();
  }

  @Test
  void storesTheHeadersAboveTheLastProofOfWorkBlockWithItsTotalDifficulty() {
    // its difficulty is not zero, as with a genesis of difficulty 1 followed by proof of stake
    final SyncChain chain =
        chain(
            Optional.of(
                checkpoint(MERGE - 1, header(MERGE - 1).getHash(), totalDifficulty(MERGE - 1))),
            true);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertThat(blockchain.getTotalDifficultyByHash(header(PIVOT - 1).getHash()))
        .contains(totalDifficulty(MERGE - 1));
  }

  @Test
  void failsWhenTheCheckpointIsNotOnTheChainOfThePivot() {
    // a checkpoint of another chain at the same height, stored already
    final BlockHeader other =
        new BlockHeaderTestFixture().number(MERGE).difficulty(Difficulty.ZERO).buildHeader();
    blockchain.unsafeStoreHeader(other, Difficulty.ONE);
    final SyncChain chain =
        chain(Optional.of(checkpoint(MERGE, other.getHash(), Difficulty.ONE)), false);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone().exceptionNow()).isInstanceOf(WrongChainException.class);
  }

  @Test
  void failsOnProofOfWorkHeadersAboveAProofOfStakeCheckpoint() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);
    chain.start(header(PIVOT));
    final BlockHeader proofOfWork =
        new BlockHeaderTestFixture()
            .number(PIVOT + 1)
            .parentHash(header(PIVOT).getHash())
            .difficulty(Difficulty.ONE)
            .buildHeader();

    assertThatThrownBy(() -> chain.storeHeaders(List.of(proofOfWork)))
        .isInstanceOf(WrongChainException.class);
    assertThat(blockchain.getBlockHeader(PIVOT + 1)).isEmpty();
  }

  @Test
  void startsOverWhenPeersDoNotServeTheCheckpoint() {
    final SyncChain chain = chain(Optional.of(checkpoint(MERGE, Hash.ZERO, Difficulty.ONE)), true);

    assertThatThrownBy(() -> chain.start(header(PIVOT))).isInstanceOf(RestartException.class);
  }

  @Test
  void rejectsACheckpointAtAnotherHeight() {
    final SyncChain chain =
        chain(
            Optional.of(checkpoint(MERGE + 1, header(MERGE).getHash(), totalDifficulty(MERGE))),
            true);

    assertThatThrownBy(() -> chain.start(header(PIVOT))).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void rejectsAPivotBelowTheCheckpoint() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);

    assertThatThrownBy(() -> chain.start(header(MERGE - 1)))
        .isInstanceOf(IllegalStateException.class);
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void resumesTheHeaderDownloadBelowTheLowestHeaderStored() {
    final SyncChain chain = chain(Optional.empty(), false);
    // stored down to block 12, as the requests below it, and those by hash, fail
    sender.fail(message -> startOf(message).orElse(0) < 12);
    chain.start(header(PIVOT));
    executor.runAll();
    assertHeadersStored(12, PIVOT);
    assertThat(blockchain.getBlockHeader(11)).isEmpty();
    final int asked = sender.requests().size();

    sender.fail(message -> false);
    resumes.runAll();
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertHeadersStored(1, PIVOT);
    assertThat(sender.requests().subList(asked, sender.requests().size()))
        .allMatch(message -> startOf(message).orElseThrow() < 12);
  }

  @Test
  void removesTheCanonicalEntriesAboveTheFirstPivotOnceTheHeadIsOnTheFloor() {
    // left by an earlier run of the node, its chain head on one of them
    blockchain.storeBlockHeaders(headers(PIVOT + 1, PIVOT + 5));
    blockchain.unsafeSetChainHead(header(PIVOT + 3), Difficulty.ONE);

    chain(Optional.empty(), false).start(header(PIVOT));

    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
    assertThat(blockchain.getBlockHeader(PIVOT + 1)).isEmpty();
    assertThat(blockchain.getBlockHeader(PIVOT + 5)).isEmpty();
    final InOrder order = inOrder(blockchain);
    order.verify(blockchain).unsafeSetChainHead(eq(header(0)), any());
    order.verify(blockchain).unsafeRemoveCanonicalIndexRange(PIVOT, PIVOT + 5);
  }

  @Test
  void downloadsTheBodiesOfTheWindowOnceTheHeadersAreStored() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    final CompletableFuture<Void> window = chain.downloadWindow(header(PIVOT));
    windowWorkers.runAll();
    assertThat(window).isNotDone();

    executor.runAll();
    windowWorkers.runAll();

    assertThat(window).isCompleted();
    final long from = PIVOT - WINDOW + 1;
    for (long number = from; number <= PIVOT; number++) {
      assertThat(blockchain.getBlockBody(header(number).getHash())).isPresent();
    }
    assertThat(blockchain.getBlockBody(header(from - 1).getHash())).isEmpty();
    assertThat(blockchain.getBodiesFrom()).contains(from);
  }

  @Test
  void downloadsTheWindowOnce() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    chain.downloadWindow(header(PIVOT));
    executor.runAll();

    final CompletableFuture<Void> second = chain.downloadWindow(header(PIVOT));
    executor.runAll();

    assertThat(second.exceptionNow()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void setsTheChainHeadAtTheFinalPivot() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    executor.runAll();

    chain.setChainHead(header(PIVOT));

    assertThat(blockchain.getChainHeadHeader()).isEqualTo(header(PIVOT));
    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(totalDifficulty(PIVOT));
  }

  @Test
  void edgeCase2_aGenesisFloorBelowTheTerminalTotalStoresNoTotalsThenTheSlowPathComputesThem() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    executor.runAll();

    // no total above a proof-of-work floor, so the fast path cannot match
    for (long number = 1; number <= PIVOT; number++) {
      assertThat(blockchain.getTotalDifficultyByHash(header(number).getHash())).isEmpty();
    }

    chain.setChainHead(header(PIVOT));

    for (long number = 1; number <= PIVOT; number++) {
      assertThat(blockchain.getTotalDifficultyByHash(header(number).getHash()))
          .contains(totalDifficulty(number));
    }
  }

  @Test
  void edgeCase2_aProofOfStakeCheckpointStoresItsTotalSoTheFastPathMatches() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);
    chain.start(header(PIVOT));
    executor.runAll();
    assertThat(blockchain.getTotalDifficultyByHash(header(PIVOT).getHash()))
        .contains(totalDifficulty(MERGE));

    chain.setChainHead(header(PIVOT));

    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(totalDifficulty(PIVOT));
  }

  @Test
  void edgeCase4_theHeaderDownloadOverwritesAStaleCanonicalEntryBelowThePivot() {
    // left by an earlier attempt: another block at PIVOT - 3, with the checkpoint's total
    final BlockHeader stale =
        new BlockHeaderTestFixture()
            .number(PIVOT - 3)
            .parentHash(Hash.ZERO)
            .difficulty(Difficulty.ZERO)
            .buildHeader();
    blockchain.storeBlockHeaders(List.of(stale), totalDifficulty(MERGE));
    final SyncChain chain = chain(Optional.of(checkpoint()), true);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertThat(blockchain.getBlockHashByNumber(PIVOT - 3)).contains(header(PIVOT - 3).getHash());
  }

  @Test
  void edgeCase4_aResumedHeaderDownloadOverwritesAStaleCanonicalEntry() {
    final BlockHeader stale =
        new BlockHeaderTestFixture()
            .number(10)
            .parentHash(Hash.ZERO)
            .difficulty(Difficulty.ZERO)
            .buildHeader();
    blockchain.storeBlockHeaders(List.of(stale), totalDifficulty(MERGE));
    final SyncChain chain = chain(Optional.of(checkpoint()), true);
    // all but the first request by hash, which fetches the checkpoint
    final java.util.concurrent.atomic.AtomicInteger byHash =
        new java.util.concurrent.atomic.AtomicInteger();
    sender.fail(
        message ->
            startOf(message).isEmpty()
                ? byHash.incrementAndGet() > 1
                : startOf(message).getAsLong() < 12);
    chain.start(header(PIVOT));
    executor.runAll();
    assertThat(blockchain.getBlockHashByNumber(10)).contains(stale.getHash());

    sender.fail(message -> false);
    resumes.runAll();
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertThat(blockchain.getBlockHashByNumber(10)).contains(header(10).getHash());
  }

  @Test
  void edgeCase7_aChainThatDoesNotLinkFailsWithoutRetrying() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    executor.runAll();
    doThrow(new IllegalStateException("block 5 does not follow the canonical chain"))
        .when(blockchain)
        .unsafeSetSyncChainHead(any(), any());

    assertThatThrownBy(() -> chain.setChainHead(header(PIVOT)))
        .isInstanceOf(IllegalStateException.class);
    verify(blockchain, times(1)).unsafeSetSyncChainHead(any(), any());
    assertThat(pauses).isEmpty();
  }

  @Test
  void edgeCase7_aStorageFailureOfTheSyncChainHeadIsRetried() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    executor.runAll();
    doThrow(new StorageException("not committed"))
        .doCallRealMethod()
        .when(blockchain)
        .unsafeSetSyncChainHead(any(), any());

    chain.setChainHead(header(PIVOT));

    verify(blockchain, times(2)).unsafeSetSyncChainHead(any(), any());
    assertThat(blockchain.getChainHeadHeader()).isEqualTo(header(PIVOT));
  }

  @Test
  void triesAWriteAgainUntilItCommits() {
    doThrow(new StorageException("not committed"))
        .doThrow(new StorageException("not committed"))
        .doCallRealMethod()
        .when(blockchain)
        .storeBlockHeaders(anyList());

    chain(Optional.empty(), false).start(header(PIVOT));

    assertThat(pauses).containsExactly(20L, 40L);
    assertThat(blockchain.getBlockHeader(PIVOT)).contains(header(PIVOT));
  }

  @Test
  void givesUpOnAWriteThatNeverCommits() {
    doThrow(new StorageException("disk failure")).when(blockchain).unsafeSetChainHead(any(), any());
    final SyncChain chain = chain(Optional.empty(), false);

    assertThatThrownBy(() -> chain.start(header(PIVOT))).isInstanceOf(StorageException.class);
    verify(blockchain, times(6)).unsafeSetChainHead(any(), any());
    assertThat(pauses).containsExactly(20L, 40L, 80L, 160L, 320L);
  }

  @Test
  void startsNothingOnceCancelled() {
    final SyncChain chain = chain(Optional.empty(), false);
    final SyncChain toCheckpoint = chain(Optional.of(checkpoint()), true);
    chain.cancel();
    toCheckpoint.cancel();

    assertThatThrownBy(() -> chain.start(header(PIVOT))).isInstanceOf(CancellationException.class);
    assertThatThrownBy(() -> toCheckpoint.start(header(PIVOT)))
        .isInstanceOf(CancellationException.class);
    // not even the checkpoint is downloaded
    assertThat(sender.requests()).isEmpty();
    assertThat(blockchain.getBlockHeader(PIVOT)).isEmpty();
  }

  @Test
  void storesNothingOnceCancelled() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    final CompletableFuture<Void> window = chain.downloadWindow(header(PIVOT));

    chain.cancel();
    executor.runAll();
    windowWorkers.runAll();

    // only the pivot
    assertThat(blockchain.getBlockHeader(PIVOT)).isPresent();
    assertThat(blockchain.getBlockHeader(PIVOT - 1)).isEmpty();
    assertThat(chain.headersDone()).isCancelled();
    assertThat(window).isCompletedExceptionally();
    assertThatThrownBy(() -> chain.storeHeaders(headers(PIVOT + 1, PIVOT + 2)))
        .isInstanceOf(CancellationException.class);
    assertThatThrownBy(() -> chain.setChainHead(header(PIVOT)))
        .isInstanceOf(CancellationException.class);
  }

  @Test
  void stopsTheWindowOnceCancelled() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    final CompletableFuture<Void> window = chain.downloadWindow(header(PIVOT));
    executor.runAll();

    chain.cancel();
    windowWorkers.runAll();

    assertThat(window).isCompletedExceptionally();
    assertThat(blockchain.getBlockBody(header(PIVOT).getHash())).isEmpty();
  }

  @Test
  void resumesNothingOnceCancelled() {
    final SyncChain chain = chain(Optional.empty(), false);
    sender.fail(message -> startOf(message).orElse(0) < 12);
    chain.start(header(PIVOT));
    executor.runAll();
    final int asked = sender.requests().size();

    chain.cancel();
    sender.fail(message -> false);
    resumes.runAll();
    executor.runAll();

    assertThat(sender.requests()).hasSize(asked);
    assertThat(chain.headersDone()).isCancelled();
  }

  @Test
  void waitsForTheWindowWorkersOnceCancelled() throws InterruptedException {
    final ExecutorService workers = mock(ExecutorService.class);
    final SyncChain chain = chain(Optional.empty(), false, workers);

    chain.cancel();
    chain.awaitStop();

    // interrupted, then awaited before the storage they write to closes
    final InOrder order = inOrder(workers);
    order.verify(workers).shutdownNow();
    order.verify(workers).awaitTermination(anyLong(), any());
  }

  @Test
  void review_aPivotAtTheCheckpointCompletesWithNothingToDownload() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);

    chain.start(header(MERGE));
    final CompletableFuture<Void> window = chain.downloadWindow(header(MERGE));
    executor.runAll();
    windowWorkers.runAll();
    chain.setChainHead(header(MERGE));

    assertThat(chain.headersDone()).isCompleted();
    assertThat(window).isCompleted();
    assertThat(blockchain.getChainHeadHeader()).isEqualTo(header(MERGE));
  }

  @Test
  void review_aPivotJustAboveTheCheckpointCompletesTheHeadersInStart() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);

    chain.start(header(MERGE + 1));

    assertThat(chain.headersDone()).isCompleted();
    final CompletableFuture<Void> window = chain.downloadWindow(header(MERGE + 1));
    executor.runAll();
    windowWorkers.runAll();
    assertThat(window).isCompleted();
    chain.setChainHead(header(MERGE + 1));
    assertThat(blockchain.getChainHead().getTotalDifficulty())
        .isEqualTo(totalDifficulty(MERGE + 1));
  }

  @Test
  void review_aNetworkThatDoesNotMergeStoresNoTotalsAndTheHeadComputesThem() {
    final SyncChain chain =
        new SyncChain(
            blockchain,
            ProtocolScheduleFixture.MAINNET,
            sender,
            new HeaderFetcher(sender, ProtocolScheduleFixture.MAINNET, Optional::empty, 4),
            Optional.of(checkpoint()),
            new SyncChain.Settings(4, 16, WINDOW, true, false, Optional.empty()),
            new NoOpMetricsSystem(),
            executor,
            windowWorkers,
            delay -> resumes,
            pauses::add);
    chain.start(header(PIVOT));
    executor.runAll();
    assertThat(blockchain.getTotalDifficultyByHash(header(PIVOT - 1).getHash())).isEmpty();

    chain.setChainHead(header(PIVOT));

    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(totalDifficulty(PIVOT));
  }

  @Test
  void review_theChainHeadIsSetWithoutTheWindow() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    chain.downloadWindow(header(PIVOT - 4));
    executor.runAll();

    // the window is for another pivot and has not run
    chain.setChainHead(header(PIVOT));

    assertThat(blockchain.getChainHeadHeader()).isEqualTo(header(PIVOT));
    assertThat(blockchain.getBlockBody(header(PIVOT).getHash())).isEmpty();
  }

  @Test
  void review_aSecondStartLeavesTheFirstDownloadRunningAfterCancel() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    chain.start(header(PIVOT));

    chain.cancel();
    executor.runAll();

    // the first download was never stopped, and its writes do not check the cancel
    assertThat(blockchain.getBlockHeader(PIVOT - 1)).isPresent();
  }

  private SyncChain chain(final Optional<Checkpoint> checkpoint, final boolean toCheckpointOnly) {
    return chain(checkpoint, toCheckpointOnly, windowWorkers);
  }

  // 16 header requests at once, more than the segments, as the stages share the test thread
  private SyncChain chain(
      final Optional<Checkpoint> checkpoint,
      final boolean toCheckpointOnly,
      final ExecutorService workers) {
    return new SyncChain(
        blockchain,
        ProtocolScheduleFixture.MAINNET,
        sender,
        new HeaderFetcher(sender, ProtocolScheduleFixture.MAINNET, Optional::empty, 4),
        checkpoint,
        new SyncChain.Settings(
            4, 16, WINDOW, toCheckpointOnly, false, Optional.of(totalDifficulty(MERGE - 1))),
        new NoOpMetricsSystem(),
        executor,
        workers,
        delay -> resumes,
        pauses::add);
  }

  // genesis, then empty blocks of proof of work below the merge and of proof of stake from it
  private List<Block> chain() {
    final List<Block> chain =
        new ArrayList<>(List.of(gen.genesisBlock(new BlockOptions().setTimestamp(0L))));
    for (int number = 1; number <= HEAD; number++) {
      chain.add(
          gen.block(
              new BlockOptions()
                  .setBlockNumber(number)
                  .setParentHash(chain.getLast().getHash())
                  .setTimestamp((long) number)
                  .setDifficulty(number < MERGE ? Difficulty.ONE : Difficulty.ZERO)
                  .hasOmmers(false)
                  .hasTransactions(false)
                  .setReceiptsRoot(Hash.EMPTY_TRIE_HASH)));
    }
    return chain;
  }

  private BlockHeader header(final long number) {
    return blocks.get((int) number).getHeader();
  }

  private List<BlockHeader> headers(final long from, final long to) {
    final List<BlockHeader> headers = new ArrayList<>();
    for (long number = from; number <= to; number++) {
      headers.add(header(number));
    }
    return headers;
  }

  private Difficulty totalDifficulty(final long number) {
    return source.getTotalDifficultyByHash(header(number).getHash()).orElseThrow();
  }

  private Checkpoint checkpoint() {
    return checkpoint(MERGE, header(MERGE).getHash(), totalDifficulty(MERGE));
  }

  private static Checkpoint checkpoint(
      final long number, final Hash hash, final Difficulty totalDifficulty) {
    return ImmutableCheckpoint.builder()
        .blockNumber(number)
        .blockHash(hash)
        .totalDifficulty(totalDifficulty)
        .build();
  }

  private void assertHeadersStored(final long from, final long to) {
    for (long number = from; number <= to; number++) {
      assertThat(blockchain.getBlockHeader(number)).contains(header(number));
    }
  }

  // the highest block a header request by number asks for, none for a request by hash
  private static OptionalLong startOf(final MessageData message) {
    return GetBlockHeadersMessage.readFrom(message).blockNumber();
  }
}
