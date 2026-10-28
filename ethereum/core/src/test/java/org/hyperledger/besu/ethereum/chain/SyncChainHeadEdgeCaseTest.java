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
package org.hyperledger.besu.ethereum.chain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Edge cases of {@link DefaultBlockchain#unsafeSetSyncChainHead} and the earliest block. */
class SyncChainHeadEdgeCaseTest {

  private final KeyValueStorage chainKv = new InMemoryKeyValueStorage();
  private final KeyValueStorage variablesKv = new InMemoryKeyValueStorage();
  private final BlockchainStorage storage = spy(createStorage());

  // 1. from == to

  @Test
  void fromEqualToToKeepsTheTotalOfFromOnAProofOfWorkGenesis() {
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    clearInvocations(storage);

    blockchain.unsafeSetSyncChainHead(genesis.getHeader(), genesis.getHeader());

    assertThat(blockchain.getChainHeadHeader()).isEqualTo(genesis.getHeader());
    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(Difficulty.of(4));
    verify(storage, never()).batchUpdater();
  }

  @Test
  void fromEqualToToKeepsTheTotalOfACheckpointFloor() {
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    final BlockHeader checkpoint = headersAbove(genesis.getHeader(), 5, Difficulty.ZERO).getLast();
    blockchain.unsafeStoreHeader(checkpoint, Difficulty.of(1234));
    clearInvocations(storage);

    blockchain.unsafeSetSyncChainHead(checkpoint, checkpoint);

    assertThat(blockchain.getChainHeadHeader()).isEqualTo(checkpoint);
    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(Difficulty.of(1234));
    verify(storage, never()).batchUpdater();
  }

  // 2. the fast path cannot be set up for proof of work, and the slow path crosses the merge

  @Test
  void headersWithTheFloorTotalRejectProofOfWork() {
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> mixed =
        new ArrayList<>(headersAbove(genesis.getHeader(), 2, Difficulty.ZERO));
    mixed.addAll(headersAbove(mixed.getLast(), 1, Difficulty.ONE));

    assertThatThrownBy(() -> blockchain.storeBlockHeaders(mixed, Difficulty.of(4)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(blockchain.getBlockHeader(1)).isEmpty();
  }

  @Test
  void slowPathComputesTheTotalsThroughTheMerge() {
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    // proof of work up to block 3, proof of stake from block 4
    final List<BlockHeader> headers =
        new ArrayList<>(headersAbove(genesis.getHeader(), 3, Difficulty.of(10)));
    headers.addAll(headersAbove(headers.getLast(), 5, Difficulty.ZERO));
    blockchain.storeBlockHeaders(headers);

    blockchain.unsafeSetSyncChainHead(genesis.getHeader(), headers.getLast());

    for (int i = 0; i < headers.size(); i++) {
      final long expected = 4 + 10L * Math.min(i + 1, 3);
      assertThat(blockchain.getTotalDifficultyByHash(headers.get(i).getHash()))
          .contains(Difficulty.of(expected));
    }
    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(Difficulty.of(34));
  }

  // 3. stale totals

  @Test
  void staleTotalsOnTheHeadAndBelowItAreComputedAgain() {
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), 4, Difficulty.ONE);
    blockchain.storeBlockHeaders(headers);
    putTotal(headers.get(1), Difficulty.of(999));
    putTotal(headers.getLast(), Difficulty.of(1));

    blockchain.unsafeSetSyncChainHead(genesis.getHeader(), headers.getLast());

    assertThat(blockchain.getTotalDifficultyByHash(headers.get(1).getHash()))
        .contains(Difficulty.of(6));
    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(Difficulty.of(8));
  }

  @Test
  void aStaleProofOfWorkTotalEqualToTheFloorTotalTakesTheFastPath() {
    // characterises the guard: only the equality decides, so the caller must never leave one
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), 4, Difficulty.ONE);
    blockchain.storeBlockHeaders(headers);
    putTotal(headers.getLast(), Difficulty.of(4));
    clearInvocations(storage);

    blockchain.unsafeSetSyncChainHead(genesis.getHeader(), headers.getLast());

    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(Difficulty.of(4));
    verify(storage, never()).batchUpdater();
  }

  // 4. the fast path does not check the parent links

  @Test
  void fastPathAcceptsACanonicalIndexThatDoesNotLink() {
    final Block genesis = genesis(Difficulty.ONE);
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> above = headersAbove(genesis.getHeader(), 4, Difficulty.ZERO);
    final BlockHeader orphan =
        new BlockHeaderTestFixture()
            .number(2)
            .parentHash(Hash.ZERO)
            .difficulty(Difficulty.ZERO)
            .buildHeader();
    blockchain.storeBlockHeaders(
        List.of(above.get(0), orphan, above.get(2), above.get(3)), Difficulty.ONE);

    blockchain.unsafeSetSyncChainHead(genesis.getHeader(), above.getLast());

    assertThat(blockchain.getChainHeadHeader()).isEqualTo(above.getLast());
    assertThat(blockchain.getBlockHeader(2)).contains(orphan);
  }

  // 5 and 6. a later chunk failing, and the node restarting

  @Test
  void aLaterChunkFailingKeepsTheEarlierTotalsAndTheHeadOnTheFloor() {
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), 10_002, Difficulty.ONE);
    final BlockHeader orphan =
        new BlockHeaderTestFixture()
            .number(10_001)
            .parentHash(Hash.ZERO)
            .difficulty(Difficulty.ONE)
            .buildHeader();
    final List<BlockHeader> stored = new ArrayList<>(headers);
    stored.set(10_000, orphan);
    blockchain.storeBlockHeaders(stored);

    assertThatThrownBy(
            () -> blockchain.unsafeSetSyncChainHead(genesis.getHeader(), headers.getLast()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("10001");

    assertThat(blockchain.getChainHeadHeader()).isEqualTo(genesis.getHeader());
    // the first chunk committed, with the right totals
    assertThat(blockchain.getTotalDifficultyByHash(headers.get(9_999).getHash()))
        .contains(Difficulty.of(10_004));
    assertThat(blockchain.getTotalDifficultyByHash(orphan.getHash())).isEmpty();
    assertThat(blockchain.getTotalDifficultyByHash(headers.getLast().getHash())).isEmpty();

    // a restarted node finds its head on the floor with its total
    final DefaultBlockchain restarted = blockchain(genesis);
    assertThat(restarted.getChainHeadHeader()).isEqualTo(genesis.getHeader());
    assertThat(restarted.getChainHead().getTotalDifficulty()).isEqualTo(Difficulty.of(4));
  }

  @Test
  void aRestartedNodeFindsTheSyncChainHeadWithItsTotal() {
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), 5, Difficulty.ONE);
    blockchain.storeBlockHeaders(headers);
    blockchain.unsafeSetSyncChainHead(genesis.getHeader(), headers.getLast());

    final DefaultBlockchain restarted = blockchain(genesis);

    assertThat(restarted.getChainHeadHeader()).isEqualTo(headers.getLast());
    assertThat(restarted.getChainHead().getTotalDifficulty()).isEqualTo(Difficulty.of(9));
  }

  @Test
  void aRestartedNodeFindsAProofOfStakeSyncChainHeadWithItsTotal() {
    final Block genesis = genesis(Difficulty.ONE);
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), 5, Difficulty.ZERO);
    blockchain.storeBlockHeaders(headers, Difficulty.ONE);
    blockchain.unsafeSetSyncChainHead(genesis.getHeader(), headers.getLast());

    final DefaultBlockchain restarted = blockchain(genesis);

    assertThat(restarted.getChainHeadHeader()).isEqualTo(headers.getLast());
    assertThat(restarted.getChainHead().getTotalDifficulty()).isEqualTo(Difficulty.ONE);
  }

  // 8. the fallback of the earliest block

  @Test
  void earliestIsGenesisWhenBlockOneHasItsBodyAndTheHeadIsOnGenesis() {
    final Block genesis = genesis(Difficulty.ONE);
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), 8, Difficulty.ZERO);
    blockchain.storeBlockHeaders(headers, Difficulty.ONE);
    storeBodies(headers, 1, 8);
    blockchain.unsafeSetBodiesFrom(1);

    assertThat(blockchain.getEarliestBlockNumber()).contains(0L);

    // the fallback cached genesis: once the head moves up and back, still not above the head
    blockchain.unsafeSetChainHead(headers.getLast(), Difficulty.ONE);
    assertThat(blockchain.getEarliestBlockNumber()).contains(0L);
    blockchain.unsafeSetChainHead(genesis.getHeader(), Difficulty.ONE);
    assertThat(blockchain.getEarliestBlockNumber()).contains(0L);
  }

  @Test
  void earliestIsEmptyWhileTheHeadIsBelowTheBodiesOnAGenesisFloor() {
    final Block genesis = genesis(Difficulty.ONE);
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), 8, Difficulty.ZERO);
    blockchain.storeBlockHeaders(headers, Difficulty.ONE);
    storeBodies(headers, 7, 8);
    blockchain.unsafeSetBodiesFrom(7);

    assertThat(blockchain.getEarliestBlockNumber()).isEmpty();
    // the head between the floor and the bodies
    blockchain.unsafeSetChainHead(headers.get(4), Difficulty.ONE);
    assertThat(blockchain.getEarliestBlockNumber()).isEmpty();
    // the head on the pivot, then a resync puts it back on the floor
    blockchain.unsafeSetChainHead(headers.getLast(), Difficulty.ONE);
    assertThat(blockchain.getEarliestBlockNumber()).contains(7L);
    blockchain.unsafeSetChainHead(genesis.getHeader(), Difficulty.ONE);
    assertThat(blockchain.getEarliestBlockNumber()).isEmpty();
  }

  @Test
  void earliestIsEmptyWhileTheHeadIsOnACheckpointFloor() {
    final Block genesis = genesis(Difficulty.ONE);
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), 8, Difficulty.ZERO);
    // the checkpoint stored alone, then the headers above it
    blockchain.unsafeStoreHeader(headers.get(3), Difficulty.ONE);
    blockchain.storeBlockHeaders(headers.subList(4, 8), Difficulty.ONE);
    storeBodies(headers, 7, 8);
    blockchain.unsafeSetBodiesFrom(7);

    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(4);
    assertThat(blockchain.getEarliestBlockNumber()).isEmpty();
  }

  // 9. commit boundaries

  @ParameterizedTest
  @CsvSource({"1, 1", "10000, 1", "10001, 2", "20000, 2", "20001, 3"})
  void commitsEveryTenThousandHeaders(final int count, final int commits) {
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), count, Difficulty.ONE);
    blockchain.storeBlockHeaders(headers);
    clearInvocations(storage);

    blockchain.unsafeSetSyncChainHead(genesis.getHeader(), headers.getLast());

    verify(storage, times(commits)).batchUpdater();
    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(Difficulty.of(4 + count));
  }

  // 10. the blockchain monitor during the slow pass

  @Test
  void theSlowPassHoldsTheBlockchainMonitor() throws Exception {
    final Block genesis = genesis(Difficulty.of(4));
    final DefaultBlockchain blockchain = blockchain(genesis);
    final List<BlockHeader> headers = headersAbove(genesis.getHeader(), 5, Difficulty.ONE);
    blockchain.storeBlockHeaders(headers);
    final CountDownLatch reading = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              reading.countDown();
              release.await(10, TimeUnit.SECONDS);
              return invocation.callRealMethod();
            })
        .when(storage)
        .getBlockHash(3L);

    final CompletableFuture<Void> pass =
        CompletableFuture.runAsync(
            () -> blockchain.unsafeSetSyncChainHead(genesis.getHeader(), headers.getLast()));
    assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();

    final CompletableFuture<Optional<BlockHeader>> safe =
        CompletableFuture.supplyAsync(() -> blockchain.getBlockHeaderSafe(genesis.getHash()));
    final CompletableFuture<Block> headBlock =
        CompletableFuture.supplyAsync(blockchain::getChainHeadBlock);
    // the unsynchronized reads go through
    assertThat(blockchain.getBlockHeader(genesis.getHash())).isPresent();
    assertThat(headBlock.get(5, TimeUnit.SECONDS).getHeader()).isEqualTo(genesis.getHeader());
    // the synchronized one waits for the pass
    Thread.sleep(300);
    assertThat(safe).isNotDone();

    release.countDown();
    pass.get(10, TimeUnit.SECONDS);
    assertThat(safe.get(10, TimeUnit.SECONDS)).contains(genesis.getHeader());
  }

  private BlockchainStorage createStorage() {
    return new KeyValueStoragePrefixedKeyBlockchainStorage(
        chainKv,
        new VariablesKeyValueStorage(variablesKv),
        new MainnetBlockHeaderFunctions(),
        false);
  }

  private DefaultBlockchain blockchain(final Block genesis) {
    return (DefaultBlockchain)
        DefaultBlockchain.createMutable(genesis, storage, new NoOpMetricsSystem(), 0);
  }

  private static Block genesis(final Difficulty difficulty) {
    return new BlockDataGenerator().genesisBlock(new BlockOptions().setDifficulty(difficulty));
  }

  private static List<BlockHeader> headersAbove(
      final BlockHeader parent, final int count, final Difficulty difficulty) {
    final List<BlockHeader> headers = new ArrayList<>(count);
    BlockHeader last = parent;
    for (int i = 0; i < count; i++) {
      last =
          new BlockHeaderTestFixture()
              .number(last.getNumber() + 1)
              .parentHash(last.getHash())
              .difficulty(difficulty)
              .buildHeader();
      headers.add(last);
    }
    return headers;
  }

  private void putTotal(final BlockHeader header, final Difficulty total) {
    final BlockchainStorage.Updater updater = createStorage().updater();
    updater.putTotalDifficulty(header.getHash(), total);
    updater.commit();
  }

  // the bodies of the headers numbered from first to last, the first header being block 1
  private void storeBodies(final List<BlockHeader> headers, final int first, final int last) {
    final BlockchainStorage.Updater updater = createStorage().updater();
    for (int number = first; number <= last; number++) {
      updater.putBlockBody(headers.get(number - 1).getHash(), BlockBody.empty());
    }
    updater.commit();
  }
}
