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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapV2BalFetcherTest {

  private final Map<Hash, SyncBlockAccessList> stored = new HashMap<>();
  private final List<Duration> delays = new ArrayList<>();
  private final DefaultBlockchain blockchain = mock(DefaultBlockchain.class);

  @BeforeEach
  void setUp() {
    final BlockchainStorage storage = mock(BlockchainStorage.class);
    final BlockchainStorage.Updater updater = mock(BlockchainStorage.Updater.class);
    when(blockchain.getBlockchainStorage()).thenReturn(storage);
    when(storage.updater()).thenReturn(updater);
    doAnswer(
            inv -> {
              stored.put(inv.getArgument(0), inv.getArgument(1));
              return null;
            })
        .when(updater)
        .putSyncBlockAccessList(any(), any());
    when(blockchain.getBlockAccessList(any()))
        .thenAnswer(
            inv ->
                stored.containsKey((Hash) inv.getArgument(0))
                    ? Optional.of(mock(BlockAccessList.class))
                    : Optional.empty());
  }

  private SnapV2BalFetcher fetcher(final SnapV2BalFetcher.BalRequester requester) {
    return new SnapV2BalFetcher(
        requester,
        delay -> {
          delays.add(delay);
          return CompletableFuture.completedFuture(null);
        },
        blockchain);
  }

  private static BlockHeader balHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).balHash(Hash.ZERO).buildHeader();
  }

  private static SyncBlockAccessList bal() {
    return new SyncBlockAccessList(Bytes.fromHexString("0xc0"));
  }

  private static List<SyncBlockAccessList> bals(final int n) {
    final List<SyncBlockAccessList> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add(bal());
    }
    return out;
  }

  @Test
  void storesEveryBalInBatchesOf32() {
    final List<BlockHeader> headers = new ArrayList<>();
    for (int i = 1; i <= 70; i++) {
      headers.add(balHeader(i));
    }
    final List<Integer> batchSizes = new ArrayList<>();
    final SnapV2BalFetcher fetcher =
        fetcher(
            batch -> {
              batchSizes.add(batch.size());
              return CompletableFuture.completedFuture(bals(batch.size()));
            });

    fetcher.fetch(headers, SnapV2BalFetcher.MOVE_ROUNDS).join();

    assertThat(batchSizes).containsExactly(32, 32, 6);
    headers.forEach(h -> assertThat(stored).containsKey(h.getHash()));
  }

  @Test
  void doesNotRequestStoredBalsOrBlocksWithoutABalHash() {
    final BlockHeader alreadyStored = balHeader(1);
    stored.put(alreadyStored.getHash(), bal());
    final BlockHeader preFork = new BlockHeaderTestFixture().number(2).buildHeader();
    final BlockHeader missing = balHeader(3);
    final List<List<BlockHeader>> requested = new ArrayList<>();
    final SnapV2BalFetcher fetcher =
        fetcher(
            batch -> {
              requested.add(batch);
              return CompletableFuture.completedFuture(bals(batch.size()));
            });

    fetcher.fetch(List.of(alreadyStored, preFork, missing), 1).join();

    assertThat(requested).containsExactly(List.of(missing));
  }

  @Test
  void backsOffAfterAnEmptyRoundAndKeepsPartialProgress() {
    final BlockHeader first = balHeader(1);
    final BlockHeader second = balHeader(2);
    final AtomicInteger calls = new AtomicInteger();
    final SnapV2BalFetcher fetcher =
        fetcher(
            batch -> {
              final int call = calls.incrementAndGet();
              if (call == 1) {
                return CompletableFuture.failedFuture(new RuntimeException("timeout"));
              }
              // second round: only the first BAL is available
              if (call == 2) {
                return CompletableFuture.completedFuture(
                    List.of(bal(), new SyncBlockAccessList(Bytes.fromHexString("0x80"))));
              }
              return CompletableFuture.completedFuture(bals(batch.size()));
            });

    fetcher.fetch(List.of(first, second), 5).join();

    assertThat(delays).containsExactly(Duration.ofSeconds(1)); // only the empty first round waits
    assertThat(stored).containsKeys(first.getHash(), second.getHash());
  }

  @Test
  void failsTheMoveAfterTheLastRound() {
    final SnapV2BalFetcher fetcher =
        fetcher(batch -> CompletableFuture.failedFuture(new RuntimeException("timeout")));

    assertThatThrownBy(() -> fetcher.fetch(List.of(balHeader(1)), 3).join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(SnapV2MoveFailedException.class);
    assertThat(delays)
        .containsExactly(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(3));
  }
}
