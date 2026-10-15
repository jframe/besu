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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class SnapV2HeaderFetcherTest {

  /** Chain 0..n, each header the parent of the next; {@code fork} makes the hashes differ. */
  private static List<BlockHeader> chain(final int n, final int fork) {
    final List<BlockHeader> headers = new ArrayList<>();
    BlockHeader parent =
        new BlockHeaderTestFixture().number(0).extraData(Bytes.of(fork)).buildHeader();
    headers.add(parent);
    for (int i = 1; i <= n; i++) {
      parent =
          new BlockHeaderTestFixture()
              .number(i)
              .parentHash(parent.getHash())
              .extraData(Bytes.of(fork))
              .buildHeader();
      headers.add(parent);
    }
    return headers;
  }

  private static List<BlockHeader> chain(final int n) {
    return chain(n, 0);
  }

  /** Serves headers descending by hash, at most {@code cap} per request. */
  private static SnapV2HeaderFetcher.HeaderRequester serving(
      final List<BlockHeader> chain, final int cap, final AtomicInteger requests) {
    final Map<Hash, BlockHeader> byHash = new HashMap<>();
    chain.forEach(h -> byHash.put(h.getHash(), h));
    return (startHash, startNumber, count) -> {
      requests.incrementAndGet();
      final List<BlockHeader> out = new ArrayList<>();
      BlockHeader cursor = byHash.get(startHash);
      while (cursor != null && out.size() < Math.min(count, cap)) {
        out.add(cursor);
        cursor = cursor.getNumber() == 0 ? null : byHash.get(cursor.getParentHash());
      }
      return CompletableFuture.completedFuture(out);
    };
  }

  @Test
  void walksDownAcrossSeveralRequests() {
    final List<BlockHeader> chain = chain(500);
    final AtomicInteger requests = new AtomicInteger();
    final SnapV2HeaderFetcher fetcher = new SnapV2HeaderFetcher(serving(chain, 192, requests), 192);

    final List<BlockHeader> result = fetcher.chainDown(chain.get(500), 101).join();

    assertThat(result).containsExactlyElementsOf(chain.subList(101, 501));
    assertThat(requests.get()).isEqualTo(3); // 399 headers below the top, 192 per request
  }

  @Test
  void continuesAfterAShortBatch() {
    final List<BlockHeader> chain = chain(300);
    final AtomicInteger requests = new AtomicInteger();
    final SnapV2HeaderFetcher fetcher = new SnapV2HeaderFetcher(serving(chain, 50, requests), 192);

    final List<BlockHeader> result = fetcher.chainDown(chain.get(300), 200).join();

    assertThat(result).containsExactlyElementsOf(chain.subList(200, 301));
    assertThat(requests.get()).isEqualTo(2);
  }

  @Test
  void returnsJustTheTopWhenNothingIsBelowIt() {
    final List<BlockHeader> chain = chain(10);
    final SnapV2HeaderFetcher fetcher =
        new SnapV2HeaderFetcher(serving(chain, 192, new AtomicInteger()), 192);

    assertThat(fetcher.chainDown(chain.get(10), 10).join()).containsExactly(chain.get(10));
  }

  @Test
  void failsOnAnEmptyBatch() {
    final List<BlockHeader> chain = chain(10);
    final SnapV2HeaderFetcher fetcher =
        new SnapV2HeaderFetcher(
            (startHash, startNumber, count) -> CompletableFuture.completedFuture(List.of()), 192);

    assertThatThrownBy(() -> fetcher.chainDown(chain.get(10), 5).join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(SnapV2MoveFailedException.class);
  }

  @Test
  void failsWhenTheBatchDoesNotLinkToTheCursor() {
    final List<BlockHeader> chain = chain(10);
    final List<BlockHeader> otherFork = chain(10, 1); // different hashes, same numbers
    final SnapV2HeaderFetcher fetcher =
        new SnapV2HeaderFetcher(
            (startHash, startNumber, count) ->
                CompletableFuture.completedFuture(List.of(otherFork.get(9), otherFork.get(8))),
            192);

    assertThatThrownBy(() -> fetcher.chainDown(chain.get(10), 5).join())
        .isInstanceOf(CompletionException.class);
  }
}
