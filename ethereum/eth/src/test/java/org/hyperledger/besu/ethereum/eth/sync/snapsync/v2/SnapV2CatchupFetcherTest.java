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
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class SnapV2CatchupFetcherTest {

  /** The "network": all blocks and BALs live here and are served by the stub sources. */
  private final ReorgBlockchainBuilder remote = new ReorgBlockchainBuilder();

  /** The local node: only receives persisted BALs from the fetcher. */
  private final DefaultBlockchain local = new ReorgBlockchainBuilder().blockchain();

  private final List<Hash> requestedBals = new CopyOnWriteArrayList<>();
  private final List<SnapV2CatchupFetcher.HeaderSource> headerOverride = new ArrayList<>();
  private boolean failBals = false;

  private SnapV2CatchupFetcher fetcher() {
    final SnapV2CatchupFetcher.HeaderSource headers =
        (startHash, startNumber, count) -> {
          if (!headerOverride.isEmpty()) {
            return headerOverride.getFirst().headersDescending(startHash, startNumber, count);
          }
          final List<BlockHeader> out = new ArrayList<>();
          Optional<BlockHeader> h = remote.blockchain().getBlockHeader(startHash);
          while (h.isPresent() && out.size() < count) {
            out.add(h.get());
            if (h.get().getNumber() == 0) {
              break;
            }
            h = remote.blockchain().getBlockHeader(h.get().getParentHash());
          }
          return CompletableFuture.completedFuture(out);
        };
    final SnapV2CatchupFetcher.BalSource bals =
        hs -> {
          final List<SyncBlockAccessList> out = new ArrayList<>();
          for (final BlockHeader h : hs) {
            requestedBals.add(h.getHash());
            out.add(
                failBals
                    ? new SyncBlockAccessList(RLP.NULL)
                    : remote
                        .blockchain()
                        .getBlockAccessList(h.getHash())
                        .map(bal -> new SyncBlockAccessList(RLP.encode(bal::writeTo)))
                        .orElse(new SyncBlockAccessList(RLP.NULL)));
          }
          return CompletableFuture.completedFuture(out);
        };
    return new SnapV2CatchupFetcher(headers, bals, local);
  }

  private static <T> T get(final CompletableFuture<T> f) throws Exception {
    return f.get(5, TimeUnit.SECONDS);
  }

  @Test
  void sameChainSegmentPersistsBalsButNotHeaders() throws Exception {
    final BlockHeader p1 = remote.appendCanonicalChain(remote.header(0), 1L, 5);
    final BlockHeader p0 = remote.header(3);
    final SnapV2CatchupFetcher fetcher = fetcher();
    get(fetcher.prefetchAncestry(p0));

    final SnapV2ChainSegment segment = get(fetcher.fetch(p0, p1));

    assertThat(segment.isReorg()).isFalse();
    assertThat(segment.commonAncestor()).isEqualTo(p0);
    assertThat(segment.canonicalHeaders()).containsExactly(remote.header(4), p1);
    assertThat(local.getBlockAccessList(p1.getHash())).isPresent();
    assertThat(local.getBlockAccessList(remote.header(4).getHash())).isPresent();
    // Gap headers must never reach blockchain storage.
    assertThat(local.getBlockHeader(p1.getHash())).isEmpty();
    assertThat(local.getBlockHeader(remote.header(4).getHash())).isEmpty();
  }

  @Test
  void reorgSegmentUsesPrefetchedAncestry() throws Exception {
    final BlockHeader stale3 = remote.appendStaleChain(remote.header(0), 1L, 3);
    final BlockHeader stale2 =
        remote.blockchain().getBlockHeader(stale3.getParentHash()).orElseThrow();
    final SnapV2CatchupFetcher fetcher = fetcher();
    get(fetcher.prefetchAncestry(stale3)); // old pivot ancestry captured before the reorg

    final Block c2 = remote.appendCanonical(remote.header(1), remote.emptyBal(), 2L);
    final Block c3 = remote.appendCanonical(c2.getHeader(), remote.emptyBal(), 3L);
    final Block c4 = remote.appendCanonical(c3.getHeader(), remote.emptyBal(), 4L);

    final SnapV2ChainSegment segment = get(fetcher.fetch(stale3, c4.getHeader()));

    assertThat(segment.isReorg()).isTrue();
    assertThat(segment.commonAncestor().getNumber()).isEqualTo(1L);
    assertThat(segment.orphanedHeaders()).containsExactly(stale2, stale3);
    assertThat(segment.canonicalHeaders())
        .containsExactly(c2.getHeader(), c3.getHeader(), c4.getHeader());
    assertThat(local.getBlockAccessList(stale3.getHash())).isPresent();
    assertThat(local.getBlockAccessList(c4.getHash())).isPresent();
  }

  @Test
  void reorgDeeperThanWalkBoundFails() throws Exception {
    final BlockHeader stale97 = remote.appendStaleChain(remote.header(0), 1L, 97);
    final SnapV2CatchupFetcher fetcher = fetcher();
    get(fetcher.prefetchAncestry(stale97));
    final BlockHeader canonical98 = remote.appendCanonicalChain(remote.header(0), 1L, 98);

    assertThatThrownBy(() -> get(fetcher.fetch(stale97, canonical98)))
        .hasCauseInstanceOf(ReorgUnrecoverableException.class);
  }

  @Test
  void nonLinkingHeadersFailTheFetch() throws Exception {
    final BlockHeader p1 = remote.appendCanonicalChain(remote.header(0), 1L, 5);
    headerOverride.add(
        (startHash, startNumber, count) ->
            CompletableFuture.completedFuture(List.of(remote.header(2)))); // skips 4 and 3
    final SnapV2CatchupFetcher fetcher = fetcher();

    assertThatThrownBy(() -> get(fetcher.fetch(remote.header(3), p1)))
        .hasCauseInstanceOf(SnapV2SegmentResolver.InvalidCatchupHeadersException.class);
  }

  @Test
  void unavailableBalFailsTheFetch() throws Exception {
    final BlockHeader p1 = remote.appendCanonicalChain(remote.header(0), 1L, 5);
    failBals = true;

    assertThatThrownBy(() -> get(fetcher().fetch(remote.header(3), p1)))
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  void alreadyStoredBalsAreNotRequested() throws Exception {
    final BlockHeader p1 = remote.appendCanonicalChain(remote.header(0), 1L, 5);
    final BlockHeader block4 = remote.header(4);
    final var updater = local.getBlockchainStorage().updater();
    updater.putSyncBlockAccessList(
        block4.getHash(),
        new SyncBlockAccessList(
            RLP.encode(
                remote.blockchain().getBlockAccessList(block4.getHash()).orElseThrow()::writeTo)));
    updater.commit();

    get(fetcher().fetch(remote.header(3), p1));

    assertThat(requestedBals).containsExactly(p1.getHash());
  }

  @Test
  void prefetchClampsAtGenesisForLowPivots() throws Exception {
    final BlockHeader p0 = remote.appendCanonicalChain(remote.header(0), 1L, 3);

    get(
        fetcher()
            .prefetchAncestry(p0)); // pivot 3 < MAX_ANCESTOR_WALK: must not fail or go negative

    assertThat(local.getBlockAccessList(remote.header(1).getHash())).isPresent();
  }

  @Test
  void headersWithoutBalHashAreNotRequested() throws Exception {
    final BlockHeader p1 = remote.appendCanonicalChain(remote.header(0), 1L, 5);
    // Genesis has no BAL hash; prefetch walks down to it.
    assertThat(remote.header(0).getBalHash()).isEmpty();
    get(fetcher().prefetchAncestry(p1));

    assertThat(requestedBals).doesNotContain(remote.header(0).getHash());
  }
}
