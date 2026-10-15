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

import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the block access lists of some headers in rounds and stores them by block hash (cf.
 * snap-new2 BalFetcher). BALs already stored are never requested; a round that receives nothing
 * waits before the next one; running out of rounds fails the move.
 */
public final class SnapV2BalFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2BalFetcher.class);

  static final int BATCH_SIZE = 32;
  public static final int MOVE_ROUNDS = 60;
  public static final int ORPHANED_ROUNDS = 4;

  @FunctionalInterface
  public interface BalRequester {
    /** Lists in request order; unavailable ones are {@code SyncBlockAccessList#isUnavailable}. */
    CompletableFuture<List<SyncBlockAccessList>> request(List<BlockHeader> headers);
  }

  @FunctionalInterface
  public interface Delay {
    CompletableFuture<Void> after(Duration delay);
  }

  private final BalRequester requester;
  private final Delay delay;
  private final DefaultBlockchain blockchain;

  public SnapV2BalFetcher(
      final BalRequester requester, final Delay delay, final DefaultBlockchain blockchain) {
    this.requester = requester;
    this.delay = delay;
    this.blockchain = blockchain;
  }

  public static SnapV2BalFetcher create(
      final EthContext ethContext,
      final DefaultBlockchain blockchain,
      final MetricsSystem metricsSystem) {
    return new SnapV2BalFetcher(
        headers ->
            new RetryingGetBlockAccessListsFromPeerTask(ethContext, headers, metricsSystem).run(),
        wait -> ethContext.getScheduler().scheduleFutureTask(() -> {}, wait),
        blockchain);
  }

  /** Completes once the BAL of every BAL-enabled header is stored. */
  public CompletableFuture<Void> fetch(final List<BlockHeader> headers, final int rounds) {
    return round(headers, 0, rounds);
  }

  private CompletableFuture<Void> round(
      final List<BlockHeader> headers, final int round, final int rounds) {
    final List<BlockHeader> missing = missing(headers);
    if (missing.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    if (round >= rounds) {
      return CompletableFuture.failedFuture(
          new SnapV2MoveFailedException(
              "No peer served the block access list of block " + missing.getFirst().getNumber()));
    }
    final List<CompletableFuture<Void>> requests = new ArrayList<>();
    for (int i = 0; i < missing.size(); i += BATCH_SIZE) {
      final List<BlockHeader> batch = missing.subList(i, Math.min(missing.size(), i + BATCH_SIZE));
      requests.add(
          requester
              .request(batch)
              .thenAccept(lists -> store(batch, lists))
              .exceptionally(
                  error -> {
                    LOG.debug(
                        "snap/2 BAL request for blocks {}..{} failed: {}",
                        batch.getFirst().getNumber(),
                        batch.getLast().getNumber(),
                        error.toString());
                    return null;
                  }));
    }
    return CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new))
        .thenCompose(
            ignored -> {
              final boolean receivedAny = missing(headers).size() < missing.size();
              if (receivedAny) {
                return round(headers, round + 1, rounds);
              }
              return delay
                  .after(Duration.ofSeconds(Math.min(10, 1 + round)))
                  .thenCompose(waited -> round(headers, round + 1, rounds));
            });
  }

  private List<BlockHeader> missing(final List<BlockHeader> headers) {
    return headers.stream()
        .filter(header -> header.getBalHash().isPresent())
        .filter(header -> blockchain.getBlockAccessList(header.getHash()).isEmpty())
        .toList();
  }

  // keeps every available list; the peer task already checked each against its header's BAL hash
  private void store(final List<BlockHeader> batch, final List<SyncBlockAccessList> lists) {
    final BlockchainStorage.Updater updater = blockchain.getBlockchainStorage().updater();
    for (int i = 0; i < Math.min(batch.size(), lists.size()); i++) {
      final SyncBlockAccessList list = lists.get(i);
      if (list != null && !list.isUnavailable()) {
        updater.putSyncBlockAccessList(batch.get(i).getHash(), list);
      }
    }
    updater.commit();
  }
}
