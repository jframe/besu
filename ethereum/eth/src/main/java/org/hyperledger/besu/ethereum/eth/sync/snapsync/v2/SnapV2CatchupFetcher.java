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

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.v2.SnapV2SegmentResolver.MAX_ANCESTOR_WALK;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * World-state-owned source of snap/2 catch-up data. Fetches the new chain's headers downward from
 * the trusted new pivot until it meets the old pivot's chain (kept in memory, never written to
 * blockchain storage), then fetches and persists, by block hash, the BALs of every BAL-enabled
 * block in the segment that is not already stored.
 */
public class SnapV2CatchupFetcher implements SnapV2CatchupDataSource {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2CatchupFetcher.class);

  static final int BAL_REQUEST_WINDOW = 16;
  static final int HEADER_BATCH = SynchronizerConfiguration.DEFAULT_DOWNLOADER_HEADER_REQUEST_SIZE;

  /** Fetches up to {@code count} headers starting at {@code startHash}, descending. */
  @FunctionalInterface
  interface HeaderSource {
    CompletableFuture<List<BlockHeader>> headersDescending(
        Hash startHash, long startNumber, int count);
  }

  /** Fetches the BALs of {@code headers}, positionally. */
  @FunctionalInterface
  interface BalSource {
    CompletableFuture<List<SyncBlockAccessList>> fetch(List<BlockHeader> headers);
  }

  private final HeaderSource headerSource;
  private final BalSource balSource;
  private final DefaultBlockchain blockchain;

  /** Old-chain headers at or below the current pivot (most recent MAX_ANCESTOR_WALK + 1). */
  private final Map<Hash, BlockHeader> ancestry = new ConcurrentHashMap<>();

  public SnapV2CatchupFetcher(
      final EthContext ethContext,
      final ProtocolSchedule protocolSchedule,
      final DefaultBlockchain blockchain,
      final MetricsSystem metricsSystem) {
    this(
        (startHash, startNumber, count) ->
            ethContext
                .getScheduler()
                .scheduleServiceTask(
                    () -> {
                      final GetHeadersFromPeerTask task =
                          new GetHeadersFromPeerTask(
                              startHash,
                              startNumber,
                              count,
                              0,
                              GetHeadersFromPeerTask.Direction.REVERSE,
                              protocolSchedule);
                      final PeerTaskExecutorResult<List<BlockHeader>> result =
                          ethContext.getPeerTaskExecutor().execute(task);
                      if (result.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
                          || result.result().isEmpty()
                          || result.result().get().isEmpty()) {
                        return CompletableFuture.failedFuture(
                            new IllegalStateException(
                                "Unable to download snap/2 catch-up headers below block "
                                    + (startNumber + 1)
                                    + ": "
                                    + result.responseCode()));
                      }
                      return CompletableFuture.completedFuture(result.result().get());
                    }),
        headers ->
            new RetryingGetBlockAccessListsFromPeerTask(ethContext, headers, metricsSystem).run(),
        blockchain);
  }

  SnapV2CatchupFetcher(
      final HeaderSource headerSource,
      final BalSource balSource,
      final DefaultBlockchain blockchain) {
    this.headerSource = headerSource;
    this.balSource = balSource;
    this.blockchain = blockchain;
  }

  @Override
  public CompletableFuture<SnapV2ChainSegment> fetch(
      final BlockHeader currentPivot, final BlockHeader newPivot) {
    final long startMillis = System.currentTimeMillis();
    final List<BlockHeader> descending = new ArrayList<>(List.of(newPivot));
    return CompletableFuture.completedFuture(descending)
        .thenCompose(d -> collectSegment(currentPivot, newPivot, d))
        .thenCompose(
            segment -> {
              final List<BlockHeader> all = new ArrayList<>(segment.orphanedHeaders());
              all.addAll(segment.canonicalHeaders());
              return fetchAndPersistBals(all)
                  .thenApply(count -> logged(segment, count, startMillis));
            })
        .thenApply(
            segment -> {
              rememberAncestry(segment.canonicalHeaders(), segment.newPivot());
              return segment;
            });
  }

  /**
   * Captures the old pivot's recent ancestry (headers and BALs) so a reorg at the first catch-up
   * can be recovered without the chain downloader. Best effort: never completes exceptionally.
   */
  public CompletableFuture<Void> prefetchAncestry(final BlockHeader pivot) {
    ancestry.put(pivot.getHash(), pivot);
    if (pivot.getNumber() == 0) {
      return CompletableFuture.completedFuture(null);
    }
    final int count = (int) Math.min(MAX_ANCESTOR_WALK, pivot.getNumber());
    return headerSource
        .headersDescending(pivot.getParentHash(), pivot.getNumber() - 1, count)
        .thenCompose(
            headers -> {
              final List<BlockHeader> chain = new ArrayList<>(List.of(pivot));
              chain.addAll(headers);
              SnapV2SegmentResolver.verifyLinkage(pivot, chain);
              headers.forEach(h -> ancestry.put(h.getHash(), h));
              return fetchAndPersistBals(chain);
            })
        .handle(
            (fetchedCount, error) -> {
              if (error != null) {
                LOG.debug(
                    "snap/2 ancestry prefetch below pivot {} failed: {}",
                    pivot.getNumber(),
                    error.toString());
              }
              return null;
            });
  }

  private CompletableFuture<SnapV2ChainSegment> collectSegment(
      final BlockHeader currentPivot,
      final BlockHeader newPivot,
      final List<BlockHeader> descending) {
    final Optional<SnapV2ChainSegment> resolved =
        SnapV2SegmentResolver.resolve(currentPivot, newPivot, descending, this::oldChainHeader);
    if (resolved.isPresent()) {
      return CompletableFuture.completedFuture(resolved.get());
    }
    final BlockHeader lowest = descending.getLast();
    if (lowest.getNumber() == 0) {
      return CompletableFuture.failedFuture(
          new ReorgUnrecoverableException(
              "Cannot recover reorg: reached genesis without a common ancestor"));
    }
    return headerSource
        .headersDescending(lowest.getParentHash(), lowest.getNumber() - 1, HEADER_BATCH)
        .thenCompose(
            batch -> {
              if (batch.isEmpty()) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException(
                        "No snap/2 catch-up headers returned below block " + lowest.getNumber()));
              }
              descending.addAll(batch);
              return collectSegment(currentPivot, newPivot, descending);
            });
  }

  private Optional<BlockHeader> oldChainHeader(final Hash hash) {
    return Optional.ofNullable(ancestry.get(hash)).or(() -> blockchain.getBlockHeader(hash));
  }

  /** Fetches and stores BALs not yet held locally; completes with the number fetched. */
  private CompletableFuture<Integer> fetchAndPersistBals(final List<BlockHeader> headers) {
    final List<BlockHeader> missing =
        headers.stream()
            .filter(h -> h.getBalHash().isPresent())
            .filter(h -> blockchain.getBlockAccessList(h.getHash()).isEmpty())
            .toList();
    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
    for (int start = 0; start < missing.size(); start += BAL_REQUEST_WINDOW) {
      final List<BlockHeader> window =
          missing.subList(start, Math.min(start + BAL_REQUEST_WINDOW, missing.size()));
      chain = chain.thenCompose(v -> balSource.fetch(window).thenAccept(b -> persist(window, b)));
    }
    return chain.thenApply(v -> missing.size());
  }

  private void persist(final List<BlockHeader> headers, final List<SyncBlockAccessList> bals) {
    if (bals == null || bals.size() < headers.size()) {
      throw new IllegalStateException(
          "snap/2 catch-up received "
              + (bals == null ? 0 : bals.size())
              + " BALs for "
              + headers.size()
              + " blocks");
    }
    final BlockchainStorage.Updater updater = blockchain.getBlockchainStorage().updater();
    for (int i = 0; i < headers.size(); i++) {
      final BlockHeader header = headers.get(i);
      final SyncBlockAccessList bal = bals.get(i);
      if (bal == null || bal.isUnavailable()) {
        throw new IllegalStateException(
            "snap/2 catch-up BAL unavailable for block "
                + header.getNumber()
                + " ("
                + header.getHash()
                + ")");
      }
      if (!BodyValidation.balHash(bal).equals(header.getBalHash().orElseThrow())) {
        throw new IllegalStateException(
            "snap/2 catch-up BAL hash mismatch for block " + header.getNumber());
      }
      updater.putSyncBlockAccessList(header.getHash(), bal);
    }
    updater.commit();
  }

  private void rememberAncestry(final List<BlockHeader> canonical, final BlockHeader newPivot) {
    canonical.forEach(h -> ancestry.put(h.getHash(), h));
    final long floor = newPivot.getNumber() - MAX_ANCESTOR_WALK;
    ancestry.values().removeIf(h -> h.getNumber() < floor || h.getNumber() > newPivot.getNumber());
  }

  private static SnapV2ChainSegment logged(
      final SnapV2ChainSegment segment, final int bals, final long startMillis) {
    LOG.info(
        "snap/2 catch-up data ({}, {}] fetched in {} ms: headers={}, bals={}, reorg={}",
        segment.commonAncestor().getNumber(),
        segment.newPivot().getNumber(),
        System.currentTimeMillis() - startMillis,
        segment.canonicalHeaders().size() + segment.orphanedHeaders().size(),
        bals,
        segment.isReorg());
    return segment;
  }
}
