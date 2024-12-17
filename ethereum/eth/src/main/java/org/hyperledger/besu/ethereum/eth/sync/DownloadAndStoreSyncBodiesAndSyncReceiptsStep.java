/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.sync;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipts;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.tasks.CompleteSyncBlocksTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadAndStoreSyncBodiesAndSyncReceiptsStep
    implements Function<List<BlockHeader>, CompletableFuture<String>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DownloadAndStoreSyncBodiesAndSyncReceiptsStep.class);

  private final ProtocolSchedule protocolSchedule;
  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;
  private final ProtocolContext protocolContext;
  private final PeerTaskExecutor peerTaskExecutor;
  private final AtomicInteger no_of_max_retries_reached = new AtomicInteger();
  private final ExecutorService vte;

  public DownloadAndStoreSyncBodiesAndSyncReceiptsStep(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final ProtocolContext protocolContext,
      final PeerTaskExecutor peerTaskExecutor) {
    this.protocolSchedule = protocolSchedule;
    this.ethContext = ethContext;
    this.metricsSystem = metricsSystem;
    this.protocolContext = protocolContext;
    this.peerTaskExecutor = peerTaskExecutor;
    vte = Executors.newVirtualThreadPerTaskExecutor();
  }

  @Override
  public CompletableFuture<String> apply(final List<BlockHeader> blockHeaders) {
    final String ret =
        "finished block and receipts for blocks from "
            + blockHeaders.getFirst().getNumber()
            + " to "
            + blockHeaders.getLast().getNumber();

    final CompletableFuture<Void> getBlocksfuture =
        CompletableFuture.runAsync(() -> blocks(blockHeaders), vte);

    // get the receipts for the headers
    final CompletableFuture<Void> getReceiptsfuture =
        CompletableFuture.runAsync(() -> receipts(blockHeaders), vte);

    CompletableFuture.allOf(getBlocksfuture, getReceiptsfuture).join();

    return CompletableFuture.completedFuture(ret);
  }

  private void receipts(final List<BlockHeader> blockHeaders) {
    final List<SyncTransactionReceiptsAndHeader> receiptsForBlocks = getReceipts(blockHeaders);

    // store the receipts
    storeReceipts(receiptsForBlocks);
  }

  private void blocks(final List<BlockHeader> blockHeaders) {
    final List<SyncBlock> blocks = getSyncBlocks(blockHeaders);

    // store the blocks
    storeBlocks(blocks);
  }

  private void storeReceipts(final List<SyncTransactionReceiptsAndHeader> receiptsForBlocks) {
    for (final SyncTransactionReceiptsAndHeader receiptsForBlock : receiptsForBlocks) {
      protocolContext
          .getBlockchain()
          .unsafeImportSyncReceipts(receiptsForBlock.receipts(), receiptsForBlock.header());
    }
    LOG.atInfo()
        .setMessage("Imported receipts for {} blocks starting at block {}")
        .addArgument(receiptsForBlocks.size())
        .addArgument(receiptsForBlocks.getFirst().header().getNumber())
        .log();
  }

  private void storeBlocks(final List<SyncBlock> blocks) {
    for (final SyncBlock block : blocks) {
      protocolContext.getBlockchain().unsafeImportSyncBlock(block);
    }
    LOG.atInfo()
        .setMessage("Imported {} blocks starting at block number {}")
        .addArgument(blocks.size())
        .addArgument(blocks.getFirst().getHeader().getNumber())
        .log();
  }

  private List<SyncTransactionReceiptsAndHeader> getReceipts(final List<BlockHeader> blockHeaders) {
    final List<BlockHeader> headers = new ArrayList<>(blockHeaders);
    Map<BlockHeader, SyncTransactionReceipts> getReceipts = new HashMap<>();
    do {
      GetSyncReceiptsFromPeerTask task = new GetSyncReceiptsFromPeerTask(headers, protocolSchedule);
      PeerTaskExecutorResult<Map<BlockHeader, SyncTransactionReceipts>> getReceiptsResult =
          peerTaskExecutor.execute(task);
      if (getReceiptsResult.responseCode() == PeerTaskExecutorResponseCode.SUCCESS
          && getReceiptsResult.result().isPresent()) {
        Map<BlockHeader, SyncTransactionReceipts> taskResult = getReceiptsResult.result().get();
        taskResult
            .keySet()
            .forEach(
                (blockHeader) ->
                    getReceipts.merge(
                        blockHeader,
                        taskResult.get(blockHeader),
                        (initialReceipts, newReceipts) -> {
                          throw new IllegalStateException(
                              "Unexpectedly got receipts for block header already populated!");
                        }));
        // remove all the headers we found receipts for
        headers.removeAll(getReceipts.keySet());
      }
      // repeat until all headers have receipts
    } while (!headers.isEmpty());
    return getReceipts.entrySet().stream()
        .map((entry) -> new SyncTransactionReceiptsAndHeader(entry.getValue(), entry.getKey()))
        .toList();
  }

  private List<SyncBlock> getSyncBlocks(final List<BlockHeader> blockHeaders) {
    List<SyncBlock> syncBlocks = null;
    try {
      syncBlocks =
          CompleteSyncBlocksTask.forHeaders(
                  protocolSchedule, ethContext, blockHeaders, metricsSystem)
              .run()
              .join();
    } catch (final Exception e) {
      if (no_of_max_retries_reached.getAndIncrement() > 5) {
        LOG.debug("MAX_RETRIES_REACHED: {}", no_of_max_retries_reached.get());
        throw new RuntimeException("Have had 5 times MAX_RETRIES_REACHED", e);
      } else {
        LOG.debug(
            "Retry number {}. Exception while getting sync blocks is {}",
            no_of_max_retries_reached.get(),
            e);
        getSyncBlocks(blockHeaders);
      }
    }
    return syncBlocks;
  }

  private record SyncTransactionReceiptsAndHeader(
      SyncTransactionReceipts receipts, BlockHeader header) {}
}
