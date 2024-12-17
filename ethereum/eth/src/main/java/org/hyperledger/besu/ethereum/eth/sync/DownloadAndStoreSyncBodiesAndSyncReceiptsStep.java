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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadAndStoreSyncBodiesAndSyncReceiptsStep implements Consumer<List<BlockHeader>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DownloadAndStoreSyncBodiesAndSyncReceiptsStep.class);

  private final ProtocolSchedule protocolSchedule;
  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;
  private final ProtocolContext protocolContext;
  private final PeerTaskExecutor peerTaskExecutor;
  private final AtomicInteger no_of_max_retries_reached = new AtomicInteger();

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
  }

  @Override
  public void accept(final List<BlockHeader> blockHeaders) {
    final List<SyncBlock> blocks = getSyncBlocks(blockHeaders);

    // store the sync blocks
    for (final SyncBlock block : blocks) {
      protocolContext.getBlockchain().unsafeImportSyncBlock(block);
      LOG.atInfo().setMessage("Imported block {}").addArgument(block::toLogString).log();
    }

    // get the receipts for the headers
    final List<SyncTransactionReceiptsAndHeader> receiptsForBlocks = getReceipts(blockHeaders);

    // store the receipts
    for (final SyncTransactionReceiptsAndHeader receiptsForBlock : receiptsForBlocks) {
      protocolContext
          .getBlockchain()
          .unsafeImportSyncReceipts(receiptsForBlock.receipts(), receiptsForBlock.header());
      LOG.atInfo()
          .setMessage("Imported {} receipts for block {}")
          .addArgument(receiptsForBlock.receipts().size())
          .addArgument(receiptsForBlock.header()::toLogString)
          .log();
    }
  }

  private List<SyncTransactionReceiptsAndHeader> getReceipts(final List<BlockHeader> blockHeaders) {
    Map<BlockHeader, SyncTransactionReceipts> getReceipts = new HashMap<>();
    do {
      GetSyncReceiptsFromPeerTask task =
          new GetSyncReceiptsFromPeerTask(blockHeaders, protocolSchedule);
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
        blockHeaders.removeAll(getReceipts.keySet());
      }
      // repeat until all headers have receipts
    } while (!blockHeaders.isEmpty());
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
