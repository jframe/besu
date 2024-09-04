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
package org.hyperledger.besu.ethereum.eth.sync.validatorsync;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.tasks.CompleteBlocksTask;
import org.hyperledger.besu.ethereum.eth.sync.tasks.GetReceiptsForHeadersTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadBodiesAndReceiptsStep
    implements Function<List<BlockHeader>, CompletableFuture<List<BlockWithReceipts>>> {
  private static final Logger LOG = LoggerFactory.getLogger(DownloadBodiesAndReceiptsStep.class);
  private final ProtocolSchedule protocolSchedule;
  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;

  public DownloadBodiesAndReceiptsStep(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final MetricsSystem metricsSystem) {
    this.protocolSchedule = protocolSchedule;
    this.ethContext = ethContext;
    this.metricsSystem = metricsSystem;
  }

  @Override
  public CompletableFuture<List<BlockWithReceipts>> apply(final List<BlockHeader> blockHeaders) {
    LOG.info(
        "Downloading {} blocks and receipts for headers starting from {}",
        blockHeaders.size(),
        blockHeaders.getFirst());
    final EthScheduler ethScheduler = ethContext.getScheduler();
    final CompletableFuture<List<Block>> downloadBlocks =
        ethScheduler.scheduleSyncWorkerTask(
            CompleteBlocksTask.forHeaders(
                protocolSchedule, ethContext, blockHeaders, metricsSystem));
    final CompletableFuture<Map<BlockHeader, List<TransactionReceipt>>> downloadReceipts =
        ethScheduler.scheduleSyncWorkerTask(
            GetReceiptsForHeadersTask.forHeaders(ethContext, blockHeaders, metricsSystem));

    return downloadBlocks.thenCombine(
        downloadReceipts,
        (blockList, receiptsMap) -> {
          // Combine the results of both tasks
          return blockList.stream()
              .map(block -> new BlockWithReceipts(block, receiptsMap.get(block.getHeader())))
              .collect(Collectors.toList());
        });
  }
}
