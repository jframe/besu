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
package org.hyperledger.besu.ethereum.eth.sync.validatorsync;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.DownloadBodiesStep;
import org.hyperledger.besu.ethereum.eth.sync.tasks.GetReceiptsForHeadersTask;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadReceiptsFromHeadersStep
    implements Function<
        List<BlockHeader>, CompletableFuture<Map<BlockHeader, List<TransactionReceipt>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(DownloadBodiesStep.class);
  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;

  public DownloadReceiptsFromHeadersStep(
      final EthContext ethContext, final MetricsSystem metricsSystem) {
    this.ethContext = ethContext;
    this.metricsSystem = metricsSystem;
  }

  @Override
  public CompletableFuture<Map<BlockHeader, List<TransactionReceipt>>> apply(
      final List<BlockHeader> blockHeaders) {
    LOG.info(
        "Download {} receipts starting from {}",
        blockHeaders.size(),
        blockHeaders.getFirst() == null ? null : blockHeaders.getFirst().getNumber());
    return GetReceiptsForHeadersTask.forHeaders(ethContext, blockHeaders, metricsSystem).run();
  }
}
