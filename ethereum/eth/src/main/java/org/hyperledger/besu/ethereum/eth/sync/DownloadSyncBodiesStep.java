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

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.tasks.CompleteBlocksTask;
import org.hyperledger.besu.ethereum.eth.sync.tasks.CompleteSyncBlocksTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadSyncBodiesStep
    implements Function<List<BlockHeader>, CompletableFuture<List<SyncBlock>>> {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadSyncBodiesStep.class);

  private final ProtocolSchedule protocolSchedule;
  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;

  public DownloadSyncBodiesStep(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final MetricsSystem metricsSystem) {
    this.protocolSchedule = protocolSchedule;
    this.ethContext = ethContext;
    this.metricsSystem = metricsSystem;
  }

  @Override
  public CompletableFuture<List<SyncBlock>> apply(final List<BlockHeader> blockHeaders) {
    final AtomicInteger no_of_max_retries_reached = new AtomicInteger();
    return CompleteSyncBlocksTask.forHeaders(protocolSchedule, ethContext, blockHeaders, metricsSystem)
            .run()
            .handle(
                    (result, error) -> {
                      if (error != null && error.getMessage().contains("MAX_RETRIES_REACHED")) {
                        no_of_max_retries_reached.getAndIncrement();
                        if (no_of_max_retries_reached.get() > 5) {
                          throw new RuntimeException("Have had 5 times MAX_RETRIES_REACHED", error);
                        }
                        LOG.debug("MAX_RETRIES_REACHED: {}", no_of_max_retries_reached.get());
                        return apply(blockHeaders).join();
                      } else {
                        return result;
                      }
                    });
  }
}
