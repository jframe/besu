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

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.ValidationPolicy;
import org.hyperledger.besu.ethereum.eth.sync.range.RangeHeaders;
import org.hyperledger.besu.ethereum.eth.sync.tasks.DownloadHeaderSequenceTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.FutureUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadHeadersBackwardsStep
    implements Function<SyncTargetNumberRange, CompletableFuture<RangeHeaders>> {
  private static final Logger LOG = LoggerFactory.getLogger(DownloadHeadersBackwardsStep.class);
  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;
  private final ValidationPolicy validationPolicy;
  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;

  public DownloadHeadersBackwardsStep(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final ValidationPolicy validationPolicy,
      final EthContext ethContext,
      final MetricsSystem metricsSystem) {
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.validationPolicy = validationPolicy;
    this.ethContext = ethContext;
    this.metricsSystem = metricsSystem;
  }

  @Override
  public CompletableFuture<RangeHeaders> apply(final SyncTargetNumberRange checkpointRange) {
    final CompletableFuture<List<BlockHeader>> taskFuture = downloadHeaders(checkpointRange);
    final CompletableFuture<RangeHeaders> processedFuture =
        taskFuture.thenApply(headers -> processHeaders(headers));
    FutureUtils.propagateCancellation(processedFuture, taskFuture);
    return processedFuture;
  }

  private CompletableFuture<List<BlockHeader>> downloadHeaders(final SyncTargetNumberRange range) {
    LOG.info(
        "Downloading headers for range {} to {}",
        range.lowerBlockNumber(),
        range.upperBlockNumber());
    if (range.getSegmentLengthExclusive() == 0) {
      // There are no extra headers to download.
      return completedFuture(emptyList());
    }
    return DownloadHeaderSequenceTask.endingAtBlockNumber(
            protocolSchedule,
            protocolContext,
            ethContext,
            range.upperBlockNumber(),
            range.getSegmentLengthExclusive(),
            validationPolicy,
            metricsSystem)
        .run();
  }

  private RangeHeaders processHeaders(final List<BlockHeader> headers) {
    return new RangeHeaders(null, headers);
  }
}
