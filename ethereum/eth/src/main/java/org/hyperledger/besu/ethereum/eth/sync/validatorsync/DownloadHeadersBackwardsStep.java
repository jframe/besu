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

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.ValidationPolicy;
import org.hyperledger.besu.ethereum.eth.sync.range.RangeHeaders;
import org.hyperledger.besu.ethereum.eth.sync.range.TargetRange;
import org.hyperledger.besu.ethereum.eth.sync.tasks.RetryingGetHeaderFromPeerByNumberTask;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.FutureUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadHeadersBackwardsStep
    implements Function<ValidatorSyncRange, CompletableFuture<RangeHeaders>> {
  private static final Logger LOG = LoggerFactory.getLogger(DownloadHeadersBackwardsStep.class);
  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;
  private final ValidationPolicy validationPolicy;
  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;
  private static final int DEFAULT_RETRIES = 5;

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
  public CompletableFuture<RangeHeaders> apply(final ValidatorSyncRange validatorSyncRange) {
    final CompletableFuture<List<BlockHeader>> taskFuture = downloadHeaders(validatorSyncRange);
    final CompletableFuture<RangeHeaders> processedFuture =
        taskFuture.thenApply(headers -> processHeaders(validatorSyncRange, headers));
    FutureUtils.propagateCancellation(processedFuture, taskFuture);
    return processedFuture;
  }

  private CompletableFuture<List<BlockHeader>> downloadHeaders(
      final ValidatorSyncRange validatorSyncRange) {
    LOG.info(
        "Downloading headers for range {} to {}",
        validatorSyncRange.lowerBlockNumber(),
        validatorSyncRange.upperBlockNumber());
    // TODO should boundaries be inclusive?
    return RetryingGetHeaderFromPeerByNumberTask.endingAtNumber(
            protocolSchedule,
            ethContext,
            metricsSystem,
            validatorSyncRange.upperBlockNumber(),
            validatorSyncRange.count() + 1,
            DEFAULT_RETRIES)
        .run();
  }

  private RangeHeaders processHeaders(
      final ValidatorSyncRange validatorSyncRange, final List<BlockHeader> headers) {
    final Map<Long, BlockHeader> headersByBlockNumber =
        headers.stream().collect(Collectors.toMap(BlockHeader::getNumber, Function.identity()));
    headers.forEach(
        header -> {
          if (!checkHeaderInRange(header, validatorSyncRange)) {
            throw new IllegalStateException(
                "Received header outside of expected range: " + header.getNumber());
          }

          long parentBlockNumber =
              header.getNumber() > 0 ? header.getNumber() - 1 : 0; // TODO is this necessary?

          // skip the first header for validation as we don't have the parent header, this will be
          // validated by RangeHeadersValidationStep
          if (parentBlockNumber <= validatorSyncRange.lowerBlockNumber()) {
            return;
          }

          if (!validateHeader(header, headersByBlockNumber.get(parentBlockNumber))) {
            throw new IllegalStateException("Received invalid header: " + header.getNumber());
          }
        });

    final BlockHeader startHeader =
        headersByBlockNumber.get(validatorSyncRange.lowerBlockNumber() + 1);
    final Optional<BlockHeader> endHeader =
        Optional.ofNullable(headersByBlockNumber.get(validatorSyncRange.upperBlockNumber()));

    // RangeHeadersValidationStep requires headers to be sorted by asc blockNumber and exclude the
    // first header as that is assumed to be parent header from the previous range
    final List<BlockHeader> headersToImport =
        headers.stream()
            .sorted(Comparator.comparing(ProcessableBlockHeader::getNumber))
            .collect(Collectors.toList());
    //            .subList(1, headers.size());

    return new RangeHeaders(new TargetRange(startHeader, endHeader), headersToImport);
  }

  private boolean checkHeaderInRange(
      final BlockHeader header, final ValidatorSyncRange validatorSyncRange) {
    return header.getNumber() >= validatorSyncRange.lowerBlockNumber()
        && header.getNumber() <= validatorSyncRange.upperBlockNumber();
  }

  private boolean validateHeader(final BlockHeader header, final BlockHeader parent) {
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(header);
    final BlockHeaderValidator blockHeaderValidator = protocolSpec.getBlockHeaderValidator();
    return blockHeaderValidator.validateHeader(
        header, parent, protocolContext, validationPolicy.getValidationModeForNextBlock());
  }
}
