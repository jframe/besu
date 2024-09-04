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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult.BlockImportStatus;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.List;

public class MainnetBlockImporter implements BlockImporter {

  final BlockValidator blockValidator;
  private final OperationTimer importBlockValidationTimer;
  private final OperationTimer importBlockAppendTimer;

  public MainnetBlockImporter(
      final BlockValidator blockValidator, final MetricsSystem metricsSystem) {
    this.blockValidator = blockValidator;
    this.importBlockValidationTimer =
        metricsSystem.createTimer(
            BesuMetricCategory.SYNCHRONIZER,
            "importblock_validation_time",
            "Time taken to validate block during block import");
    this.importBlockAppendTimer =
        metricsSystem.createTimer(
            BesuMetricCategory.SYNCHRONIZER,
            "importblock_append_time",
            "Time taken to append block during block import");
  }

  @Override
  public synchronized BlockImportResult importBlock(
      final ProtocolContext context,
      final Block block,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode) {
    if (context.getBlockchain().contains(block.getHash())) {
      return new BlockImportResult(BlockImportStatus.ALREADY_IMPORTED);
    }

    final var result =
        blockValidator.validateAndProcessBlock(
            context, block, headerValidationMode, ommerValidationMode);

    if (result.isSuccessful()) {
      result
          .getYield()
          .ifPresent(
              processingOutputs ->
                  context.getBlockchain().appendBlock(block, processingOutputs.getReceipts()));
    }

    return new BlockImportResult(result.isSuccessful());
  }

  @Override
  public BlockImportResult fastImportBlock(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode) {
    final boolean isBlockValid;
    try (final var ignored = importBlockValidationTimer.startTimer()) {
      isBlockValid =
          blockValidator.fastBlockValidation(
              context,
              block,
              receipts,
              block.getBody().getRequests(),
              headerValidationMode,
              ommerValidationMode);
    }

    if (isBlockValid) {
      try (final var ignored = importBlockAppendTimer.startTimer()) {
        context.getBlockchain().appendBlock(block, receipts);
      }
      return new BlockImportResult(true);
    }

    return new BlockImportResult(false);
  }
}
