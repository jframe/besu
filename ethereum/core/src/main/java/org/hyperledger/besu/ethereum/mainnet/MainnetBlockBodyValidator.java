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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.ValidatedBlockBody;
import org.hyperledger.besu.ethereum.core.ValidatedTransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestsValidatorCoordinator;
import org.hyperledger.besu.evm.log.LogsBloomFilter;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainnetBlockBodyValidator implements BlockBodyValidator {

  private static final Logger LOG = LoggerFactory.getLogger(MainnetBlockBodyValidator.class);

  private static final int MAX_OMMERS = 2;

  private static final int MAX_GENERATION = 6;
  protected final ProtocolSchedule protocolSchedule;

  public MainnetBlockBodyValidator(final ProtocolSchedule protocolSchedule) {
    this.protocolSchedule = protocolSchedule;
  }

  @Override
  public boolean validateBody(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final Optional<List<Request>> requests,
      final Hash worldStateRootHash,
      final HeaderValidationMode ommerValidationMode) {

    if (!validateBodyLight(context, block, receipts, requests, ommerValidationMode)) {
      return false;
    }

    if (!validateStateRoot(
        block.getHeader(), block.getHeader().getStateRoot(), worldStateRootHash)) {
      LOG.warn("Invalid block RLP : {}", block.toRlp().toHexString());
      receipts.forEach(
          receipt ->
              LOG.warn("Transaction receipt found in the invalid block {}", receipt.toString()));
      return false;
    }

    return true;
  }

  @Override
  public boolean validateBodyLight(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final Optional<List<Request>> requests,
      final HeaderValidationMode ommerValidationMode) {
    final BlockHeader header = block.getHeader();
    final BlockBody blockBody = block.getBody();

    final long gasUsed =
        receipts.isEmpty() ? 0 : receipts.get(receipts.size() - 1).getCumulativeGasUsed();
    if (!validateGasUsed(header, header.getGasUsed(), gasUsed)) {
      return false;
    }

    if (!validateOmmers(context, header, blockBody.getOmmers(), ommerValidationMode)) {
      return false;
    }

    if (!validateWithdrawals(block)) {
      return false;
    }

    // If the block body and receipts are already validated, we can skip the rest of the validation
    final boolean isValidatedBlockBody = blockBody instanceof ValidatedBlockBody;
    final boolean receiptsAreValidated =
        receipts.stream().allMatch(r -> r instanceof ValidatedTransactionReceipt);
    if (isValidatedBlockBody && receiptsAreValidated) {
      return true;
    }

    if (!validateTransactionsRoot(header, blockBody)) {
      return false;
    }

    if (!validateReceiptsRoot(header, receipts)) {
      return false;
    }

    if (!validateLogsBloom(header, header.getLogsBloom(), BodyValidation.logsBloom(receipts))) {
      return false;
    }

    if (!validateOmmersHash(header, blockBody)) {
      return false;
    }

    if (!validateRequests(block, requests, receipts)) {
      return false;
    }

    return true;
  }

  private static boolean validateTransactionsRoot(
      final BlockHeader header, final BlockBody blockBody) {
    final Hash expected = header.getTransactionsRoot();
    final Hash actual = BodyValidation.transactionsRoot(blockBody.getTransactions());
    if (!expected.equals(actual)) {
      LOG.info(
          "Invalid block {}: transaction root mismatch (expected={}, actual={})",
          header.toLogString(),
          expected,
          actual);
      return false;
    }

    return true;
  }

  private static boolean validateLogsBloom(
      final BlockHeader header, final LogsBloomFilter expected, final LogsBloomFilter actual) {
    if (!expected.equals(actual)) {
      LOG.warn(
          "Invalid block {}: logs bloom filter mismatch (expected={}, actual={})",
          header.toLogString(),
          expected,
          actual);
      return false;
    }

    return true;
  }

  private static boolean validateGasUsed(
      final BlockHeader header, final long expected, final long actual) {
    if (expected != actual) {
      LOG.warn(
          "Invalid block {}: gas used mismatch (expected={}, actual={})",
          header.toLogString(),
          expected,
          actual);
      return false;
    }

    return true;
  }

  private static boolean validateReceiptsRoot(
      final BlockHeader header, final List<TransactionReceipt> receipts) {
    final Bytes32 expectedReceiptsRoot = header.getReceiptsRoot();
    final Bytes32 actualReceiptsRoot = BodyValidation.receiptsRoot(receipts);

    if (!expectedReceiptsRoot.equals(actualReceiptsRoot)) {
      LOG.warn(
          "Invalid block {}: receipts root mismatch (expected={}, actual={})",
          header.toLogString(),
          expectedReceiptsRoot,
          actualReceiptsRoot);
      return false;
    }

    return true;
  }

  private static boolean validateStateRoot(
      final BlockHeader header, final Bytes32 expected, final Bytes32 actual) {
    if (!expected.equals(actual)) {
      LOG.warn(
          "Invalid block {}: state root mismatch (expected={}, actual={})",
          header.toLogString(),
          expected,
          actual);
      return false;
    }

    return true;
  }

  private static boolean validateOmmersHash(final BlockHeader header, final BlockBody blockBody) {
    final Bytes32 actualOmmersHash = BodyValidation.ommersHash(blockBody.getOmmers());
    final Bytes32 expectedOmmersHash = header.getOmmersHash();

    if (!expectedOmmersHash.equals(actualOmmersHash)) {
      LOG.warn(
          "Invalid block {}: ommers hash mismatch (expected={}, actual={})",
          header.toLogString(),
          expectedOmmersHash,
          actualOmmersHash);
      return false;
    }

    return true;
  }

  private boolean validateOmmers(
      final ProtocolContext context,
      final BlockHeader header,
      final List<BlockHeader> ommers,
      final HeaderValidationMode ommerValidationMode) {
    if (ommers.size() > MAX_OMMERS) {
      LOG.warn(
          "Invalid block {}: ommer count {} exceeds ommer limit {}",
          header.toLogString(),
          ommers.size(),
          MAX_OMMERS);
      return false;
    }

    if (!areOmmersUnique(ommers)) {
      LOG.warn("Invalid block {}: ommers are not unique", header.toLogString());
      return false;
    }

    for (final BlockHeader ommer : ommers) {
      if (!isOmmerValid(context, header, ommer, ommerValidationMode)) {
        LOG.warn("Invalid block {}: ommer is invalid", header.toLogString());
        return false;
      }
    }

    return true;
  }

  private static boolean areOmmersUnique(final List<BlockHeader> ommers) {
    final Set<BlockHeader> uniqueOmmers = new HashSet<>(ommers);

    return uniqueOmmers.size() == ommers.size();
  }

  private static boolean areSiblings(final BlockHeader a, final BlockHeader b) {
    // Siblings cannot be the same.
    if (a.equals(b)) {
      return false;
    }

    return a.getParentHash().equals(b.getParentHash());
  }

  private boolean isOmmerValid(
      final ProtocolContext context,
      final BlockHeader current,
      final BlockHeader ommer,
      final HeaderValidationMode ommerValidationMode) {
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(ommer);
    if (!protocolSpec
        .getOmmerHeaderValidator()
        .validateHeader(ommer, context, ommerValidationMode)) {
      return false;
    }

    if (!ommerValidationMode.isFormOfLightValidation()) {
      return isOmmerSiblingOfAncestor(context, current, ommer);
    } else {
      return true;
    }
  }

  private boolean isOmmerSiblingOfAncestor(
      final ProtocolContext context, final BlockHeader current, final BlockHeader ommer) {
    // The current block is guaranteed to have a parent because it's a valid header.
    final long lastAncestorBlockNumber = Math.max(current.getNumber() - MAX_GENERATION, 0);

    BlockHeader previous = current;
    while (previous.getNumber() > lastAncestorBlockNumber) {
      final BlockHeader ancestor =
          context.getBlockchain().getBlockHeader(previous.getParentHash()).get();
      if (areSiblings(ommer, ancestor)) {
        return true;
      }
      previous = ancestor;
    }
    return false;
  }

  private boolean validateWithdrawals(final Block block) {
    final WithdrawalsValidator withdrawalsValidator =
        protocolSchedule.getByBlockHeader(block.getHeader()).getWithdrawalsValidator();

    if (!withdrawalsValidator.validateWithdrawals(block.getBody().getWithdrawals())) {
      return false;
    }

    if (!withdrawalsValidator.validateWithdrawalsRoot(block)) {
      return false;
    }

    return true;
  }

  private boolean validateRequests(
      final Block block,
      final Optional<List<Request>> requests,
      final List<TransactionReceipt> receipts) {
    final RequestsValidatorCoordinator requestValidator =
        protocolSchedule.getByBlockHeader(block.getHeader()).getRequestsValidatorCoordinator();
    return requestValidator.validate(block, requests, receipts);
  }
}
