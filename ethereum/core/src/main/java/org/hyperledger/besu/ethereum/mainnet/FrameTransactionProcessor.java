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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_APPROVAL_SCOPE;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_FRAME_INDEX;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_IN_VERIFY;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_PARAMS_BYTES;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_TRANSACTION;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Frame;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes EIP-8141 FRAME transactions. Each transaction contains one or more frames that execute
 * sequentially. Authentication is delegated to VERIFY frames that run in a read-only snapshot; only
 * if every VERIFY frame calls APPROVE does the transaction proceed.
 */
public class FrameTransactionProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FrameTransactionProcessor.class);

  /**
   * The ENTRY_POINT address. Frames with an empty target field are dispatched here. DEFAULT frames
   * use ENTRY_POINT as msg.sender.
   */
  public static final Address ENTRY_POINT =
      Address.fromHexString("0x000000000000000000000000000000000000AA");

  private final FeeMarket feeMarket;
  private final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator;
  private final MainnetTransactionProcessor parent;
  private final int maxStackSize;

  FrameTransactionProcessor(
      final GasCalculator gasCalculator,
      final FeeMarket feeMarket,
      final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator,
      final MainnetTransactionProcessor parent,
      final int maxStackSize) {
    this.feeMarket = feeMarket;
    this.coinbaseFeePriceCalculator = coinbaseFeePriceCalculator;
    this.parent = parent;
    this.maxStackSize = maxStackSize;
  }

  /**
   * Processes a FRAME transaction.
   *
   * @param worldState the mutable world state
   * @param blockHeader the current block header
   * @param transaction the FRAME transaction to execute
   * @param miningBeneficiary the address that receives the miner tip
   * @param operationTracer the tracer for EVM operations
   * @param blockHashLookup lookup for BLOCKHASH opcode
   * @param transactionValidationParams validation parameters
   * @return the processing result
   */
  public TransactionProcessingResult process(
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final OperationTracer operationTracer,
      final BlockHashLookup blockHashLookup,
      final TransactionValidationParams transactionValidationParams) {

    final Address senderAddress = transaction.getSender();
    final List<Frame> frames = transaction.getFrames().orElseThrow();
    final Bytes txRlpBytes = transaction.encoded();

    // Effective gas price: min(maxFeePerGas, baseFee + maxPriorityFeePerGas)
    final Wei transactionGasPrice =
        feeMarket.getTransactionPriceCalculator().price(transaction, blockHeader.getBaseFee());

    // Upfront cost: Σ(frame.gasLimit) × effectiveGasPrice
    final long totalFrameGas = frames.stream().mapToLong(Frame::getGasLimit).sum();
    final Wei upfrontGasCost = transactionGasPrice.multiply(totalFrameGas);

    // Deduct upfront cost and increment nonce on sender
    final MutableAccount sender = worldState.getOrCreateSenderAccount(senderAddress);
    try {
      sender.decrementBalance(upfrontGasCost);
    } catch (final IllegalStateException ise) {
      if (!transactionValidationParams.allowUnderpriced()) {
        throw ise;
      }
    }
    sender.incrementNonce();

    // Find first VERIFY frame index; frames before it are pre-VERIFY DEFAULT frames
    final int firstVerifyIdx =
        IntStream.range(0, frames.size())
            .filter(i -> frames.get(i).getMode() == Frame.MODE_VERIFY)
            .findFirst()
            .orElse(frames.size());

    // One shared world updater for all committed state changes
    final WorldUpdater mainUpdater = worldState.updater();
    operationTracer.traceStartTransaction(mainUpdater, transaction);

    final List<Log> allLogs = new ArrayList<>();
    long totalGasUsed = 0L;

    // --- 1. Execute pre-VERIFY DEFAULT frames ---
    for (int i = 0; i < firstVerifyIdx; i++) {
      final Frame frame = frames.get(i);
      final MessageFrame msgFrame =
          buildMessageFrame(
              mainUpdater,
              transaction,
              frame,
              i,
              /* inVerify= */ false,
              txRlpBytes,
              transactionGasPrice,
              blockHeader,
              miningBeneficiary,
              blockHashLookup);

      executeMessageFrame(msgFrame, operationTracer);

      final long gasUsedByFrame = frame.getGasLimit() - msgFrame.getRemainingGas();
      totalGasUsed += gasUsedByFrame;

      if (msgFrame.getState() != MessageFrame.State.COMPLETED_SUCCESS) {
        LOG.debug(
            "FRAME tx {} pre-VERIFY frame {} failed, reverting",
            transaction.getHash(),
            i);
        mainUpdater.revert();
        return buildFailedResult(
            transaction,
            sender,
            transactionGasPrice,
            totalGasUsed,
            totalFrameGas,
            msgFrame,
            worldState,
            miningBeneficiary,
            blockHeader,
            operationTracer);
      }
      allLogs.addAll(msgFrame.getLogs());
    }

    // --- 2. Execute VERIFY frames in read-only snapshots ---
    Address feePayerAddress = senderAddress;
    for (int i = firstVerifyIdx; i < frames.size(); i++) {
      final Frame frame = frames.get(i);
      if (frame.getMode() != Frame.MODE_VERIFY) {
        break; // end of VERIFY block
      }

      final WorldUpdater snapshotUpdater = mainUpdater.updater();
      final MessageFrame msgFrame =
          buildMessageFrame(
              snapshotUpdater,
              transaction,
              frame,
              i,
              /* inVerify= */ true,
              txRlpBytes,
              transactionGasPrice,
              blockHeader,
              miningBeneficiary,
              blockHashLookup);

      executeMessageFrame(msgFrame, operationTracer);
      snapshotUpdater.revert(); // discard all VERIFY state changes

      final long gasUsedByFrame = frame.getGasLimit() - msgFrame.getRemainingGas();
      totalGasUsed += gasUsedByFrame;

      // VERIFY frame must call APPROVE or the whole transaction is rejected
      final Integer approvalScope = msgFrame.getContextVariable(FRAME_TX_APPROVAL_SCOPE);
      if (approvalScope == null || msgFrame.getState() != MessageFrame.State.COMPLETED_SUCCESS) {
        LOG.debug(
            "FRAME tx {} VERIFY frame {} did not APPROVE, reverting", transaction.getHash(), i);
        mainUpdater.revert();
        return buildFailedResult(
            transaction,
            sender,
            transactionGasPrice,
            totalGasUsed,
            totalFrameGas,
            msgFrame,
            worldState,
            miningBeneficiary,
            blockHeader,
            operationTracer);
      }

      // First APPROVE with scope ≥ 1 designates a paymaster as fee payer
      if (feePayerAddress.equals(senderAddress) && approvalScope >= 1) {
        feePayerAddress = frame.getTarget().orElse(ENTRY_POINT);
      }
    }

    // --- 3. Execute post-VERIFY (non-VERIFY) frames ---
    for (int i = firstVerifyIdx; i < frames.size(); i++) {
      final Frame frame = frames.get(i);
      if (frame.getMode() == Frame.MODE_VERIFY) {
        continue; // already processed above
      }

      final MessageFrame msgFrame =
          buildMessageFrame(
              mainUpdater,
              transaction,
              frame,
              i,
              /* inVerify= */ false,
              txRlpBytes,
              transactionGasPrice,
              blockHeader,
              miningBeneficiary,
              blockHashLookup);

      executeMessageFrame(msgFrame, operationTracer);

      final long gasUsedByFrame = frame.getGasLimit() - msgFrame.getRemainingGas();
      totalGasUsed += gasUsedByFrame;

      if (msgFrame.getState() != MessageFrame.State.COMPLETED_SUCCESS) {
        LOG.debug(
            "FRAME tx {} post-VERIFY frame {} failed, reverting", transaction.getHash(), i);
        mainUpdater.revert();
        return buildFailedResult(
            transaction,
            sender,
            transactionGasPrice,
            totalGasUsed,
            totalFrameGas,
            msgFrame,
            worldState,
            miningBeneficiary,
            blockHeader,
            operationTracer);
      }
      allLogs.addAll(msgFrame.getLogs());
    }

    // --- 4. Commit all state changes ---
    mainUpdater.commit();

    // --- 5. Settle gas: refund sender, pay miner from feePayer ---
    final long unusedGas = totalFrameGas - totalGasUsed;
    final long refundedGas = Math.min(totalGasUsed / 5, unusedGas); // EIP-3529
    final long gasSpent = totalFrameGas - refundedGas;

    // Refund unused gas to sender
    final Wei refundWei = transactionGasPrice.multiply(refundedGas);
    sender.incrementBalance(refundWei);

    // Pay miner tip from feePayer
    final Wei coinbaseWeiDelta =
        coinbaseFeePriceCalculator.price(gasSpent, transactionGasPrice, blockHeader.getBaseFee());
    if (!coinbaseWeiDelta.isZero()) {
      final MutableAccount feePayer = worldState.getOrCreateSenderAccount(feePayerAddress);
      feePayer.decrementBalance(coinbaseWeiDelta);
      worldState.getOrCreate(miningBeneficiary).incrementBalance(coinbaseWeiDelta);
    }

    operationTracer.traceEndTransaction(
        worldState.updater(),
        transaction,
        true,
        Bytes.EMPTY,
        allLogs,
        totalGasUsed,
        java.util.Set.of(),
        0L);

    return TransactionProcessingResult.successful(
        allLogs,
        totalGasUsed,
        unusedGas + refundedGas,
        Bytes.EMPTY,
        Optional.empty(),
        ValidationResult.valid());
  }

  private MessageFrame buildMessageFrame(
      final WorldUpdater updater,
      final Transaction transaction,
      final Frame frame,
      final int frameIndex,
      final boolean inVerify,
      final Bytes txRlpBytes,
      final Wei gasPrice,
      final ProcessableBlockHeader blockHeader,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup) {

    final Address senderAddress = transaction.getSender();
    final Address msgSender =
        (frame.getMode() == Frame.MODE_SENDER) ? senderAddress : ENTRY_POINT;
    final Address target = frame.getTarget().orElse(ENTRY_POINT);

    // Load code at target
    final Account targetAccount = updater.get(target);
    final Code code =
        targetAccount == null || targetAccount.getCodeHash() == null
            ? Code.EMPTY_CODE
            : parent
                .getMessageCallProcessor()
                .getOrCreateCachedJumpDest(targetAccount.getCodeHash(), targetAccount.getCode());

    final Map<String, Object> contextVars = new HashMap<>();
    contextVars.put(FRAME_TX_TRANSACTION, transaction);
    contextVars.put(FRAME_TX_FRAME_INDEX, frameIndex);
    contextVars.put(FRAME_TX_IN_VERIFY, inVerify);
    contextVars.put(FRAME_TX_PARAMS_BYTES, txRlpBytes);

    return MessageFrame.builder()
        .type(MessageFrame.Type.MESSAGE_CALL)
        .maxStackSize(maxStackSize)
        .worldUpdater(updater.updater())
        .initialGas(frame.getGasLimit())
        .originator(senderAddress)
        .gasPrice(gasPrice)
        .blobGasPrice(Wei.ZERO)
        .sender(msgSender)
        .address(target)
        .contract(target)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .inputData(frame.getData())
        .code(code)
        .blockValues(blockHeader)
        .completer(__ -> {})
        .miningBeneficiary(miningBeneficiary)
        .blockHashLookup(blockHashLookup)
        .contextVariables(contextVars)
        .build();
  }

  private void executeMessageFrame(
      final MessageFrame msgFrame, final OperationTracer operationTracer) {
    final Deque<MessageFrame> stack = msgFrame.getMessageFrameStack();
    while (!stack.isEmpty()) {
      parent.process(stack.peekFirst(), operationTracer);
    }
  }

  private TransactionProcessingResult buildFailedResult(
      final Transaction transaction,
      final MutableAccount sender,
      final Wei transactionGasPrice,
      final long totalGasUsed,
      final long totalFrameGas,
      final MessageFrame failedFrame,
      final WorldUpdater worldState,
      final Address miningBeneficiary,
      final ProcessableBlockHeader blockHeader,
      final OperationTracer operationTracer) {

    // Charge for gas used, refund remainder
    final long unusedGas = totalFrameGas - totalGasUsed;
    final Wei refundWei = transactionGasPrice.multiply(unusedGas);
    sender.incrementBalance(refundWei);

    // Pay miner for gas used (from sender, since VERIFY failed / frame reverted)
    final Wei coinbaseWeiDelta =
        coinbaseFeePriceCalculator.price(
            totalGasUsed, transactionGasPrice, blockHeader.getBaseFee());
    if (!coinbaseWeiDelta.isZero()) {
      sender.decrementBalance(coinbaseWeiDelta);
      worldState.getOrCreate(miningBeneficiary).incrementBalance(coinbaseWeiDelta);
    }

    operationTracer.traceEndTransaction(
        worldState.updater(),
        transaction,
        false,
        Bytes.EMPTY,
        List.of(),
        totalGasUsed,
        java.util.Set.of(),
        0L);

    return TransactionProcessingResult.failed(
        totalGasUsed,
        unusedGas,
        ValidationResult.invalid(
            TransactionInvalidReason.EXECUTION_HALTED,
            failedFrame
                .getExceptionalHaltReason()
                .map(r -> r.getDescription())
                .orElse("frame execution failed")),
        failedFrame.getRevertReason(),
        failedFrame.getExceptionalHaltReason(),
        Optional.empty());
  }
}
