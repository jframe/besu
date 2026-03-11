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
package org.hyperledger.besu.evm.operation;

import static org.hyperledger.besu.evm.internal.Words.clampedToLong;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_PARAMS_BYTES;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * EIP-8141 TXPARAMCOPY opcode (0xb2).
 *
 * <p>Copies a slice of the raw RLP-encoded FRAME transaction into EVM memory. Mirrors CALLDATACOPY
 * but reads from the context variable set by FrameTransactionProcessor instead of frame input data.
 *
 * <p>Stack inputs: [destOffset, srcOffset, length] (all consumed).
 */
public class TxParamCopyOperation extends AbstractOperation {

  /**
   * Instantiates TxParamCopyOperation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxParamCopyOperation(final GasCalculator gasCalculator) {
    super(0xb2, "TXPARAMCOPY", 3, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final long destOffset = clampedToLong(frame.popStackItem());
    final long srcOffset = clampedToLong(frame.popStackItem());
    final long length = clampedToLong(frame.popStackItem());

    final long cost = gasCalculator().dataCopyOperationGasCost(frame, destOffset, length);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    if (length == 0) {
      return new OperationResult(cost, null);
    }

    final Bytes params = frame.getContextVariable(FRAME_TX_PARAMS_BYTES);
    final Bytes src = params == null ? Bytes.EMPTY : params;
    frame.writeMemory(destOffset, srcOffset, length, src, true);
    return new OperationResult(cost, null);
  }
}
