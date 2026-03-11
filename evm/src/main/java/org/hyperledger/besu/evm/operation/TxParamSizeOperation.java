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

import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_PARAMS_BYTES;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * EIP-8141 TXPARAMSIZE opcode (0xb1).
 *
 * <p>Pushes the byte length of the raw RLP-encoded FRAME transaction onto the stack. Mirrors
 * CALLDATASIZE but reads from the context variable set by FrameTransactionProcessor.
 */
public class TxParamSizeOperation extends AbstractOperation {

  /**
   * Instantiates TxParamSizeOperation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxParamSizeOperation(final GasCalculator gasCalculator) {
    super(0xb1, "TXPARAMSIZE", 0, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final Bytes params = frame.getContextVariable(FRAME_TX_PARAMS_BYTES);
    frame.pushStackItem(UInt256.valueOf(params == null ? 0 : params.size()));
    return new OperationResult(2L, null);
  }
}
