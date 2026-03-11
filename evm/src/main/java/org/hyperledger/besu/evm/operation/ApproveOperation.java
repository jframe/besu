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

import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_APPROVAL_SCOPE;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_IN_VERIFY;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * EIP-8141 APPROVE opcode (0xaa).
 *
 * <p>Valid only inside a VERIFY frame. Pops a scope value from the stack (0 = sender only, 1 =
 * sender + paymaster, 2 = paymaster only) and records it as the approval scope for this
 * transaction. If called outside a VERIFY frame, or with an out-of-range scope value, the
 * execution halts exceptionally.
 */
public class ApproveOperation extends AbstractOperation {

  /** Gas cost of APPROVE — approximately a warm SSTORE. */
  private static final long GAS_COST = 100L;

  /**
   * Instantiates ApproveOperation.
   *
   * @param gasCalculator the gas calculator
   */
  public ApproveOperation(final GasCalculator gasCalculator) {
    super(0xaa, "APPROVE", 1, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (!Boolean.TRUE.equals(frame.getContextVariable(FRAME_TX_IN_VERIFY))) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final UInt256 scopeVal = UInt256.fromBytes(frame.popStackItem());
    if (scopeVal.fitsInt() && scopeVal.intValue() <= 2) {
      frame.setContextVariable(FRAME_TX_APPROVAL_SCOPE, scopeVal.intValue());
      return new OperationResult(GAS_COST, null);
    }
    return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
  }
}
