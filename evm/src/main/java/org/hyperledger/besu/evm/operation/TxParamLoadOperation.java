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

import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_FRAME_INDEX;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_TRANSACTION;

import org.hyperledger.besu.datatypes.Frame;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * EIP-8141 TXPARAMLOAD opcode (0xb0).
 *
 * <p>Pops an index from the stack and pushes the corresponding transaction field as a 32-byte word.
 * Mirrors CALLDATALOAD but reads structured transaction fields rather than raw calldata bytes.
 *
 * <p>Index mapping:
 *
 * <pre>
 *  0 — txType             (always 0x06 for FRAME)
 *  1 — nonce
 *  2 — sender             (left-padded to 32 bytes)
 *  3 — maxFeePerGas
 *  4 — maxPriorityFeePerGas
 *  5 — sigHash            (the transaction hash)
 *  6 — frameIndex         (0-based index of the current frame)
 *  7 — frameMode          (0=DEFAULT, 1=VERIFY, 2=SENDER)
 *  8 — frameTarget        (20-byte address, or 0 if entry-point)
 *  9 — frameGasLimit
 * </pre>
 *
 * Out-of-range indices push 0.
 */
public class TxParamLoadOperation extends AbstractOperation {

  /**
   * Instantiates TxParamLoadOperation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxParamLoadOperation(final GasCalculator gasCalculator) {
    super(0xb0, "TXPARAMLOAD", 1, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final int index = UInt256.fromBytes(frame.popStackItem()).intValue();
    final Transaction tx = frame.getContextVariable(FRAME_TX_TRANSACTION);
    if (tx == null) {
      frame.pushStackItem(UInt256.ZERO);
      return new OperationResult(2L, null);
    }
    final Integer frameIdx = frame.getContextVariable(FRAME_TX_FRAME_INDEX);
    final int fi = frameIdx == null ? 0 : frameIdx;

    final UInt256 result;
    switch (index) {
      case 0 -> result = UInt256.valueOf(0x06); // txType
      case 1 -> result = UInt256.valueOf(tx.getNonce()); // nonce
      case 2 -> result = UInt256.fromBytes(tx.getSender().getBytes()); // sender
      case 3 ->
          result =
              tx.getMaxFeePerGas()
                  .map(q -> UInt256.valueOf(q.getAsBigInteger()))
                  .orElse(UInt256.ZERO); // maxFeePerGas
      case 4 ->
          result =
              tx.getMaxPriorityFeePerGas()
                  .map(q -> UInt256.valueOf(q.getAsBigInteger()))
                  .orElse(UInt256.ZERO); // maxPriorityFeePerGas
      case 5 -> result = UInt256.fromBytes(tx.getHash().getBytes()); // sigHash
      case 6 -> result = UInt256.valueOf(fi); // frameIndex
      default -> {
        if (tx.getFrames().isEmpty()) {
          result = UInt256.ZERO;
          break;
        }
        final java.util.List<Frame> frames = tx.getFrames().get();
        if (fi < 0 || fi >= frames.size()) {
          result = UInt256.ZERO;
          break;
        }
        final Frame f = frames.get(fi);
        result =
            switch (index) {
              case 7 -> UInt256.valueOf(f.getMode()); // frameMode
              case 8 ->
                  f.getTarget()
                      .map(addr -> UInt256.fromBytes(addr.getBytes()))
                      .orElse(UInt256.ZERO); // frameTarget
              case 9 -> UInt256.valueOf(f.getGasLimit()); // frameGasLimit
              default -> UInt256.ZERO;
            };
      }
    }
    frame.pushStackItem(result);
    return new OperationResult(2L, null);
  }
}
