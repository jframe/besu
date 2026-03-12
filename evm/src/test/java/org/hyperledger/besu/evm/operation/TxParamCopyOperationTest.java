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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_PARAMS_BYTES;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class TxParamCopyOperationTest {

  private final BerlinGasCalculator gasCalculator = new BerlinGasCalculator();
  private final TxParamCopyOperation operation = new TxParamCopyOperation(gasCalculator);

  /**
   * Build a frame with the three copy operands on the stack (length first so destOffset is popped
   * first) and the given params bytes in the context variable.
   */
  private MessageFrame buildFrame(
      final long destOffset,
      final long srcOffset,
      final long length,
      final Bytes params,
      final long gas) {
    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .initialGas(gas)
            // pushed last = popped first: destOffset, srcOffset, length
            .pushStackItem(UInt256.valueOf(length))
            .pushStackItem(UInt256.valueOf(srcOffset))
            .pushStackItem(UInt256.valueOf(destOffset))
            .build();
    if (params != null) {
      frame.setContextVariable(FRAME_TX_PARAMS_BYTES, params);
    }
    return frame;
  }

  @Test
  void shouldCopyParamsBytesIntoMemory() {
    final Bytes params = Bytes.fromHexString("0xdeadbeef01020304");
    final MessageFrame frame = buildFrame(0, 0, params.size(), params, Long.MAX_VALUE);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // memory at offset 0 should contain the params bytes
    assertThat(frame.readMemory(0, params.size())).isEqualTo(params);
  }

  @Test
  void shouldCopySliceFromSrcOffset() {
    final Bytes params = Bytes.fromHexString("0x0102030405060708");
    // copy 4 bytes starting at srcOffset=2 into memory at destOffset=0
    final MessageFrame frame = buildFrame(0, 2, 4, params, Long.MAX_VALUE);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.readMemory(0, 4)).isEqualTo(params.slice(2, 4));
  }

  @Test
  void shouldSucceedWithZeroLength() {
    final Bytes params = Bytes.fromHexString("0xdeadbeef");
    final MessageFrame frame = buildFrame(0, 0, 0, params, Long.MAX_VALUE);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
  }

  @Test
  void shouldUseEmptyBytesWhenNoParamsInContext() {
    // No params bytes set — should copy zeros (empty source pads with zeros)
    final MessageFrame frame = buildFrame(0, 0, 4, null, Long.MAX_VALUE);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.readMemory(0, 4)).isEqualTo(Bytes.repeat((byte) 0, 4));
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final Bytes params = Bytes.fromHexString("0xdeadbeef");
    // Provide zero gas — copy cost will exceed it
    final MessageFrame frame = buildFrame(0, 0, 4, params, 0L);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }
}
