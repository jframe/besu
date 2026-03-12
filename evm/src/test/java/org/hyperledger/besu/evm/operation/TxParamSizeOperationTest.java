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
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TxParamSizeOperationTest {

  private final TxParamSizeOperation operation = new TxParamSizeOperation(new BerlinGasCalculator());

  @Test
  void shouldReturnParamsBytesLength() {
    final Bytes params = Bytes.fromHexString("0xdeadbeefcafe");
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getContextVariable(FRAME_TX_PARAMS_BYTES)).thenReturn(params);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(2L);
    assertPushed(frame, UInt256.valueOf(params.size()));
  }

  @Test
  void shouldReturnZeroWhenNoBytesInContext() {
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getContextVariable(FRAME_TX_PARAMS_BYTES)).thenReturn(null);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertPushed(frame, UInt256.ZERO);
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }

  private void assertPushed(final MessageFrame frame, final UInt256 expected) {
    final ArgumentCaptor<Bytes> captor = forClass(Bytes.class);
    verify(frame).pushStackItem(captor.capture());
    assertThat(UInt256.fromBytes(captor.getValue())).isEqualTo(expected);
  }
}
