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
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_APPROVAL_SCOPE;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_IN_VERIFY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ApproveOperationTest {

  private final ApproveOperation operation = new ApproveOperation(new BerlinGasCalculator());

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void shouldApproveWithValidScopeInsideVerifyFrame(final int scope) {
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getContextVariable(FRAME_TX_IN_VERIFY)).thenReturn(Boolean.TRUE);
    when(frame.popStackItem()).thenReturn(UInt256.valueOf(scope));

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(100L);
    verify(frame).setContextVariable(FRAME_TX_APPROVAL_SCOPE, scope);
  }

  @Test
  void shouldHaltWhenCalledOutsideVerifyFrame() {
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getContextVariable(FRAME_TX_IN_VERIFY)).thenReturn(null);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(result.getGasCost()).isEqualTo(100L);
  }

  @Test
  void shouldHaltWhenVerifyFlagIsFalse() {
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getContextVariable(FRAME_TX_IN_VERIFY)).thenReturn(Boolean.FALSE);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 100, Integer.MAX_VALUE})
  void shouldHaltWithOutOfRangeScope(final int scope) {
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getContextVariable(FRAME_TX_IN_VERIFY)).thenReturn(Boolean.TRUE);
    when(frame.popStackItem()).thenReturn(UInt256.valueOf(scope));

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(result.getGasCost()).isEqualTo(100L);
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }
}
