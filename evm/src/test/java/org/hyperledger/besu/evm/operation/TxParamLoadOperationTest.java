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
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_FRAME_INDEX;
import static org.hyperledger.besu.evm.operation.FrameTxContextKeys.FRAME_TX_TRANSACTION;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Frame;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TxParamLoadOperationTest {

  private static final Address SENDER =
      Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  private static final Hash TX_HASH =
      Hash.fromHexString(
          "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  private final TxParamLoadOperation operation = new TxParamLoadOperation(new BerlinGasCalculator());

  // A real Frame (final class — not mockable)
  private static final Frame TX_FRAME =
      new Frame(Frame.MODE_VERIFY, Optional.of(SENDER), 100_000L, Bytes.EMPTY);

  private MessageFrame frame;
  private Transaction tx;

  @BeforeEach
  void setUp() {
    frame = mock(MessageFrame.class);
    tx = mock(Transaction.class);

    when(tx.getNonce()).thenReturn(42L);
    when(tx.getSender()).thenReturn(SENDER);
    when(tx.getMaxFeePerGas()).thenReturn(Optional.of(Wei.of(1_000_000_000L)));
    when(tx.getMaxPriorityFeePerGas()).thenReturn(Optional.of(Wei.of(2_000_000_000L)));
    when(tx.getHash()).thenReturn(TX_HASH);
    when(tx.getFrames()).thenReturn(Optional.of(List.of(TX_FRAME)));

    when(frame.getContextVariable(FRAME_TX_TRANSACTION)).thenReturn(tx);
    when(frame.getContextVariable(FRAME_TX_FRAME_INDEX)).thenReturn(0);
  }

  @Test
  void index0_returnsTxType() {
    pushIndex(0);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(2L);
    assertPushed(UInt256.valueOf(0x06));
  }

  @Test
  void index1_returnsNonce() {
    pushIndex(1);
    operation.execute(frame, null);
    assertPushed(UInt256.valueOf(42L));
  }

  @Test
  void index2_returnsSenderAddress() {
    pushIndex(2);
    operation.execute(frame, null);
    assertPushed(UInt256.fromBytes(Bytes32.leftPad(SENDER.getBytes())));
  }

  @Test
  void index3_returnsMaxFeePerGas() {
    pushIndex(3);
    operation.execute(frame, null);
    assertPushed(UInt256.valueOf(1_000_000_000L));
  }

  @Test
  void index3_returnsZeroWhenMaxFeePerGasAbsent() {
    when(tx.getMaxFeePerGas()).thenReturn(Optional.empty());
    pushIndex(3);
    operation.execute(frame, null);
    assertPushed(UInt256.ZERO);
  }

  @Test
  void index4_returnsMaxPriorityFeePerGas() {
    pushIndex(4);
    operation.execute(frame, null);
    assertPushed(UInt256.valueOf(2_000_000_000L));
  }

  @Test
  void index4_returnsZeroWhenMaxPriorityFeePerGasAbsent() {
    when(tx.getMaxPriorityFeePerGas()).thenReturn(Optional.empty());
    pushIndex(4);
    operation.execute(frame, null);
    assertPushed(UInt256.ZERO);
  }

  @Test
  void index5_returnsTxHash() {
    pushIndex(5);
    operation.execute(frame, null);
    assertPushed(UInt256.fromBytes(TX_HASH.getBytes()));
  }

  @Test
  void index6_returnsFrameIndex() {
    when(frame.getContextVariable(FRAME_TX_FRAME_INDEX)).thenReturn(0);
    pushIndex(6);
    operation.execute(frame, null);
    assertPushed(UInt256.ZERO);
  }

  @Test
  void index7_returnsFrameMode() {
    pushIndex(7);
    operation.execute(frame, null);
    assertPushed(UInt256.valueOf(Frame.MODE_VERIFY));
  }

  @Test
  void index8_returnsFrameTarget() {
    pushIndex(8);
    operation.execute(frame, null);
    assertPushed(UInt256.fromBytes(Bytes32.leftPad(SENDER.getBytes())));
  }

  @Test
  void index8_returnsZeroWhenFrameTargetAbsent() {
    final Frame noTargetFrame =
        new Frame(Frame.MODE_VERIFY, Optional.empty(), 100_000L, Bytes.EMPTY);
    when(tx.getFrames()).thenReturn(Optional.of(List.of(noTargetFrame)));
    pushIndex(8);
    operation.execute(frame, null);
    assertPushed(UInt256.ZERO);
  }

  @Test
  void index9_returnsFrameGasLimit() {
    pushIndex(9);
    operation.execute(frame, null);
    assertPushed(UInt256.valueOf(100_000L));
  }

  @Test
  void outOfRangeIndex_returnsZero() {
    pushIndex(10);
    operation.execute(frame, null);
    assertPushed(UInt256.ZERO);
  }

  @Test
  void noTransactionContext_returnsZero() {
    when(frame.getContextVariable(FRAME_TX_TRANSACTION)).thenReturn(null);
    pushIndex(1);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertPushed(UInt256.ZERO);
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }

  private void pushIndex(final int index) {
    when(frame.popStackItem()).thenReturn(UInt256.valueOf(index));
  }

  private void assertPushed(final UInt256 expected) {
    final ArgumentCaptor<Bytes> captor = forClass(Bytes.class);
    verify(frame).pushStackItem(captor.capture());
    assertThat(UInt256.fromBytes(captor.getValue())).isEqualTo(expected);
  }
}
