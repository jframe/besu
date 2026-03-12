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
package org.hyperledger.besu.ethereum.core.encoding.receipt;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.FrameReceipt;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class FrameReceiptEncodingTest {

  private static final Address FEE_PAYER =
      Address.fromHexString("0x1234567890123456789012345678901234567890");
  private static final Address LOG_EMITTER =
      Address.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  @Test
  void roundTripWithFeePayerAndFrameReceipts() {
    final Log log =
        new Log(
            LOG_EMITTER,
            Bytes.fromHexString("0xdeadbeef"),
            List.of(LogTopic.fromHexString("0x" + "ab".repeat(32))));

    final FrameReceipt frame0Receipt = new FrameReceipt(1, 21_000L, List.of(log));
    final FrameReceipt frame1Receipt = new FrameReceipt(1, 30_000L, List.of());

    final TransactionReceipt receipt =
        new TransactionReceipt(
            TransactionType.FRAME,
            1,
            51_000L,
            List.of(log),
            Optional.empty(),
            Optional.of(FEE_PAYER),
            Optional.of(List.of(frame0Receipt, frame1Receipt)));

    final TransactionReceipt decoded = encodeDecode(receipt, TransactionReceiptEncodingConfiguration.DEFAULT);

    assertThat(decoded.getTransactionType()).isEqualTo(TransactionType.FRAME);
    assertThat(decoded.getStatus()).isEqualTo(1);
    assertThat(decoded.getCumulativeGasUsed()).isEqualTo(51_000L);
    assertThat(decoded.getLogsList()).hasSize(1);

    assertThat(decoded.getFeePayerAddress()).contains(FEE_PAYER);

    assertThat(decoded.getFrameReceipts()).isPresent();
    final List<FrameReceipt> decodedFrames = decoded.getFrameReceipts().get();
    assertThat(decodedFrames).hasSize(2);

    assertThat(decodedFrames.get(0).status()).isEqualTo(1);
    assertThat(decodedFrames.get(0).gasUsed()).isEqualTo(21_000L);
    assertThat(decodedFrames.get(0).logs()).hasSize(1);

    assertThat(decodedFrames.get(1).status()).isEqualTo(1);
    assertThat(decodedFrames.get(1).gasUsed()).isEqualTo(30_000L);
    assertThat(decodedFrames.get(1).logs()).isEmpty();
  }

  @Test
  void roundTripFailedReceiptWithNoFeePayerOrFrameReceipts() {
    final TransactionReceipt receipt =
        new TransactionReceipt(
            TransactionType.FRAME,
            0,
            10_000L,
            List.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    final TransactionReceipt decoded = encodeDecode(receipt, TransactionReceiptEncodingConfiguration.DEFAULT);

    assertThat(decoded.getTransactionType()).isEqualTo(TransactionType.FRAME);
    assertThat(decoded.getStatus()).isEqualTo(0);
    assertThat(decoded.getCumulativeGasUsed()).isEqualTo(10_000L);
    // Zero fee_payer address is encoded as 20 zero bytes and decoded back as empty
    assertThat(decoded.getFeePayerAddress()).isEmpty();
  }

  @Test
  void roundTripWithStorageConfiguration() {
    final FrameReceipt frameReceipt = new FrameReceipt(1, 50_000L, List.of());

    final TransactionReceipt receipt =
        new TransactionReceipt(
            TransactionType.FRAME,
            1,
            50_000L,
            List.of(),
            Optional.empty(),
            Optional.of(FEE_PAYER),
            Optional.of(List.of(frameReceipt)));

    final TransactionReceipt decoded =
        encodeDecode(receipt, TransactionReceiptEncodingConfiguration.STORAGE_WITHOUT_COMPACTION);

    assertThat(decoded.getTransactionType()).isEqualTo(TransactionType.FRAME);
    assertThat(decoded.getFeePayerAddress()).contains(FEE_PAYER);
    assertThat(decoded.getFrameReceipts()).isPresent();
    assertThat(decoded.getFrameReceipts().get()).hasSize(1);
    assertThat(decoded.getFrameReceipts().get().get(0).gasUsed()).isEqualTo(50_000L);
  }

  @Test
  void feePayerZeroAddressDecodesAsEmpty() {
    // A receipt with no fee payer encodes the zero address; decoder returns empty
    final TransactionReceipt receipt =
        new TransactionReceipt(
            TransactionType.FRAME,
            1,
            21_000L,
            List.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(List.of()));

    final TransactionReceipt decoded =
        encodeDecode(receipt, TransactionReceiptEncodingConfiguration.DEFAULT);

    assertThat(decoded.getFeePayerAddress()).isEmpty();
    assertThat(decoded.getFrameReceipts()).isPresent();
    assertThat(decoded.getFrameReceipts().get()).isEmpty();
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }

  private static TransactionReceipt encodeDecode(
      final TransactionReceipt receipt, final TransactionReceiptEncodingConfiguration config) {
    final Bytes encoded =
        RLP.encode(out -> TransactionReceiptEncoder.writeTo(receipt, out, config));
    return TransactionReceiptDecoder.readFrom(RLP.input(encoded), true);
  }
}
