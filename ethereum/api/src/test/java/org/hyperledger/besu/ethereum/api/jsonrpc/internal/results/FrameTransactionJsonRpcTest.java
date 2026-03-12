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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Frame;
import org.hyperledger.besu.datatypes.FrameReceipt;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.query.TransactionReceiptWithMetadata;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class FrameTransactionJsonRpcTest {

  private static final BigInteger CHAIN_ID = BigInteger.valueOf(8141);
  private static final Address SENDER =
      Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  private static final Address TARGET =
      Address.fromHexString("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
  private static final Address FEE_PAYER =
      Address.fromHexString("0x1234567890123456789012345678901234567890");

  private final ObjectMapper mapper = new ObjectMapper();

  // --- TransactionCompleteResult tests ---

  @Test
  void frameTransactionHasNoSignatureFields() {
    final TransactionCompleteResult result = buildCompleteResult();

    assertThat(result.getType()).isEqualTo("0x6");
    assertThat(result.getFrom()).isEqualTo(SENDER.toString());
    assertThat(result.getR()).isNull();
    assertThat(result.getS()).isNull();
    assertThat(result.getV()).isNull();
    assertThat(result.getYParity()).isNull();
  }

  @Test
  void frameTransactionIncludesFramesList() {
    final TransactionCompleteResult result = buildCompleteResult();

    assertThat(result.getFrames()).isNotNull();
    assertThat(result.getFrames()).hasSize(2);

    final TransactionCompleteResult.FrameResult frame0 = result.getFrames().get(0);
    assertThat(frame0.mode()).isEqualTo(Quantity.create(Frame.MODE_VERIFY));
    assertThat(frame0.to()).isEqualTo(TARGET.getBytes().toHexString());
    assertThat(frame0.gas()).isEqualTo(Quantity.create(100_000L));
    assertThat(frame0.input()).isEqualTo("0x");

    final TransactionCompleteResult.FrameResult frame1 = result.getFrames().get(1);
    assertThat(frame1.mode()).isEqualTo(Quantity.create(Frame.MODE_DEFAULT));
    assertThat(frame1.to()).isNull(); // empty target
    assertThat(frame1.gas()).isEqualTo(Quantity.create(50_000L));
    assertThat(frame1.input()).isEqualTo("0xdeadbeef");
  }

  @Test
  void frameTransactionSerializesToJsonWithoutRAndS() {
    final TransactionCompleteResult result = buildCompleteResult();
    final JsonNode json = mapper.valueToTree(result);

    assertThat(json.has("r")).isFalse();
    assertThat(json.has("s")).isFalse();
    assertThat(json.has("frames")).isTrue();
    assertThat(json.get("frames").size()).isEqualTo(2);
    assertThat(json.get("type").asText()).isEqualTo("0x6");
  }

  // --- TransactionReceiptStatusResult tests ---

  @Test
  void frameReceiptIncludesFeePayerAndFrameReceipts() {
    final TransactionReceiptStatusResult result = buildReceiptResult();

    assertThat(result.getFeePayer()).isEqualTo(FEE_PAYER.toString());
    assertThat(result.getFrameReceipts()).isNotNull();
    assertThat(result.getFrameReceipts()).hasSize(2);

    final TransactionReceiptResult.FrameReceiptResult fr0 = result.getFrameReceipts().get(0);
    assertThat(fr0.status()).isEqualTo(Quantity.create(1));
    assertThat(fr0.gasUsed()).isEqualTo(Quantity.create(21_000L));

    final TransactionReceiptResult.FrameReceiptResult fr1 = result.getFrameReceipts().get(1);
    assertThat(fr1.status()).isEqualTo(Quantity.create(1));
    assertThat(fr1.gasUsed()).isEqualTo(Quantity.create(30_000L));
  }

  @Test
  void frameReceiptSerializesToJsonWithFeePayerField() {
    final TransactionReceiptStatusResult result = buildReceiptResult();
    final JsonNode json = mapper.valueToTree(result);

    assertThat(json.get("feePayer").asText()).isEqualTo(FEE_PAYER.toString());
    assertThat(json.get("frameReceipts").size()).isEqualTo(2);
    assertThat(json.get("frameReceipts").get(0).get("status").asText())
        .isEqualTo(Quantity.create(1));
    assertThat(json.get("frameReceipts").get(0).get("gasUsed").asText())
        .isEqualTo(Quantity.create(21_000L));
  }

  @Test
  void nonFrameReceiptOmitsFeePayerAndFrameReceipts() {
    // Build a standard EIP-1559 receipt — feePayer and frameReceipts should be null / absent
    final Transaction tx =
        new org.hyperledger.besu.ethereum.core.BlockDataGenerator()
            .transaction(TransactionType.EIP1559);
    final TransactionReceipt receipt =
        new TransactionReceipt(TransactionType.EIP1559, 1, 21_000L, List.of(), Optional.empty());
    final TransactionReceiptWithMetadata withMeta =
        TransactionReceiptWithMetadata.create(
            receipt, tx, tx.getHash(), 0, 21_000L, Optional.of(Wei.of(7L)),
            Hash.ZERO, 0L, 1L, Optional.empty(), Optional.empty(), 0);
    final TransactionReceiptStatusResult result = new TransactionReceiptStatusResult(withMeta);

    assertThat(result.getFeePayer()).isNull();
    assertThat(result.getFrameReceipts()).isNull();

    final JsonNode json = mapper.valueToTree(result);
    assertThat(json.has("feePayer")).isFalse();
    assertThat(json.has("frameReceipts")).isFalse();
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }

  // --- helpers ---

  private Transaction buildFrameTx() {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(CHAIN_ID)
        .nonce(1L)
        .frameSender(SENDER)
        .frames(
            List.of(
                new Frame(Frame.MODE_VERIFY, Optional.of(TARGET), 100_000L, Bytes.EMPTY),
                new Frame(
                    Frame.MODE_DEFAULT, Optional.empty(), 50_000L,
                    Bytes.fromHexString("0xdeadbeef"))))
        .maxPriorityFeePerGas(Wei.of(500_000_000L))
        .maxFeePerGas(Wei.of(2_000_000_000L))
        .build();
  }

  private TransactionCompleteResult buildCompleteResult() {
    final Transaction tx = buildFrameTx();
    return new TransactionCompleteResult(
        new TransactionWithMetadata(tx, 10L, Optional.of(Wei.of(1_000_000_000L)), Hash.ZERO, 0, 0L));
  }

  private TransactionReceiptStatusResult buildReceiptResult() {
    final Transaction tx = buildFrameTx();
    final TransactionReceipt receipt =
        new TransactionReceipt(
            TransactionType.FRAME,
            1,
            51_000L,
            List.of(),
            Optional.empty(),
            Optional.of(FEE_PAYER),
            Optional.of(
                List.of(
                    new FrameReceipt(1, 21_000L, List.of()),
                    new FrameReceipt(1, 30_000L, List.of()))));
    return new TransactionReceiptStatusResult(
        TransactionReceiptWithMetadata.create(
            receipt, tx, tx.getHash(), 0, 51_000L, Optional.of(Wei.of(1_000_000_000L)),
            Hash.ZERO, 0L, 10L, Optional.empty(), Optional.empty(), 0));
  }
}
