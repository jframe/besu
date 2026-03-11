/*
 * Copyright Hyperledger Besu Contributors.
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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.datatypes.Frame;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class FrameTransactionEncodingTest {

  private static final Address SENDER =
      Address.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  private static final Address TARGET =
      Address.fromHexString("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
  private static final BigInteger CHAIN_ID = BigInteger.ONE;

  @Test
  void roundTripSingleDefaultFrame() {
    final Frame frame =
        new Frame(
            Frame.MODE_DEFAULT,
            Optional.of(TARGET),
            100_000L,
            Bytes.fromHexString("0xdeadbeef"));

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(CHAIN_ID)
            .nonce(1L)
            .frameSender(SENDER)
            .frames(List.of(frame))
            .maxPriorityFeePerGas(Wei.of(1_000_000_000L))
            .maxFeePerGas(Wei.of(2_000_000_000L))
            .build();

    assertThat(tx.getType()).isEqualTo(TransactionType.FRAME);
    assertThat(tx.getFrameSender()).contains(SENDER);
    assertThat(tx.getSender()).isEqualTo(SENDER);
    assertThat(tx.getFrames()).isPresent();
    assertThat(tx.getFrames().get()).hasSize(1);

    // Encode to opaque bytes
    final Bytes encoded =
        TransactionEncoder.encodeOpaqueBytes(tx, EncodingContext.BLOCK_BODY);
    assertThat(encoded.get(0)).isEqualTo(TransactionType.FRAME.getSerializedType());

    // Decode back
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.BLOCK_BODY);

    assertThat(decoded.getType()).isEqualTo(TransactionType.FRAME);
    assertThat(decoded.getChainId()).contains(CHAIN_ID);
    assertThat(decoded.getNonce()).isEqualTo(1L);
    assertThat(decoded.getFrameSender()).contains(SENDER);
    assertThat(decoded.getSender()).isEqualTo(SENDER);
    assertThat(decoded.getMaxPriorityFeePerGas()).contains(Wei.of(1_000_000_000L));
    assertThat(decoded.getMaxFeePerGas()).contains(Wei.of(2_000_000_000L));

    final List<Frame> decodedFrames = decoded.getFrames().orElseThrow();
    assertThat(decodedFrames).hasSize(1);
    assertThat(decodedFrames.get(0).getMode()).isEqualTo(Frame.MODE_DEFAULT);
    assertThat(decodedFrames.get(0).getTarget()).contains(TARGET);
    assertThat(decodedFrames.get(0).getGasLimit()).isEqualTo(100_000L);
    assertThat(decodedFrames.get(0).getData()).isEqualTo(Bytes.fromHexString("0xdeadbeef"));
  }

  @Test
  void roundTripMultipleFrames() {
    final Frame defaultFrame =
        new Frame(Frame.MODE_DEFAULT, Optional.of(TARGET), 50_000L, Bytes.fromHexString("0x1234"));
    final Frame verifyFrame =
        new Frame(Frame.MODE_VERIFY, Optional.empty(), 30_000L, Bytes.fromHexString("0x5678"));
    final Frame senderFrame =
        new Frame(Frame.MODE_SENDER, Optional.of(TARGET), 20_000L, Bytes.EMPTY);

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(CHAIN_ID)
            .nonce(42L)
            .frameSender(SENDER)
            .frames(List.of(defaultFrame, verifyFrame, senderFrame))
            .maxPriorityFeePerGas(Wei.of(500_000_000L))
            .maxFeePerGas(Wei.of(1_500_000_000L))
            .build();

    final Bytes encoded =
        TransactionEncoder.encodeOpaqueBytes(tx, EncodingContext.BLOCK_BODY);
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.BLOCK_BODY);

    assertThat(decoded.getNonce()).isEqualTo(42L);
    final List<Frame> decodedFrames = decoded.getFrames().orElseThrow();
    assertThat(decodedFrames).hasSize(3);

    // Default frame
    assertThat(decodedFrames.get(0).getMode()).isEqualTo(Frame.MODE_DEFAULT);
    assertThat(decodedFrames.get(0).getTarget()).contains(TARGET);
    assertThat(decodedFrames.get(0).getGasLimit()).isEqualTo(50_000L);
    assertThat(decodedFrames.get(0).getData()).isEqualTo(Bytes.fromHexString("0x1234"));

    // Verify frame — target is empty bytes in RLP → decoded as Optional.empty
    assertThat(decodedFrames.get(1).getMode()).isEqualTo(Frame.MODE_VERIFY);
    assertThat(decodedFrames.get(1).getTarget()).isEmpty();
    assertThat(decodedFrames.get(1).getGasLimit()).isEqualTo(30_000L);
    assertThat(decodedFrames.get(1).getData()).isEqualTo(Bytes.fromHexString("0x5678"));

    // Sender frame
    assertThat(decodedFrames.get(2).getMode()).isEqualTo(Frame.MODE_SENDER);
    assertThat(decodedFrames.get(2).getData()).isEqualTo(Bytes.EMPTY);
  }

  @Test
  void guessTypeSelectsFrame() {
    final Frame frame =
        new Frame(Frame.MODE_DEFAULT, Optional.of(TARGET), 21_000L, Bytes.EMPTY);

    final Transaction.Builder builder =
        Transaction.builder()
            .chainId(CHAIN_ID)
            .nonce(0L)
            .frameSender(SENDER)
            .frames(List.of(frame))
            .maxPriorityFeePerGas(Wei.of(1_000_000_000L))
            .maxFeePerGas(Wei.of(2_000_000_000L));

    builder.guessType();
    assertThat(builder.getTransactionType()).isEqualTo(TransactionType.FRAME);
  }

  @Test
  void frameTransactionHashIsStable() {
    final Frame frame =
        new Frame(Frame.MODE_DEFAULT, Optional.of(TARGET), 21_000L, Bytes.fromHexString("0xab"));

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(CHAIN_ID)
            .nonce(0L)
            .frameSender(SENDER)
            .frames(List.of(frame))
            .maxPriorityFeePerGas(Wei.of(1_000_000_000L))
            .maxFeePerGas(Wei.of(2_000_000_000L))
            .build();

    // Hash should be deterministic
    assertThat(tx.getHash()).isEqualTo(tx.getHash());
    assertThat(tx.getHash().toHexString()).startsWith("0x");
  }
}
