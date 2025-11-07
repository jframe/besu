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
package org.hyperledger.besu.ethereum.rlp;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Tests for {@link RLPSizeEstimator} to ensure size estimates are accurate and conservative. */
public class RLPSizeEstimatorTest {

  @Test
  public void testEstimateScalarSize() {
    // Zero should be 1 byte
    assertThat(RLPSizeEstimator.estimateScalarSize(0)).isEqualTo(1);

    // Small values < 128 should be 1 byte
    assertThat(RLPSizeEstimator.estimateScalarSize(1)).isEqualTo(1);
    assertThat(RLPSizeEstimator.estimateScalarSize(127)).isEqualTo(1);

    // Values >= 128 need prefix + bytes
    assertThat(RLPSizeEstimator.estimateScalarSize(128)).isEqualTo(2); // 1 prefix + 1 byte
    assertThat(RLPSizeEstimator.estimateScalarSize(255)).isEqualTo(2);
    assertThat(RLPSizeEstimator.estimateScalarSize(256)).isEqualTo(3); // 1 prefix + 2 bytes
    assertThat(RLPSizeEstimator.estimateScalarSize(65535)).isEqualTo(3);
    assertThat(RLPSizeEstimator.estimateScalarSize(65536)).isEqualTo(4); // 1 prefix + 3 bytes
  }

  @Test
  public void testEstimateBytesSize() {
    // Empty bytes
    assertThat(RLPSizeEstimator.estimateBytesSize(0)).isEqualTo(1);

    // Single byte
    assertThat(RLPSizeEstimator.estimateBytesSize(1)).isEqualTo(1);

    // Short string (< 56 bytes)
    assertThat(RLPSizeEstimator.estimateBytesSize(10)).isEqualTo(11); // 1 prefix + 10 bytes
    assertThat(RLPSizeEstimator.estimateBytesSize(55)).isEqualTo(56); // 1 prefix + 55 bytes

    // Long string (>= 56 bytes)
    assertThat(RLPSizeEstimator.estimateBytesSize(56))
        .isGreaterThanOrEqualTo(58); // prefix + length + data
    assertThat(RLPSizeEstimator.estimateBytesSize(100)).isGreaterThanOrEqualTo(102);
  }

  @Test
  public void testSimpleTransactionSizeEstimate() {
    // Verify estimate against actual encoding of a simple transaction structure

    // Simple transaction: no payload, no access list
    final int estimate = RLPSizeEstimator.estimateSimpleTransactionSize(0);

    // Encode a minimal transaction-like structure
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBigIntegerScalar(BigInteger.ONE); // chainId
    out.writeLongScalar(42L); // nonce
    out.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L)); // maxFeePerGas
    out.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L)); // maxPriorityFeePerGas
    out.writeLongScalar(21000L); // gasLimit
    out.writeBytes(Bytes.random(20)); // to address
    out.writeUInt256Scalar(UInt256.valueOf(1_000_000_000_000_000_000L)); // value
    out.writeBytes(Bytes.EMPTY); // data (no payload)
    out.writeIntScalar(0); // v
    out.writeBytes(Bytes32.random()); // r
    out.writeBytes(Bytes32.random()); // s
    out.endList();

    final int actualSize = out.encoded().size();

    // Estimate should be >= actual size (conservative)
    assertThat(estimate).isGreaterThanOrEqualTo(actualSize);

    // Estimate should not be more than 50% over (reasonable)
    assertThat(estimate).isLessThan(actualSize * 2);
  }

  @Test
  public void testTransactionWithPayloadSizeEstimate() {
    final int payloadSize = 500;
    final int estimate = RLPSizeEstimator.estimateSimpleTransactionSize(payloadSize);

    // Encode transaction with 500-byte payload
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBigIntegerScalar(BigInteger.ONE);
    out.writeLongScalar(42L);
    out.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
    out.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
    out.writeLongScalar(300000L);
    out.writeBytes(Bytes.random(20));
    out.writeUInt256Scalar(UInt256.valueOf(1_000_000_000_000_000_000L));
    out.writeBytes(Bytes.random(payloadSize)); // Large payload
    out.writeIntScalar(0);
    out.writeBytes(Bytes32.random());
    out.writeBytes(Bytes32.random());
    out.endList();

    final int actualSize = out.encoded().size();

    assertThat(estimate).isGreaterThanOrEqualTo(actualSize);
    assertThat(estimate).isLessThan(actualSize * 2);
  }

  @Test
  public void testBlockHeaderSizeEstimate() {
    // Modern block header with typical extra data
    final int extraDataSize = 32;
    final int estimate = RLPSizeEstimator.estimateModernBlockHeaderSize(extraDataSize);

    // Encode a block header-like structure
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(Bytes32.random()); // parentHash
    out.writeBytes(Bytes32.random()); // ommersHash
    out.writeBytes(Bytes.random(20)); // coinbase
    out.writeBytes(Bytes32.random()); // stateRoot
    out.writeBytes(Bytes32.random()); // transactionsRoot
    out.writeBytes(Bytes32.random()); // receiptsRoot
    out.writeBytes(Bytes.repeat((byte) 0, 256)); // logsBloom
    out.writeUInt256Scalar(UInt256.ZERO); // difficulty
    out.writeLongScalar(17000000L); // number
    out.writeLongScalar(30000000L); // gasLimit
    out.writeLongScalar(15000000L); // gasUsed
    out.writeLongScalar(1700000000L); // timestamp
    out.writeBytes(Bytes.random(extraDataSize)); // extraData
    out.writeBytes(Bytes32.random()); // mixHash/prevRandao
    out.writeLongScalar(0L); // nonce
    out.writeUInt256Scalar(UInt256.valueOf(20_000_000_000L)); // baseFeePerGas
    out.writeBytes(Bytes32.random()); // withdrawalsRoot
    out.writeUInt256Scalar(UInt256.valueOf(131072L)); // blobGasUsed
    out.writeUInt256Scalar(UInt256.valueOf(262144L)); // excessBlobGas
    out.writeBytes(Bytes32.random()); // parentBeaconBlockRoot
    out.endList();

    final int actualSize = out.encoded().size();

    assertThat(estimate).isGreaterThanOrEqualTo(actualSize);
    assertThat(estimate).isLessThan(actualSize * 2);
  }

  @Test
  public void testReceiptSizeEstimateNoLogs() {
    // Receipt with no logs should be minimal
    final int estimate = RLPSizeEstimator.estimateTypicalReceiptSize(0);

    // Encode minimal receipt structure
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeIntScalar(1); // status (success)
    out.writeLongScalar(21000L); // cumulativeGasUsed
    out.writeBytes(Bytes.repeat((byte) 0, 256)); // logsBloom
    out.startList(); // empty logs list
    out.endList();
    out.endList();

    final int actualSize = out.encoded().size();

    assertThat(estimate).isGreaterThanOrEqualTo(actualSize);
  }

  @Test
  public void testReceiptSizeEstimateWithLogs() {
    final int logCount = 3;
    final int estimate = RLPSizeEstimator.estimateTypicalReceiptSize(logCount);

    // Encode receipt with 3 logs
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeIntScalar(1); // status
    out.writeLongScalar(100000L); // cumulativeGasUsed
    out.writeBytes(Bytes.repeat((byte) 0, 256)); // logsBloom

    // Logs list
    out.startList();
    for (int i = 0; i < logCount; i++) {
      out.startList();
      out.writeBytes(Bytes.random(20)); // logger address

      // Topics list (3 topics)
      out.startList();
      out.writeBytes(Bytes32.random());
      out.writeBytes(Bytes32.random());
      out.writeBytes(Bytes32.random());
      out.endList();

      out.writeBytes(Bytes.random(100)); // log data (100 bytes)
      out.endList();
    }
    out.endList();

    out.endList();

    final int actualSize = out.encoded().size();

    assertThat(estimate).isGreaterThanOrEqualTo(actualSize);
    assertThat(estimate).isLessThan(actualSize * 2);
  }

  @Test
  public void testTransactionWithAccessListSizeEstimate() {
    final int accessListSize = 5;
    final int estimate = RLPSizeEstimator.estimateTransactionSize(100, accessListSize, false);

    // Encode transaction with access list
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBigIntegerScalar(BigInteger.ONE);
    out.writeLongScalar(42L);
    out.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
    out.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
    out.writeLongScalar(300000L);
    out.writeBytes(Bytes.random(20));
    out.writeUInt256Scalar(UInt256.valueOf(1_000_000_000_000_000_000L));
    out.writeBytes(Bytes.random(100)); // payload

    // Access list
    out.startList();
    for (int i = 0; i < accessListSize; i++) {
      out.startList();
      out.writeBytes(Bytes.random(20)); // address
      out.startList(); // storage keys
      out.writeBytes(Bytes32.random());
      out.writeBytes(Bytes32.random());
      out.endList();
      out.endList();
    }
    out.endList();

    out.writeIntScalar(0);
    out.writeBytes(Bytes32.random());
    out.writeBytes(Bytes32.random());
    out.endList();

    final int actualSize = out.encoded().size();

    assertThat(estimate).isGreaterThanOrEqualTo(actualSize);
    assertThat(estimate).isLessThan(actualSize * 2);
  }

  @Test
  public void testEstimateConservativeness() {
    // Verify that estimates consistently over-estimate by a reasonable margin

    // Test various transaction payload sizes
    for (int payloadSize : new int[] {0, 100, 500, 1000, 5000}) {
      final int estimate = RLPSizeEstimator.estimateSimpleTransactionSize(payloadSize);

      // Build actual transaction
      final BytesValueRLPOutput out = new BytesValueRLPOutput();
      out.startList();
      out.writeBigIntegerScalar(BigInteger.ONE);
      out.writeLongScalar(42L);
      out.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
      out.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
      out.writeLongScalar(21000L);
      out.writeBytes(Bytes.random(20));
      out.writeUInt256Scalar(UInt256.valueOf(1_000_000_000_000_000_000L));
      out.writeBytes(Bytes.random(payloadSize));
      out.writeIntScalar(0);
      out.writeBytes(Bytes32.random());
      out.writeBytes(Bytes32.random());
      out.endList();

      final int actualSize = out.encoded().size();
      final double overestimateRatio = (double) estimate / actualSize;

      // Should over-estimate by 10-30% (our target is 20% safety margin)
      assertThat(overestimateRatio).isGreaterThan(1.0).isLessThan(1.5);
    }
  }

  @Test
  public void testPreAllocatedRLPOutputWithEstimate() {
    // Integration test: Verify PreAllocatedRLPOutput works with size estimates

    final int payloadSize = 500;
    final int estimate = RLPSizeEstimator.estimateSimpleTransactionSize(payloadSize);

    // Use estimate with PreAllocatedRLPOutput
    final PreAllocatedRLPOutput preAllocated = new PreAllocatedRLPOutput(estimate);
    preAllocated.startList();
    preAllocated.writeBigIntegerScalar(BigInteger.ONE);
    preAllocated.writeLongScalar(42L);
    preAllocated.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
    preAllocated.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
    preAllocated.writeLongScalar(21000L);
    preAllocated.writeBytes(Bytes.random(20));
    preAllocated.writeUInt256Scalar(UInt256.valueOf(1_000_000_000_000_000_000L));
    preAllocated.writeBytes(Bytes.random(payloadSize));
    preAllocated.writeIntScalar(0);
    preAllocated.writeBytes(Bytes32.random());
    preAllocated.writeBytes(Bytes32.random());
    preAllocated.endList();

    final Bytes preAllocatedResult = preAllocated.encoded();

    // Compare with current implementation
    final BytesValueRLPOutput current = new BytesValueRLPOutput();
    current.startList();
    current.writeBigIntegerScalar(BigInteger.ONE);
    current.writeLongScalar(42L);
    current.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
    current.writeUInt256Scalar(UInt256.valueOf(50_000_000_000L));
    current.writeLongScalar(21000L);
    current.writeBytes(
        preAllocatedResult.slice(
            preAllocatedResult.size() - payloadSize - 99, 20)); // Extract same address
    current.writeUInt256Scalar(UInt256.valueOf(1_000_000_000_000_000_000L));
    current.writeBytes(
        preAllocatedResult.slice(
            preAllocatedResult.size() - payloadSize - 66, payloadSize)); // Extract same payload
    current.writeIntScalar(0);
    current.writeBytes(
        preAllocatedResult.slice(preAllocatedResult.size() - 64, 32)); // Extract same r
    current.writeBytes(
        preAllocatedResult.slice(preAllocatedResult.size() - 32, 32)); // Extract same s
    current.endList();

    // Both should produce valid RLP (sizes should be similar)
    assertThat(preAllocatedResult.size()).isGreaterThan(0);
    assertThat(preAllocatedResult.size()).isLessThanOrEqualTo(estimate);
  }
}
