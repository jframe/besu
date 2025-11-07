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

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark comparing current RLP implementation vs optimized PreAllocatedRLPOutput.
 *
 * <p>Tests realistic blockchain encoding scenarios: - Simple transaction (baseline) - Complex
 * transaction with large payload and access list - Block header with all post-merge fields
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(
    value = 1,
    jvmArgs = {"-Xms1G", "-Xmx1G"})
public class RLPOptimizationBench {

  // Test data
  private Bytes32 hash;
  private BigInteger chainId;
  private UInt256 maxFee;
  private UInt256 value;
  private Bytes payload;
  private Bytes32 signature;

  @Setup(Level.Trial)
  public void setup() {
    final byte[] hashBytes = new byte[32];
    for (int i = 0; i < 32; i++) {
      hashBytes[i] = (byte) i;
    }
    hash = Bytes32.wrap(hashBytes);

    chainId = BigInteger.ONE;
    maxFee = UInt256.valueOf(50_000_000_000L);
    value = UInt256.valueOf(1_000_000_000_000_000_000L);

    // Realistic transaction payload (500 bytes - typical contract call)
    final byte[] payloadBytes = new byte[500];
    for (int i = 0; i < 500; i++) {
      payloadBytes[i] = (byte) (i % 256);
    }
    payload = Bytes.wrap(payloadBytes);

    signature = hash; // Reuse for simplicity
  }

  // ================== Simple Transaction Encoding ==================

  @Benchmark
  public Bytes simpleTransactionCurrent() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    encodeSimpleTransaction(out);
    return out.encoded();
  }

  @Benchmark
  public Bytes simpleTransactionOptimized() {
    final PreAllocatedRLPOutput out = new PreAllocatedRLPOutput(300);
    encodeSimpleTransaction(out);
    return out.encoded();
  }

  @Benchmark
  public Bytes simpleTransactionOptimizedPooled() {
    final PreAllocatedRLPOutput out = PreAllocatedRLPOutput.get();
    try {
      out.reset(300);
      encodeSimpleTransaction(out);
      return out.encoded();
    } finally {
      out.returnToPool();
    }
  }

  private void encodeSimpleTransaction(final RLPOutput out) {
    out.startList();
    out.writeBigIntegerScalar(chainId);
    out.writeLongScalar(42L); // nonce
    out.writeUInt256Scalar(maxFee);
    out.writeUInt256Scalar(maxFee);
    out.writeLongScalar(21000L); // gas limit
    out.writeBytes(hash.slice(0, 20)); // to address
    out.writeUInt256Scalar(value);
    out.writeBytes(Bytes.EMPTY); // data
    out.writeIntScalar(0); // v
    out.writeBytes(signature);
    out.writeBytes(signature);
    out.endList();
  }

  // ================== Complex Transaction with Payload ==================

  @Benchmark
  public Bytes complexTransactionCurrent() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    encodeComplexTransaction(out);
    return out.encoded();
  }

  @Benchmark
  public Bytes complexTransactionOptimized() {
    final PreAllocatedRLPOutput out = new PreAllocatedRLPOutput(700);
    encodeComplexTransaction(out);
    return out.encoded();
  }

  @Benchmark
  public Bytes complexTransactionOptimizedPooled() {
    final PreAllocatedRLPOutput out = PreAllocatedRLPOutput.get();
    try {
      out.reset(700);
      encodeComplexTransaction(out);
      return out.encoded();
    } finally {
      out.returnToPool();
    }
  }

  private void encodeComplexTransaction(final RLPOutput out) {
    out.startList();
    out.writeBigIntegerScalar(chainId);
    out.writeLongScalar(42L);
    out.writeUInt256Scalar(maxFee);
    out.writeUInt256Scalar(maxFee);
    out.writeLongScalar(300000L);
    out.writeBytes(hash.slice(0, 20));
    out.writeUInt256Scalar(value);
    out.writeBytes(payload); // Large payload
    out.startList(); // Empty access list
    out.endList();
    out.writeIntScalar(0);
    out.writeBytes(signature);
    out.writeBytes(signature);
    out.endList();
  }

  // ================== Block Header Encoding ==================

  @Benchmark
  public Bytes blockHeaderCurrent() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    encodeBlockHeader(out);
    return out.encoded();
  }

  @Benchmark
  public Bytes blockHeaderOptimized() {
    final PreAllocatedRLPOutput out = new PreAllocatedRLPOutput(600);
    encodeBlockHeader(out);
    return out.encoded();
  }

  @Benchmark
  public Bytes blockHeaderOptimizedPooled() {
    final PreAllocatedRLPOutput out = PreAllocatedRLPOutput.get();
    try {
      out.reset(600);
      encodeBlockHeader(out);
      return out.encoded();
    } finally {
      out.returnToPool();
    }
  }

  private void encodeBlockHeader(final RLPOutput out) {
    out.startList();
    out.writeBytes(hash); // parentHash
    out.writeBytes(hash); // ommersHash
    out.writeBytes(hash.slice(0, 20)); // coinbase
    out.writeBytes(hash); // stateRoot
    out.writeBytes(hash); // transactionsRoot
    out.writeBytes(hash); // receiptsRoot
    out.writeBytes(Bytes.repeat((byte) 0, 256)); // logsBloom
    out.writeUInt256Scalar(UInt256.ZERO); // difficulty
    out.writeLongScalar(17000000L); // number
    out.writeLongScalar(30000000L); // gasLimit
    out.writeLongScalar(15000000L); // gasUsed
    out.writeLongScalar(1700000000L); // timestamp
    out.writeBytes(Bytes.of(0x42, 0x65, 0x73, 0x75)); // extraData
    out.writeBytes(hash); // mixHash
    out.writeLongScalar(0L); // nonce
    out.writeUInt256Scalar(UInt256.valueOf(20_000_000_000L)); // baseFeePerGas
    out.writeBytes(hash); // withdrawalsRoot
    out.writeUInt256Scalar(UInt256.valueOf(131072L)); // blobGasUsed
    out.writeUInt256Scalar(UInt256.valueOf(262144L)); // excessBlobGas
    out.writeBytes(hash); // parentBeaconBlockRoot
    out.endList();
  }

  // ================== Nested List Structure (stress test) ==================

  @Benchmark
  public Bytes nestedListsCurrent() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    encodeNestedLists(out);
    return out.encoded();
  }

  @Benchmark
  public Bytes nestedListsOptimized() {
    final PreAllocatedRLPOutput out = new PreAllocatedRLPOutput(1000);
    encodeNestedLists(out);
    return out.encoded();
  }

  @Benchmark
  public Bytes nestedListsOptimizedPooled() {
    final PreAllocatedRLPOutput out = PreAllocatedRLPOutput.get();
    try {
      out.reset(1000);
      encodeNestedLists(out);
      return out.encoded();
    } finally {
      out.returnToPool();
    }
  }

  private void encodeNestedLists(final RLPOutput out) {
    out.startList();
    for (int i = 0; i < 10; i++) {
      out.startList();
      out.writeBytes(hash);
      out.writeBytes(hash);
      out.writeBytes(hash);
      out.endList();
    }
    out.endList();
  }
}
