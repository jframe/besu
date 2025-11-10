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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark comparing batch vs individual RLP encoding performance.
 *
 * <p>Tests different batch sizes to measure:
 * <ul>
 *   <li>Thread-local pool access overhead
 *   <li>Buffer allocation overhead
 *   <li>CPU cache effects
 *   <li>Function call overhead
 * </ul>
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BatchEncodingBench {

  @Param({"1", "10", "50", "100", "200"})
  public int batchSize;

  // Test data structures
  private List<SimpleTransaction> transactions;
  private List<Bytes32> hashes;
  private List<List<Bytes>> nestedData;

  @Setup(Level.Trial)
  public void setUp() {
    final Random random = new Random(42); // Fixed seed for reproducibility

    // Create simple transaction objects
    transactions = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      transactions.add(new SimpleTransaction(
          random.nextLong(1000000),        // nonce
          BigInteger.valueOf(random.nextInt(1000000000)), // gasPrice
          random.nextLong(21000, 1000000), // gasLimit
          randomBytes(20, random),         // to address
          BigInteger.valueOf(random.nextLong(1000000)), // value
          randomBytes(100 + random.nextInt(400), random))); // payload
    }

    // Create hash list
    hashes = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      hashes.add(Bytes32.random());
    }

    // Create nested data
    nestedData = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      List<Bytes> inner = new ArrayList<>();
      for (int j = 0; j < 5; j++) {
        inner.add(randomBytes(32, random));
      }
      nestedData.add(inner);
    }
  }

  private static Bytes randomBytes(final int size, final Random random) {
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return Bytes.wrap(bytes);
  }

  // ========== Simple Transaction Encoding ==========

  @Benchmark
  public Bytes simpleTransaction_batch() {
    return BatchRLPEncoder.encodeBatch(
        transactions,
        (tx, out) -> {
          out.startList();
          out.writeLongScalar(tx.nonce);
          out.writeBigIntegerScalar(tx.gasPrice);
          out.writeLongScalar(tx.gasLimit);
          out.writeBytes(tx.to);
          out.writeBigIntegerScalar(tx.value);
          out.writeBytes(tx.payload);
          out.endList();
        },
        tx -> 200 + tx.payload.size());
  }

  @Benchmark
  public List<Bytes> simpleTransaction_individual() {
    return BatchRLPEncoder.encodeIndividually(
        transactions,
        (tx, out) -> {
          out.startList();
          out.writeLongScalar(tx.nonce);
          out.writeBigIntegerScalar(tx.gasPrice);
          out.writeLongScalar(tx.gasLimit);
          out.writeBytes(tx.to);
          out.writeBigIntegerScalar(tx.value);
          out.writeBytes(tx.payload);
          out.endList();
        },
        tx -> 200 + tx.payload.size());
  }

  @Benchmark
  public Bytes simpleTransaction_individualThenWrap() {
    return BatchRLPEncoder.encodeIndividuallyThenWrap(
        transactions,
        (tx, out) -> {
          out.startList();
          out.writeLongScalar(tx.nonce);
          out.writeBigIntegerScalar(tx.gasPrice);
          out.writeLongScalar(tx.gasLimit);
          out.writeBytes(tx.to);
          out.writeBigIntegerScalar(tx.value);
          out.writeBytes(tx.payload);
          out.endList();
        },
        tx -> 200 + tx.payload.size());
  }

  // ========== Hash List Encoding (simple case) ==========

  @Benchmark
  public Bytes hashList_batch() {
    return BatchRLPEncoder.encodeBatch(
        hashes,
        (hash, out) -> out.writeBytes(hash),
        hash -> 33);
  }

  @Benchmark
  public List<Bytes> hashList_individual() {
    return BatchRLPEncoder.encodeIndividually(
        hashes,
        (hash, out) -> out.writeBytes(hash),
        hash -> 33);
  }

  @Benchmark
  public Bytes hashList_individualThenWrap() {
    return BatchRLPEncoder.encodeIndividuallyThenWrap(
        hashes,
        (hash, out) -> out.writeBytes(hash),
        hash -> 33);
  }

  // ========== Nested Data Encoding (complex case) ==========

  @Benchmark
  public Bytes nestedData_batch() {
    return BatchRLPEncoder.encodeBatch(
        nestedData,
        (list, out) -> out.writeList(list, (item, itemOut) -> itemOut.writeBytes(item)),
        list -> 10 + list.size() * 33);
  }

  @Benchmark
  public List<Bytes> nestedData_individual() {
    return BatchRLPEncoder.encodeIndividually(
        nestedData,
        (list, out) -> out.writeList(list, (item, itemOut) -> itemOut.writeBytes(item)),
        list -> 10 + list.size() * 33);
  }

  @Benchmark
  public Bytes nestedData_individualThenWrap() {
    return BatchRLPEncoder.encodeIndividuallyThenWrap(
        nestedData,
        (list, out) -> out.writeList(list, (item, itemOut) -> itemOut.writeBytes(item)),
        list -> 10 + list.size() * 33);
  }

  // ========== Test Data Structures ==========

  static class SimpleTransaction {
    final long nonce;
    final BigInteger gasPrice;
    final long gasLimit;
    final Bytes to;
    final BigInteger value;
    final Bytes payload;

    SimpleTransaction(
        final long nonce,
        final BigInteger gasPrice,
        final long gasLimit,
        final Bytes to,
        final BigInteger value,
        final Bytes payload) {
      this.nonce = nonce;
      this.gasPrice = gasPrice;
      this.gasLimit = gasLimit;
      this.to = to;
      this.value = value;
      this.payload = payload;
    }
  }
}
