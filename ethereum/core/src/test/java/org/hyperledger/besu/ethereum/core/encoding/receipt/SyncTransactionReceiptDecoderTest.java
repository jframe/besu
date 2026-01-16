/*
 * Copyright contributors to Besu.
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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.evm.log.Log;
import org.hyperledger.besu.evm.log.LogTopic;
import org.hyperledger.besu.evm.log.LogsBloomFilter;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the lazy SyncTransactionReceipt design.
 *
 * <p>These tests verify that:
 *
 * <ul>
 *   <li>The decoder stores raw bytes without parsing
 *   <li>The encoder produces canonical output for receipts root calculation
 *   <li>The canonical output matches what TransactionReceipt produces
 * </ul>
 */
public class SyncTransactionReceiptDecoderTest {

  private SyncTransactionReceiptDecoder syncTransactionReceiptDecoder;
  private SyncTransactionReceiptEncoder syncTransactionReceiptEncoder;

  @BeforeEach
  public void beforeTest() {
    syncTransactionReceiptDecoder = new SyncTransactionReceiptDecoder();
    syncTransactionReceiptEncoder = new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder());
  }

  @Test
  public void testDecodeLegacyReceipt() {
    final Hash stateRoot = Hash.hash(Bytes.random(32));
    final long cumulativeGasUsed = 2;
    final List<Log> logs =
        List.of(
            new Log(
                Address.fromHexString("03"),
                Bytes.fromHexStringLenient("04"),
                List.of(LogTopic.fromHexString("05"))));
    final LogsBloomFilter bloomFilter = LogsBloomFilter.fromHexString("0x" + "deadbeef".repeat(64));
    final Optional<Bytes> revertReason = Optional.of(Bytes.fromHexString("06"));
    TransactionReceipt transactionReceipt =
        new TransactionReceipt(
            TransactionType.FRONTIER,
            stateRoot,
            cumulativeGasUsed,
            logs,
            bloomFilter,
            revertReason);

    Bytes encodedReceipt =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt, rlpOut, TransactionReceiptEncodingConfiguration.DEFAULT));

    SyncTransactionReceipt syncTransactionReceipt =
        syncTransactionReceiptDecoder.decode(encodedReceipt);

    // Verify raw bytes are stored
    Assertions.assertEquals(encodedReceipt, syncTransactionReceipt.getRlpBytes());

    // Verify canonical encoding matches expected output for receipts root calculation
    Bytes canonicalEncoding =
        syncTransactionReceiptEncoder.encodeForRootCalculation(syncTransactionReceipt);
    Bytes expectedCanonical =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt,
                    rlpOut,
                    TransactionReceiptEncodingConfiguration.TRIE_ROOT));
    Assertions.assertEquals(expectedCanonical, canonicalEncoding);
  }

  @Test
  public void testDecodeEth69Receipt() {
    final Hash stateRoot = Hash.hash(Bytes.random(32));
    final long cumulativeGasUsed = 2;
    final List<Log> logs =
        List.of(
            new Log(
                Address.fromHexString("03"),
                Bytes.fromHexStringLenient("04"),
                List.of(LogTopic.fromHexString("05"))));
    TransactionReceipt transactionReceipt =
        new TransactionReceipt(stateRoot, cumulativeGasUsed, logs, Optional.empty());

    // Encode in eth/69 compacted format (no bloom filter)
    Bytes encodedReceipt =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt,
                    rlpOut,
                    TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION));

    SyncTransactionReceipt syncTransactionReceipt =
        syncTransactionReceiptDecoder.decode(encodedReceipt);

    // Verify raw bytes are stored
    Assertions.assertEquals(encodedReceipt, syncTransactionReceipt.getRlpBytes());

    // Verify canonical encoding includes computed bloom filter
    Bytes canonicalEncoding =
        syncTransactionReceiptEncoder.encodeForRootCalculation(syncTransactionReceipt);
    // Expected canonical form is standard eth/68 format with bloom filter
    LogsBloomFilter computedBloom = LogsBloomFilter.builder().insertLogs(logs).build();
    TransactionReceipt canonicalReceipt =
        new TransactionReceipt(
            TransactionType.FRONTIER,
            stateRoot,
            cumulativeGasUsed,
            logs,
            computedBloom,
            Optional.empty());
    Bytes expectedCanonical =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    canonicalReceipt,
                    rlpOut,
                    TransactionReceiptEncodingConfiguration.TRIE_ROOT));
    Assertions.assertEquals(expectedCanonical, canonicalEncoding);
  }

  @Test
  public void testDecodeTypedReceipt() {
    final TransactionType transactionType = TransactionType.EIP1559;
    final Hash stateRoot = Hash.hash(Bytes.random(32));
    final long cumulativeGasUsed = 2;
    final List<Log> logs =
        List.of(
            new Log(
                Address.fromHexString("03"),
                Bytes.fromHexStringLenient("04"),
                List.of(LogTopic.fromHexString("05"))));
    final LogsBloomFilter bloomFilter = LogsBloomFilter.fromHexString("0x" + "deadbeef".repeat(64));
    final Optional<Bytes> revertReason = Optional.of(Bytes.fromHexString("06"));
    TransactionReceipt transactionReceipt =
        new TransactionReceipt(
            transactionType, stateRoot, cumulativeGasUsed, logs, bloomFilter, revertReason);

    Bytes encodedReceipt =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt, rlpOut, TransactionReceiptEncodingConfiguration.DEFAULT));

    SyncTransactionReceipt syncTransactionReceipt =
        syncTransactionReceiptDecoder.decode(encodedReceipt);

    // Verify raw bytes are stored
    Assertions.assertEquals(encodedReceipt, syncTransactionReceipt.getRlpBytes());

    // Verify canonical encoding matches expected output
    Bytes canonicalEncoding =
        syncTransactionReceiptEncoder.encodeForRootCalculation(syncTransactionReceipt);
    Bytes expectedCanonical =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt,
                    rlpOut,
                    TransactionReceiptEncodingConfiguration.TRIE_ROOT));
    Assertions.assertEquals(expectedCanonical, canonicalEncoding);
  }

  @Test
  public void testDecodeTypedCompactedReceipt() {
    final TransactionType transactionType = TransactionType.EIP1559;
    final Hash stateRoot = Hash.hash(Bytes.random(32));
    final long cumulativeGasUsed = 2;
    final List<Log> logs =
        List.of(
            new Log(
                Address.fromHexString("03"),
                Bytes.fromHexStringLenient("04"),
                List.of(LogTopic.fromHexString("05"))));
    final LogsBloomFilter computedBloom = LogsBloomFilter.builder().insertLogs(logs).build();
    TransactionReceipt transactionReceipt =
        new TransactionReceipt(
            transactionType, stateRoot, cumulativeGasUsed, logs, computedBloom, Optional.empty());

    // Create compacted typed receipt configuration (no bloom filter)
    TransactionReceiptEncodingConfiguration compactedTypedConfig =
        new TransactionReceiptEncodingConfiguration.Builder()
            .withBloomFilter(false)
            .withRevertReason(false)
            .build();

    // Encode in compacted format (no bloom filter)
    Bytes encodedReceipt =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(transactionReceipt, rlpOut, compactedTypedConfig));

    SyncTransactionReceipt syncTransactionReceipt =
        syncTransactionReceiptDecoder.decode(encodedReceipt);

    // Verify raw bytes are stored
    Assertions.assertEquals(encodedReceipt, syncTransactionReceipt.getRlpBytes());

    // Verify canonical encoding includes computed bloom filter
    Bytes canonicalEncoding =
        syncTransactionReceiptEncoder.encodeForRootCalculation(syncTransactionReceipt);
    Bytes expectedCanonical =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt,
                    rlpOut,
                    TransactionReceiptEncodingConfiguration.TRIE_ROOT));
    Assertions.assertEquals(expectedCanonical, canonicalEncoding);
  }
}
