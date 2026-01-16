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

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.evm.log.LogsBloomFilter;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * Encoder for producing canonical RLP encoding of SyncTransactionReceipt for receipts root
 * calculation.
 *
 * <p>This encoder parses the raw RLP bytes inline and re-encodes them in canonical form. For
 * compacted receipts (eth/69), the bloom filter is computed from logs and included in the output.
 */
public class SyncTransactionReceiptEncoder {

  private final SimpleNoCopyRlpEncoder rlpEncoder;

  public SyncTransactionReceiptEncoder(final SimpleNoCopyRlpEncoder rlpEncoder) {
    this.rlpEncoder = rlpEncoder;
  }

  /**
   * Encodes a SyncTransactionReceipt in canonical form for receipts root calculation.
   *
   * <p>The canonical form always includes the bloom filter, even if the original receipt was in
   * compacted format (eth/69).
   *
   * @param receipt the receipt to encode
   * @return the canonical RLP encoding
   */
  public Bytes encodeForRootCalculation(final SyncTransactionReceipt receipt) {
    final Bytes rawRlp = receipt.getRlpBytes();
    final ParsedReceipt parsed = parseReceipt(rawRlp);
    return encodeCanonical(parsed);
  }

  /** Parsed receipt fields needed for canonical encoding. */
  private record ParsedReceipt(
      Bytes transactionTypeCode,
      Bytes statusOrStateRoot,
      Bytes cumulativeGasUsed,
      Bytes bloomFilter,
      List<List<Bytes>> logs) {}

  /**
   * Parses raw RLP bytes to extract receipt fields.
   *
   * @param rawRlp the raw RLP-encoded receipt
   * @return parsed receipt fields
   */
  private ParsedReceipt parseReceipt(final Bytes rawRlp) {
    RLPInput rlpInput = RLP.input(rawRlp);

    // The first byte indicates whether the receipt is typed (eth/68) or flat (eth/69).
    if (!rlpInput.nextIsList()) {
      return parseTypedReceipt(rlpInput);
    } else {
      return parseFlatReceipt(rlpInput);
    }
  }

  private ParsedReceipt parseTypedReceipt(final RLPInput rlpInput) {
    RLPInput input = rlpInput;
    Bytes transactionTypeCode = input.readBytes();
    input = new BytesValueRLPInput(transactionTypeCode.slice(1), false);
    transactionTypeCode = transactionTypeCode.slice(0, 1);

    input.enterList();
    Bytes statusOrStateRoot = input.readBytes();
    Bytes cumulativeGasUsed = input.readBytes();
    final boolean isCompacted = isNextNotBloomFilter(input);
    Bytes bloomFilter = null;
    if (!isCompacted) {
      bloomFilter = input.readBytes();
    }
    List<List<Bytes>> logs = parseLogs(input);
    // if the receipt is compacted, we need to build the bloom filter from the logs
    if (isCompacted) {
      bloomFilter = LogsBloomFilter.builder().insertRawLogs(logs).build();
    }
    input.leaveList();

    return new ParsedReceipt(
        transactionTypeCode, statusOrStateRoot, cumulativeGasUsed, bloomFilter, logs);
  }

  private ParsedReceipt parseFlatReceipt(final RLPInput rlpInput) {
    rlpInput.enterList();
    // Flat receipts can be either legacy or eth/69 receipts.
    // To determine the type, we need to examine the logs' position, as the bloom filter cannot be
    // used. This is because compacted legacy receipts also lack a bloom filter.
    // The first element can be either the transaction type (eth/69) or stateRootOrStatus (eth/68)
    final Bytes firstElement = rlpInput.readBytes();
    // The second element can be either the state root or status (eth/68) or stateRootOrStatus
    // (eth/69)
    final Bytes secondElement = rlpInput.readBytes();
    final boolean isCompacted = isNextNotBloomFilter(rlpInput);
    Bytes bloomFilter = null;
    if (!isCompacted) {
      bloomFilter = rlpInput.readBytes();
    }
    boolean isEth69Receipt = isCompacted && !rlpInput.nextIsList();
    ParsedReceipt result;
    if (isEth69Receipt) {
      result = parseEth69Receipt(rlpInput, firstElement, secondElement);
    } else {
      result = parseLegacyReceipt(rlpInput, firstElement, secondElement, bloomFilter);
    }
    rlpInput.leaveList();
    return result;
  }

  private ParsedReceipt parseEth69Receipt(
      final RLPInput input, final Bytes transactionByteRlp, final Bytes statusOrStateRoot) {
    Bytes transactionTypeCode =
        transactionByteRlp.isEmpty()
            ? Bytes.of(TransactionType.FRONTIER.getEthSerializedType())
            : transactionByteRlp;
    Bytes cumulativeGasUsed = input.readBytes();
    List<List<Bytes>> logs = parseLogs(input);
    Bytes bloomFilter = LogsBloomFilter.builder().insertRawLogs(logs).build();
    return new ParsedReceipt(
        transactionTypeCode, statusOrStateRoot, cumulativeGasUsed, bloomFilter, logs);
  }

  private ParsedReceipt parseLegacyReceipt(
      final RLPInput input,
      final Bytes statusOrStateRoot,
      final Bytes cumulativeGasUsed,
      final Bytes bloomFilter) {
    Bytes transactionTypeCode = Bytes.of(TransactionType.FRONTIER.getEthSerializedType());
    List<List<Bytes>> logs = parseLogs(input);
    return new ParsedReceipt(
        transactionTypeCode,
        statusOrStateRoot,
        cumulativeGasUsed,
        bloomFilter == null ? LogsBloomFilter.builder().insertRawLogs(logs).build() : bloomFilter,
        logs);
  }

  private List<List<Bytes>> parseLogs(final RLPInput input) {
    return input.readList(
        logInput -> {
          logInput.enterList();

          final Bytes logger = logInput.readBytes();

          final List<Bytes> topics = logInput.readList(RLPInput::readBytes32);
          final Bytes data = logInput.readBytes();

          logInput.leaveList();
          List<Bytes> result = new ArrayList<>(topics.size() + 2);
          result.add(logger);
          result.addAll(topics);
          result.add(data);
          return result;
        });
  }

  private boolean isNextNotBloomFilter(final RLPInput input) {
    return input.nextIsList() || input.nextSize() != LogsBloomFilter.BYTE_SIZE;
  }

  /**
   * Encodes parsed receipt fields in canonical form.
   *
   * @param parsed the parsed receipt fields
   * @return canonical RLP encoding
   */
  private Bytes encodeCanonical(final ParsedReceipt parsed) {
    final boolean isFrontier =
        !parsed.transactionTypeCode().isEmpty()
            && parsed.transactionTypeCode().get(0) == TransactionType.FRONTIER.getEthSerializedType();

    List<Bytes> encodedLogs =
        parsed.logs().stream()
            .map(
                (List<Bytes> log) -> {
                  Bytes encodedLogAddress = rlpEncoder.encode(log.getFirst());
                  List<Bytes> encodedLogTopics = new ArrayList<>();
                  for (int i = 1; i < log.size() - 1; i++) {
                    encodedLogTopics.add(rlpEncoder.encode(log.get(i)));
                  }
                  Bytes encodedLogData = rlpEncoder.encode(log.getLast());
                  return rlpEncoder.encodeList(
                      List.of(
                          Bytes.concatenate(
                              encodedLogAddress,
                              rlpEncoder.encodeList(encodedLogTopics),
                              encodedLogData)));
                })
            .toList();
    List<Bytes> mainList =
        List.of(
            rlpEncoder.encode(parsed.statusOrStateRoot()),
            rlpEncoder.encode(parsed.cumulativeGasUsed()),
            rlpEncoder.encode(parsed.bloomFilter()),
            rlpEncoder.encodeList(encodedLogs));

    return !isFrontier
        ? Bytes.concatenate(parsed.transactionTypeCode(), rlpEncoder.encodeList(mainList))
        : rlpEncoder.encodeList(mainList);
  }
}
