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

/**
 * Utility class for estimating RLP-encoded sizes of blockchain data structures.
 *
 * <p>These estimates are used to pre-allocate buffers for {@link PreAllocatedRLPOutput}, avoiding
 * buffer growth during encoding. Estimates should be conservative (slightly over-estimate) to
 * minimize the need for buffer expansion.
 *
 * <p>All estimates include: - RLP list headers and length prefixes - Field encoding overhead - A
 * 20% safety margin to account for variability
 */
public final class RLPSizeEstimator {

  private RLPSizeEstimator() {
    // Utility class
  }

  /**
   * Estimate the RLP-encoded size of a transaction.
   *
   * <p>Transaction sizes vary significantly by type: - Legacy transactions: ~150-200 bytes base +
   * payload - EIP-1559 transactions: ~180-220 bytes base + payload + access list - Blob
   * transactions: ~200-250 bytes base + payload + access list + blob commitments
   *
   * @param payloadSize Size of transaction payload (call data or init code) in bytes
   * @param accessListSize Number of entries in the access list (0 for legacy transactions)
   * @param hasBlobCommitments True if this is an EIP-4844 blob transaction
   * @return Estimated RLP-encoded size in bytes (conservative, may over-estimate by 10-20%)
   */
  public static int estimateTransactionSize(
      final int payloadSize, final int accessListSize, final boolean hasBlobCommitments) {

    // Base transaction fields (common to all types):
    // - List header: 1-3 bytes
    // - Nonce: 1-9 bytes (typically 1-3)
    // - Gas price fields (1-3 depending on type): 1-9 bytes each (typically 5-6 each)
    // - Gas limit: 1-4 bytes (typically 3)
    // - To address: 1 + 20 = 21 bytes (or 1 byte if null)
    // - Value: 1-33 bytes (typically 1-9 for Wei values)
    // - Payload header: 1-5 bytes
    // - Chain ID: 1-2 bytes (typically 1)
    // - Signature (v, r, s): 1 + (1 + 32) + (1 + 32) = ~67 bytes
    int baseSize = 120; // More accurate base estimate

    // Add payload with RLP header overhead
    baseSize += payloadSize + estimateBytesHeaderSize(payloadSize);

    // Access list: each entry is ~80 bytes (address + storage keys)
    // Address: 20 bytes + 1 byte header
    // Storage keys list: typically 1-5 keys of 32 bytes each
    if (accessListSize > 0) {
      baseSize += accessListSize * 80;
    }

    // Blob commitments add significant size for EIP-4844
    if (hasBlobCommitments) {
      // Versioned hashes: typically 1-6 hashes of 32 bytes each
      // KZG commitments: 48 bytes per blob
      // KZG proofs: 48 bytes per blob
      baseSize += 300; // Conservative estimate for blob-related data
    }

    // Add 15% safety margin to avoid buffer growth
    return (int) (baseSize * 1.15);
  }

  /**
   * Estimate just the header size for a byte array.
   *
   * @param dataSize Size of the data
   * @return Header size in bytes
   */
  private static int estimateBytesHeaderSize(final int dataSize) {
    if (dataSize == 0) {
      return 0; // Empty byte string encoded as single 0x80 byte
    }
    if (dataSize == 1) {
      return 0; // Single byte value < 128 is encoded as itself
    }
    if (dataSize < 56) {
      return 1; // Short string: 1 byte prefix
    }
    // Long string: 1 byte prefix + length encoding
    int lengthBytes = 1;
    int remaining = dataSize;
    while (remaining > 255) {
      lengthBytes++;
      remaining >>= 8;
    }
    return 1 + lengthBytes;
  }

  /**
   * Estimate the RLP-encoded size of a transaction (simplified version).
   *
   * <p>For basic transactions without access lists or blob commitments.
   *
   * @param payloadSize Size of transaction payload in bytes
   * @return Estimated RLP-encoded size in bytes
   */
  public static int estimateSimpleTransactionSize(final int payloadSize) {
    return estimateTransactionSize(payloadSize, 0, false);
  }

  /**
   * Estimate the RLP-encoded size of a block header.
   *
   * <p>Block headers have a relatively fixed size structure: - Pre-merge: ~530 bytes (without extra
   * data) - Post-merge: ~580 bytes (with prevRandao, withdrawalsRoot) - Post-Cancun: ~620 bytes
   * (with blobGas fields, parentBeaconBlockRoot)
   *
   * @param extraDataSize Size of the extra data field in bytes (typically 0-32)
   * @param hasWithdrawals True if this is a post-Shanghai block with withdrawals
   * @param hasBlobFields True if this is a post-Cancun block with blob gas fields
   * @return Estimated RLP-encoded size in bytes
   */
  public static int estimateBlockHeaderSize(
      final int extraDataSize, final boolean hasWithdrawals, final boolean hasBlobFields) {

    // Block header fixed fields:
    // - List header: 1-5 bytes
    // - Parent hash: 1 + 32 = 33 bytes
    // - Ommers hash: 1 + 32 = 33 bytes
    // - Coinbase: 1 + 20 = 21 bytes
    // - State root: 1 + 32 = 33 bytes
    // - Transactions root: 1 + 32 = 33 bytes
    // - Receipts root: 1 + 32 = 33 bytes
    // - Logs bloom: 3 + 256 = 259 bytes (always 256 bytes data)
    // - Difficulty: 1-9 bytes
    // - Number: 1-9 bytes
    // - Gas limit: 1-9 bytes
    // - Gas used: 1-9 bytes
    // - Timestamp: 1-9 bytes
    // - Extra data: 1-5 byte header + extraDataSize
    // - Mix hash/prevRandao: 1 + 32 = 33 bytes
    // - Nonce: 1 + 8 = 9 bytes (PoW only, 1 byte for PoS)
    // - Base fee: 1-9 bytes (post-London)

    int baseSize = 540;

    // Add extra data with header
    baseSize += extraDataSize + 5;

    // Post-Shanghai additions
    if (hasWithdrawals) {
      baseSize += 33; // withdrawalsRoot (1 + 32 bytes)
    }

    // Post-Cancun additions
    if (hasBlobFields) {
      baseSize += 10; // blobGasUsed (1-9 bytes)
      baseSize += 10; // excessBlobGas (1-9 bytes)
      baseSize += 33; // parentBeaconBlockRoot (1 + 32 bytes)
    }

    // Add 15% safety margin (headers are more predictable than transactions)
    return (int) (baseSize * 1.15);
  }

  /**
   * Estimate the RLP-encoded size of a block header (simplified version).
   *
   * <p>For modern post-Cancun blocks with typical extra data.
   *
   * @param extraDataSize Size of the extra data field in bytes
   * @return Estimated RLP-encoded size in bytes
   */
  public static int estimateModernBlockHeaderSize(final int extraDataSize) {
    return estimateBlockHeaderSize(extraDataSize, true, true);
  }

  /**
   * Estimate the RLP-encoded size of a transaction receipt.
   *
   * <p>Receipt sizes vary dramatically based on the number and size of logs: - Minimal receipt (no
   * logs): ~280 bytes - Typical receipt (2-5 logs): ~500-1000 bytes - Complex DeFi receipt (10+
   * logs): 2000+ bytes
   *
   * @param logCount Number of logs in the receipt
   * @param averageLogDataSize Average size of log data in bytes (typically 50-200)
   * @param averageTopicsPerLog Average number of topics per log (typically 1-4)
   * @return Estimated RLP-encoded size in bytes
   */
  public static int estimateReceiptSize(
      final int logCount, final int averageLogDataSize, final int averageTopicsPerLog) {

    // Base receipt fields:
    // - List header: 1-5 bytes
    // - Transaction type: 1 byte
    // - Status or state root: 1 or 33 bytes
    // - Cumulative gas used: 1-9 bytes
    // - Logs bloom: 3 + 256 = 259 bytes
    // - Logs list header: 1-5 bytes

    int baseSize = 280; // Base without logs

    if (logCount > 0) {
      // Each log consists of:
      // - List header: 1-5 bytes
      // - Logger address: 1 + 20 = 21 bytes
      // - Topics list: 1-5 byte header + (topics * 33 bytes)
      // - Data: 1-5 byte header + data size

      final int topicsSize = averageTopicsPerLog * 33; // Each topic is 32 bytes + 1 byte prefix
      final int logHeaderSize = 10; // Conservative estimate for list headers

      final int sizePerLog = logHeaderSize + 21 + topicsSize + averageLogDataSize + 5;

      baseSize += logCount * sizePerLog;
    }

    // Add 25% safety margin (receipts with logs are highly variable)
    return (int) (baseSize * 1.25);
  }

  /**
   * Estimate the RLP-encoded size of a transaction receipt (simplified version).
   *
   * <p>For typical receipts with moderate log complexity.
   *
   * @param logCount Number of logs in the receipt
   * @return Estimated RLP-encoded size in bytes
   */
  public static int estimateTypicalReceiptSize(final int logCount) {
    // Typical values based on mainnet data:
    // - Average log data size: 100 bytes
    // - Average topics per log: 3 (indexed parameters)
    return estimateReceiptSize(logCount, 100, 3);
  }

  /**
   * Estimate the RLP-encoded size for a simple scalar value.
   *
   * <p>Used for encoding individual fields during size calculations.
   *
   * @param value The scalar value to encode
   * @return Estimated size in bytes (1-9 bytes depending on value)
   */
  public static int estimateScalarSize(final long value) {
    if (value == 0) {
      return 1; // Empty byte
    }
    if (value < 128) {
      return 1; // Single byte for small values
    }
    // Calculate byte length needed for the value
    int byteLength = 0;
    long remaining = value;
    while (remaining > 0) {
      byteLength++;
      remaining >>= 8;
    }
    return 1 + byteLength; // 1 byte prefix + value bytes
  }

  /**
   * Estimate the RLP-encoded size for a byte array.
   *
   * @param dataSize Size of the byte array
   * @return Estimated size in bytes (header + data)
   */
  public static int estimateBytesSize(final int dataSize) {
    if (dataSize == 0) {
      return 1; // Empty byte string (0x80)
    }
    if (dataSize == 1) {
      return 1; // Single byte (if value < 128, encoded as itself)
    }
    if (dataSize < 56) {
      return 1 + dataSize; // Short string: 1 byte prefix + data
    }
    // Long string: 1 byte prefix + length bytes + data
    // Calculate how many bytes needed to encode the length
    int lengthBytes = 1;
    int remaining = dataSize;
    while (remaining > 255) {
      lengthBytes++;
      remaining >>= 8;
    }
    return 1 + lengthBytes + dataSize;
  }
}
