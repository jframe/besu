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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Objects;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/**
 * A minimally decoded transaction receipt used during sync to reduce memory usage.
 *
 * <p>This class stores only the raw RLP-encoded bytes of a transaction receipt without fully
 * decoding the receipt structure. This is sufficient for verifying the receipts root during sync,
 * as the verification only requires computing the merkle trie root from the encoded bytes.
 *
 * <p>The full {@link TransactionReceipt} can be lazily decoded when needed via {@link
 * #getReceiptSupplier()}.
 *
 * <p>This approach significantly reduces memory usage during sync, similar to the optimization done
 * for block bodies in {@link SyncBlockBody}.
 */
public class SyncTransactionReceipt {

  private final Bytes encodedReceipt;
  private final Supplier<TransactionReceipt> receiptSupplier;

  /**
   * Creates a SyncTransactionReceipt from the raw RLP-encoded bytes.
   *
   * @param encodedReceipt the RLP-encoded transaction receipt bytes, including any type prefix for
   *     typed transactions
   */
  public SyncTransactionReceipt(final Bytes encodedReceipt) {
    this.encodedReceipt = encodedReceipt;
    this.receiptSupplier = null; // Will be lazily created in getReceiptSupplier()
  }

  /**
   * Private constructor for creating a SyncTransactionReceipt with a pre-decoded receipt. Used by
   * fromDecoded() factory method.
   */
  private SyncTransactionReceipt(final Supplier<TransactionReceipt> receiptSupplier) {
    this.encodedReceipt = null;
    this.receiptSupplier = receiptSupplier;
  }

  /**
   * Returns the raw RLP-encoded bytes of this receipt.
   *
   * <p>These bytes are used for computing the receipts root in the merkle trie verification.
   *
   * @return the encoded receipt bytes
   */
  public Bytes getEncodedBytes() {
    if (encodedReceipt != null) {
      return encodedReceipt;
    }
    // If we only have a decoded receipt (from fallback path), encode it for trie root computation
    throw new UnsupportedOperationException(
        "Cannot get encoded bytes from a SyncTransactionReceipt created from decoded receipt. "
            + "Use getReceiptSupplier() instead.");
  }

  /**
   * Returns a supplier that lazily decodes the full TransactionReceipt when called.
   *
   * <p>This should only be called when the full receipt structure is actually needed (e.g., for
   * database storage, API responses, or event processing).
   *
   * <p>The encoded bytes may be in different formats depending on how they were encoded:
   *
   * <ul>
   *   <li>TRIE_ROOT format: Raw bytes (type || rlp-list) for typed receipts, or rlp-list for legacy
   *   <li>Network format: RLP byte string wrapping (type || rlp-list) for typed receipts
   * </ul>
   *
   * @return a supplier that decodes and returns the full TransactionReceipt
   */
  public Supplier<TransactionReceipt> getReceiptSupplier() {
    // If this was created from an already-decoded receipt (fallback path), return that supplier
    if (receiptSupplier != null) {
      return receiptSupplier;
    }

    // Otherwise, create a supplier that lazily decodes from the encoded bytes
    return () -> {
      final BytesValueRLPInput input = new BytesValueRLPInput(encodedReceipt, false);

      // Check if this is a typed receipt in TRIE_ROOT format (raw bytes without RLP wrapper)
      // Typed receipts start with a transaction type byte (0x01, 0x02, etc.)
      // Legacy receipts start with an RLP list header (0xc0 or higher)
      if (encodedReceipt.size() > 0 && (encodedReceipt.get(0) & 0xFF) < 0xc0) {
        // This is a typed receipt in raw format (TRIE_ROOT encoding)
        // Wrap it in an RLP byte string so the decoder can read it
        final BytesValueRLPOutput wrapper = new BytesValueRLPOutput();
        wrapper.writeBytes(encodedReceipt);
        return TransactionReceiptDecoder.readFrom(
            new BytesValueRLPInput(wrapper.encoded(), false), true);
      } else {
        // Legacy receipt or already wrapped typed receipt
        return TransactionReceiptDecoder.readFrom(input, true);
      }
    };
  }

  /**
   * Reads a SyncTransactionReceipt from RLP input without fully decoding it.
   *
   * <p>This method captures the raw bytes of the receipt from the RLP stream. It handles both:
   *
   * <ul>
   *   <li>Legacy receipts (pre-EIP-2718): RLP list format
   *   <li>Typed receipts (EIP-2718+): Encoded as opaque bytes containing tx-type || rlp([...])
   * </ul>
   *
   * <p>Note: When receipts are encoded with TRIE_ROOT configuration (as used for merkle trie
   * computation), typed receipts are wrapped as RLP byte strings, while legacy receipts remain as
   * RLP lists.
   *
   * @param input the RLP input stream positioned at a receipt
   * @return a SyncTransactionReceipt containing the raw encoded bytes
   */
  public static SyncTransactionReceipt readFrom(final RLPInput input) {
    final Bytes receiptBytes;

    if (!input.nextIsList()) {
      // Typed receipt (EIP-2718): encoded as RLP bytes containing: tx-type || rlp([...])
      // TransactionReceiptEncoder.writeTo() with TRIE_ROOT config wraps typed receipts
      // as: rlpOutput.writeBytes(RLP.encode(...))
      // So we just read the bytes directly - they already contain the full encoded receipt
      receiptBytes = input.readBytes();
    } else {
      // Legacy receipt: capture the list with its RLP header
      // Format: rlp([post-state-or-status, cumulative-gas, bloom, logs])
      receiptBytes = input.currentListAsBytesNoCopy(true);
    }

    return new SyncTransactionReceipt(receiptBytes);
  }

  /**
   * Create a SyncTransactionReceipt from an already decoded TransactionReceipt. This is used for
   * the fallback path when receipts are fetched using the old system that fully decodes receipts.
   *
   * @param receipt The fully decoded transaction receipt
   * @return SyncTransactionReceipt wrapping the decoded receipt
   */
  public static SyncTransactionReceipt fromDecoded(final TransactionReceipt receipt) {
    // Since we already have a decoded receipt, we don't need to store the encoded form
    // Just create a supplier that returns the receipt directly
    return new SyncTransactionReceipt(() -> receipt);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SyncTransactionReceipt that = (SyncTransactionReceipt) o;
    // For equality, compare based on what we have
    if (encodedReceipt != null && that.encodedReceipt != null) {
      return Objects.equals(encodedReceipt, that.encodedReceipt);
    }
    // If one or both have decoded receipts, we can't easily compare without decoding both
    return false;
  }

  @Override
  public int hashCode() {
    return encodedReceipt != null ? Objects.hash(encodedReceipt) : Objects.hash(receiptSupplier);
  }

  @Override
  public String toString() {
    if (encodedReceipt != null) {
      return "SyncTransactionReceipt{" + "encodedReceipt=" + encodedReceipt + '}';
    } else {
      return "SyncTransactionReceipt{" + "decodedReceipt=<supplier>" + '}';
    }
  }
}
