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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.evm.log.Log;
import org.hyperledger.besu.evm.log.LogTopic;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class SyncTransactionReceiptTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x1234567890123456789012345678901234567890");
  private static final LogTopic TOPIC1 = LogTopic.create(Bytes.repeat((byte) 0x01, 32));
  private static final LogTopic TOPIC2 = LogTopic.create(Bytes.repeat((byte) 0x02, 32));

  @Test
  public void shouldReadLegacyStatusReceipt() {
    // Create a legacy receipt with status
    final TransactionReceipt receipt =
        new TransactionReceipt(
            TransactionType.FRONTIER, 1, 12345L, createLogs(2), Optional.empty());

    // Encode it
    final Bytes encodedBytes = encodeReceipt(receipt);

    // Create SyncTransactionReceipt by reading from RLP
    final BytesValueRLPInput input = new BytesValueRLPInput(encodedBytes, false);
    final SyncTransactionReceipt syncReceipt = SyncTransactionReceipt.readFrom(input);

    // Verify the encoded bytes are stored correctly
    assertThat(syncReceipt.getEncodedBytes()).isEqualTo(encodedBytes);

    // Verify lazy decoding produces the same receipt
    final TransactionReceipt decoded = syncReceipt.getReceiptSupplier().get();
    assertThat(decoded.getStatus()).isEqualTo(1);
    assertThat(decoded.getCumulativeGasUsed()).isEqualTo(12345L);
    assertThat(decoded.getLogsList()).hasSize(2);
    assertThat(decoded.getTransactionType()).isEqualTo(TransactionType.FRONTIER);
  }

  @Test
  public void shouldReadLegacyStateRootReceipt() {
    // Create a legacy receipt with state root (pre-Byzantium)
    final Hash stateRoot =
        Hash.fromHexString("0x1234567890123456789012345678901234567890123456789012345678901234");
    final TransactionReceipt receipt =
        new TransactionReceipt(stateRoot, 54321L, createLogs(1), Optional.empty());

    // Encode it
    final Bytes encodedBytes = encodeReceipt(receipt);

    // Create SyncTransactionReceipt by reading from RLP
    final BytesValueRLPInput input = new BytesValueRLPInput(encodedBytes, false);
    final SyncTransactionReceipt syncReceipt = SyncTransactionReceipt.readFrom(input);

    // Verify the encoded bytes are stored correctly
    assertThat(syncReceipt.getEncodedBytes()).isEqualTo(encodedBytes);

    // Verify lazy decoding produces the same receipt
    final TransactionReceipt decoded = syncReceipt.getReceiptSupplier().get();
    assertThat(decoded.getStateRoot()).isEqualTo(stateRoot);
    assertThat(decoded.getCumulativeGasUsed()).isEqualTo(54321L);
    assertThat(decoded.getLogsList()).hasSize(1);
  }

  @Test
  public void shouldReadTypedReceipt_EIP2930() {
    // Create an EIP-2930 typed receipt
    final TransactionReceipt receipt =
        new TransactionReceipt(
            TransactionType.ACCESS_LIST, 1, 98765L, createLogs(3), Optional.empty());

    // Encode it the way it's encoded for trie root
    final Bytes encodedBytes = encodeReceipt(receipt);

    // For TRIE_ROOT encoding, typed receipts are raw bytes (type || rlp-list), not wrapped in RLP
    // So we create SyncTransactionReceipt directly from the bytes
    final SyncTransactionReceipt syncReceipt = new SyncTransactionReceipt(encodedBytes);

    // Verify the encoded bytes are stored correctly (including type prefix)
    assertThat(syncReceipt.getEncodedBytes()).isEqualTo(encodedBytes);

    // Verify lazy decoding produces the same receipt
    final TransactionReceipt decoded = syncReceipt.getReceiptSupplier().get();
    assertThat(decoded.getTransactionType()).isEqualTo(TransactionType.ACCESS_LIST);
    assertThat(decoded.getStatus()).isEqualTo(1);
    assertThat(decoded.getCumulativeGasUsed()).isEqualTo(98765L);
    assertThat(decoded.getLogsList()).hasSize(3);
  }

  @Test
  public void shouldReadTypedReceipt_EIP1559() {
    // Create an EIP-1559 typed receipt
    final TransactionReceipt receipt =
        new TransactionReceipt(TransactionType.EIP1559, 0, 11111L, createLogs(0), Optional.empty());

    // Encode it the way it's encoded for trie root
    final Bytes encodedBytes = encodeReceipt(receipt);

    // For TRIE_ROOT encoding, typed receipts are raw bytes (type || rlp-list), not wrapped in RLP
    // So we create SyncTransactionReceipt directly from the bytes
    final SyncTransactionReceipt syncReceipt = new SyncTransactionReceipt(encodedBytes);

    // Verify the encoded bytes are stored correctly
    assertThat(syncReceipt.getEncodedBytes()).isEqualTo(encodedBytes);

    // Verify lazy decoding produces the same receipt
    final TransactionReceipt decoded = syncReceipt.getReceiptSupplier().get();
    assertThat(decoded.getTransactionType()).isEqualTo(TransactionType.EIP1559);
    assertThat(decoded.getStatus()).isEqualTo(0); // Failed transaction
    assertThat(decoded.getCumulativeGasUsed()).isEqualTo(11111L);
    assertThat(decoded.getLogsList()).isEmpty();
  }

  @Test
  public void shouldReadReceiptWithRevertReason() {
    // Create a receipt with revert reason
    final Bytes revertReason = Bytes.fromHexString("0x08c379a0"); // Error(string) selector
    final TransactionReceipt receipt =
        new TransactionReceipt(
            TransactionType.FRONTIER, 0, 55555L, createLogs(0), Optional.of(revertReason));

    // Encode it - TRIE_ROOT config doesn't include revert reasons!
    // We need to use a different config that includes them
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    TransactionReceiptEncoder.writeTo(
        receipt, output, TransactionReceiptEncodingConfiguration.STORAGE_WITHOUT_COMPACTION);
    final Bytes encodedBytes = output.encoded();

    // Create SyncTransactionReceipt directly from the bytes
    final SyncTransactionReceipt syncReceipt = new SyncTransactionReceipt(encodedBytes);

    // Verify lazy decoding preserves revert reason
    // Note: we need to tell the decoder that revert reasons are allowed
    final TransactionReceipt decoded = syncReceipt.getReceiptSupplier().get();
    assertThat(decoded.getRevertReason()).isPresent();
    assertThat(decoded.getRevertReason().get()).isEqualTo(revertReason);
    assertThat(decoded.getStatus()).isEqualTo(0);
  }

  @Test
  public void shouldComputeSameReceiptsRootAsFullyDecoded() {
    // Create a list of receipts with mixed types
    final List<TransactionReceipt> fullReceipts = new ArrayList<>();
    fullReceipts.add(
        new TransactionReceipt(
            TransactionType.FRONTIER, 1, 1000L, createLogs(1), Optional.empty()));
    fullReceipts.add(
        new TransactionReceipt(
            TransactionType.ACCESS_LIST, 1, 2000L, createLogs(2), Optional.empty()));
    fullReceipts.add(
        new TransactionReceipt(TransactionType.EIP1559, 0, 3000L, createLogs(0), Optional.empty()));

    // Compute receipts root using fully decoded receipts
    final Hash fullReceiptsRoot = BodyValidation.receiptsRoot(fullReceipts);

    // Create SyncTransactionReceipts from the same receipts
    // Each receipt is encoded the same way as BodyValidation.receiptsRoot() does it
    final List<SyncTransactionReceipt> syncReceipts = new ArrayList<>();
    for (final TransactionReceipt receipt : fullReceipts) {
      final Bytes encodedBytes = encodeReceipt(receipt);
      syncReceipts.add(new SyncTransactionReceipt(encodedBytes));
    }

    // Compute receipts root using sync receipts
    final Hash syncReceiptsRoot = BodyValidation.syncReceiptsRoot(syncReceipts);

    // They must match!
    assertThat(syncReceiptsRoot).isEqualTo(fullReceiptsRoot);
  }

  @Test
  public void shouldReadMultipleReceiptsFromRLPList() {
    // Encode a list of receipts as would be received from a peer (wire protocol encoding)
    final List<TransactionReceipt> receipts = new ArrayList<>();
    receipts.add(
        new TransactionReceipt(
            TransactionType.FRONTIER, 1, 1000L, createLogs(1), Optional.empty()));
    receipts.add(
        new TransactionReceipt(TransactionType.EIP1559, 1, 2000L, createLogs(2), Optional.empty()));
    receipts.add(
        new TransactionReceipt(
            TransactionType.ACCESS_LIST, 0, 3000L, createLogs(0), Optional.empty()));

    // Encode as a list using the network protocol encoding (with opaque bytes for typed receipts)
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    output.startList();
    for (final TransactionReceipt receipt : receipts) {
      TransactionReceiptEncoder.writeTo(
          receipt, output, TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);
    }
    output.endList();

    // Read back as SyncTransactionReceipts
    final BytesValueRLPInput input = new BytesValueRLPInput(output.encoded(), false);
    input.enterList();
    final List<SyncTransactionReceipt> syncReceipts = new ArrayList<>();
    while (!input.isEndOfCurrentList()) {
      syncReceipts.add(SyncTransactionReceipt.readFrom(input));
    }
    input.leaveList();

    // Verify we got all receipts
    assertThat(syncReceipts).hasSize(3);

    // Verify each can be decoded correctly
    assertThat(syncReceipts.get(0).getReceiptSupplier().get().getTransactionType())
        .isEqualTo(TransactionType.FRONTIER);
    assertThat(syncReceipts.get(1).getReceiptSupplier().get().getTransactionType())
        .isEqualTo(TransactionType.EIP1559);
    assertThat(syncReceipts.get(2).getReceiptSupplier().get().getTransactionType())
        .isEqualTo(TransactionType.ACCESS_LIST);
  }

  @Test
  public void shouldHandleEmptyLogs() {
    final TransactionReceipt receipt =
        new TransactionReceipt(TransactionType.FRONTIER, 1, 1000L, List.of(), Optional.empty());

    final Bytes encodedBytes = encodeReceipt(receipt);
    final BytesValueRLPInput input = new BytesValueRLPInput(encodedBytes, false);
    final SyncTransactionReceipt syncReceipt = SyncTransactionReceipt.readFrom(input);

    final TransactionReceipt decoded = syncReceipt.getReceiptSupplier().get();
    assertThat(decoded.getLogsList()).isEmpty();
  }

  // Helper methods

  private List<Log> createLogs(final int count) {
    final List<Log> logs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      logs.add(new Log(ADDRESS, Bytes.of(i), List.of(TOPIC1, TOPIC2)));
    }
    return logs;
  }

  private Bytes encodeReceipt(final TransactionReceipt receipt) {
    // Encode the same way receipts are encoded for trie root calculation
    // This matches what BodyValidation.receiptsRoot() does
    return RLP.encode(
        rlpOutput ->
            TransactionReceiptEncoder.writeTo(
                receipt, rlpOutput, TransactionReceiptEncodingConfiguration.TRIE_ROOT));
  }
}
