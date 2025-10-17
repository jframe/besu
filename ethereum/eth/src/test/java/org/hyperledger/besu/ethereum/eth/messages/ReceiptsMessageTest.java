/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.messages;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public final class ReceiptsMessageTest {
  @Test
  public void testReceiptsMessageEth68() {
    roundTripTest(TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);
  }

  @Test
  public void testReceiptsMessageEth69() {
    roundTripTest(TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION);
  }

  public void roundTripTest(final TransactionReceiptEncodingConfiguration encodingConfiguration) {
    // Generate some data
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final List<List<TransactionReceipt>> receipts = new ArrayList<>();
    final int dataCount = 20;
    final int receiptsPerSet = 3;
    for (int i = 0; i < dataCount; ++i) {
      final List<TransactionReceipt> receiptSet = new ArrayList<>();
      for (int j = 0; j < receiptsPerSet; j++) {
        receiptSet.add(gen.receipt());
      }
      receipts.add(receiptSet);
    }

    // Perform round-trip transformation
    // Create specific message, copy it to a generic message, then read back into a specific format
    final MessageData initialMessage = ReceiptsMessage.create(receipts, encodingConfiguration);
    final MessageData raw = new RawMessage(EthProtocolMessages.RECEIPTS, initialMessage.getData());
    final ReceiptsMessage message = ReceiptsMessage.readFrom(raw);

    // Read data back out after round trip and check they match originals.
    final Iterator<List<TransactionReceipt>> readData = message.receipts().iterator();
    for (int i = 0; i < dataCount; ++i) {
      Assertions.assertThat(readData.next()).isEqualTo(receipts.get(i));
    }
    Assertions.assertThat(readData.hasNext()).isFalse();
  }

  @Test
  public void testSyncReceiptsMessageEth68() {
    syncReceiptsRoundTripTest(
        TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);
  }

  @Test
  public void testSyncReceiptsMessageEth69() {
    syncReceiptsRoundTripTest(TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION);
  }

  public void syncReceiptsRoundTripTest(
      final TransactionReceiptEncodingConfiguration encodingConfiguration) {
    // Generate some data
    final BlockDataGenerator gen = new BlockDataGenerator(1);
    final List<List<TransactionReceipt>> receipts = new ArrayList<>();
    final int dataCount = 20;
    final int receiptsPerSet = 3;
    for (int i = 0; i < dataCount; ++i) {
      final List<TransactionReceipt> receiptSet = new ArrayList<>();
      for (int j = 0; j < receiptsPerSet; j++) {
        receiptSet.add(gen.receipt());
      }
      receipts.add(receiptSet);
    }

    // Perform round-trip transformation
    final MessageData initialMessage = ReceiptsMessage.create(receipts, encodingConfiguration);
    final MessageData raw = new RawMessage(EthProtocolMessages.RECEIPTS, initialMessage.getData());
    final ReceiptsMessage message = ReceiptsMessage.readFrom(raw);

    // Read data back as sync receipts
    final Iterator<List<SyncTransactionReceipt>> readSyncData = message.syncReceipts().iterator();
    for (int i = 0; i < dataCount; ++i) {
      List<SyncTransactionReceipt> syncReceiptSet = readSyncData.next();
      List<TransactionReceipt> originalReceiptSet = receipts.get(i);

      // Verify same number of receipts
      Assertions.assertThat(syncReceiptSet).hasSize(originalReceiptSet.size());

      // Verify each sync receipt can be decoded back to the original
      for (int j = 0; j < syncReceiptSet.size(); j++) {
        TransactionReceipt decodedReceipt = syncReceiptSet.get(j).getReceiptSupplier().get();
        Assertions.assertThat(decodedReceipt).isEqualTo(originalReceiptSet.get(j));
      }
    }
    Assertions.assertThat(readSyncData.hasNext()).isFalse();
  }

  @Test
  public void testSyncReceiptsComputeSameReceiptsRoot() {
    final TransactionReceiptEncodingConfiguration encodingConfiguration =
        TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION;

    // Generate some data
    final BlockDataGenerator gen = new BlockDataGenerator(42);
    final List<List<TransactionReceipt>> receipts = new ArrayList<>();
    final int dataCount = 5;
    final int receiptsPerSet = 4;
    for (int i = 0; i < dataCount; ++i) {
      final List<TransactionReceipt> receiptSet = new ArrayList<>();
      for (int j = 0; j < receiptsPerSet; j++) {
        receiptSet.add(gen.receipt());
      }
      receipts.add(receiptSet);
    }

    // Create message
    final MessageData initialMessage = ReceiptsMessage.create(receipts, encodingConfiguration);
    final MessageData raw = new RawMessage(EthProtocolMessages.RECEIPTS, initialMessage.getData());
    final ReceiptsMessage message = ReceiptsMessage.readFrom(raw);

    // Verify that sync receipts compute the same receipts root as fully decoded receipts
    final List<List<SyncTransactionReceipt>> syncReceiptsList = message.syncReceipts();
    for (int i = 0; i < dataCount; i++) {
      List<TransactionReceipt> originalReceipts = receipts.get(i);
      List<SyncTransactionReceipt> syncReceipts = syncReceiptsList.get(i);

      // Compute roots using both methods
      var expectedRoot = BodyValidation.receiptsRoot(originalReceipts);
      var actualRoot = BodyValidation.syncReceiptsRoot(syncReceipts);

      Assertions.assertThat(actualRoot)
          .withFailMessage(
              "Receipts root mismatch for block %d: expected %s but got %s",
              i, expectedRoot, actualRoot)
          .isEqualTo(expectedRoot);
    }
  }

  @Test
  public void testSyncReceiptsWithEmptyReceiptsList() {
    final TransactionReceiptEncodingConfiguration encodingConfiguration =
        TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION;

    // Create message with empty receipts
    final List<List<TransactionReceipt>> emptyReceipts = List.of(List.of());
    final MessageData initialMessage = ReceiptsMessage.create(emptyReceipts, encodingConfiguration);
    final MessageData raw = new RawMessage(EthProtocolMessages.RECEIPTS, initialMessage.getData());
    final ReceiptsMessage message = ReceiptsMessage.readFrom(raw);

    // Read as sync receipts
    final List<List<SyncTransactionReceipt>> syncReceiptsList = message.syncReceipts();

    Assertions.assertThat(syncReceiptsList).hasSize(1);
    Assertions.assertThat(syncReceiptsList.get(0)).isEmpty();
  }
}
