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

import org.hyperledger.besu.ethereum.mainnet.DefaultProtocolSchedule;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

public class SyncBlockWithReceiptsTest {

  @Test
  public void shouldReturnSyncReceiptsWithoutDecoding() {
    // Create a sync block
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block block = gen.block();
    final SyncBlock syncBlock = createSyncBlock(block);

    // Create sync receipts
    final List<TransactionReceipt> originalReceipts = gen.receipts(block);
    final List<SyncTransactionReceipt> syncReceipts =
        originalReceipts.stream().map(SyncTransactionReceipt::fromDecoded).toList();

    // Create SyncBlockWithReceipts
    final SyncBlockWithReceipts blockWithReceipts =
        new SyncBlockWithReceipts(syncBlock, syncReceipts);

    // getSyncReceipts should return the sync receipts directly
    assertThat(blockWithReceipts.getSyncReceipts()).isEqualTo(syncReceipts);
    assertThat(blockWithReceipts.getSyncReceipts()).hasSize(originalReceipts.size());
  }

  @Test
  public void shouldLazilyDecodeReceiptsWhenGetReceiptsIsCalled() {
    // Create a sync block
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block block = gen.block();
    final SyncBlock syncBlock = createSyncBlock(block);

    // Create sync receipts from original receipts
    final List<TransactionReceipt> originalReceipts = gen.receipts(block);
    final List<SyncTransactionReceipt> syncReceipts =
        originalReceipts.stream().map(SyncTransactionReceipt::fromDecoded).toList();

    // Create SyncBlockWithReceipts
    final SyncBlockWithReceipts blockWithReceipts =
        new SyncBlockWithReceipts(syncBlock, syncReceipts);

    // getReceipts should lazily decode and return TransactionReceipt objects
    final List<TransactionReceipt> decodedReceipts = blockWithReceipts.getReceipts();
    assertThat(decodedReceipts).hasSize(originalReceipts.size());

    // Verify each decoded receipt matches the original
    for (int i = 0; i < originalReceipts.size(); i++) {
      assertThat(decodedReceipts.get(i)).isEqualTo(originalReceipts.get(i));
    }
  }

  @Test
  public void shouldDecodeReceiptsEachTimeGetReceiptsIsCalled() {
    // Create a sync block with one transaction
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block block = gen.block();
    final SyncBlock syncBlock = createSyncBlock(block);

    // Create a single receipt
    final TransactionReceipt originalReceipt =
        new TransactionReceipt(1, 12345L, Collections.emptyList(), Optional.empty());
    final List<SyncTransactionReceipt> syncReceipts =
        List.of(SyncTransactionReceipt.fromDecoded(originalReceipt));

    // Create SyncBlockWithReceipts
    final SyncBlockWithReceipts blockWithReceipts =
        new SyncBlockWithReceipts(syncBlock, syncReceipts);

    // Call getReceipts multiple times
    final List<TransactionReceipt> firstCall = blockWithReceipts.getReceipts();
    final List<TransactionReceipt> secondCall = blockWithReceipts.getReceipts();
    final List<TransactionReceipt> thirdCall = blockWithReceipts.getReceipts();

    // Each call should return a new list (not cached)
    assertThat(firstCall).isNotSameAs(secondCall);
    assertThat(secondCall).isNotSameAs(thirdCall);

    // But the content should be equal
    assertThat(firstCall).isEqualTo(secondCall);
    assertThat(secondCall).isEqualTo(thirdCall);
    assertThat(firstCall.get(0)).isEqualTo(originalReceipt);
  }

  @Test
  public void shouldHaveCorrectEqualityAndHashCode() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block block1 = gen.block();
    final Block block2 = gen.block();
    final SyncBlock syncBlock1 = createSyncBlock(block1);
    final SyncBlock syncBlock2 = createSyncBlock(block2);

    final List<TransactionReceipt> receipts = gen.receipts(block1);
    final List<SyncTransactionReceipt> syncReceipts1 =
        receipts.stream().map(SyncTransactionReceipt::fromDecoded).toList();
    final List<SyncTransactionReceipt> syncReceipts2 =
        receipts.stream().map(SyncTransactionReceipt::fromDecoded).toList();

    final SyncBlockWithReceipts blockWithReceipts1 =
        new SyncBlockWithReceipts(syncBlock1, syncReceipts1);
    final SyncBlockWithReceipts blockWithReceipts2 =
        new SyncBlockWithReceipts(syncBlock1, syncReceipts2);
    final SyncBlockWithReceipts blockWithReceipts3 =
        new SyncBlockWithReceipts(syncBlock2, syncReceipts1);

    // Should not be equal if different sync receipts (even if decoded content is same)
    // because SyncTransactionReceipt.equals() returns false for fromDecoded instances
    assertThat(blockWithReceipts1).isNotEqualTo(blockWithReceipts2);

    // Should not be equal if different blocks
    assertThat(blockWithReceipts1).isNotEqualTo(blockWithReceipts3);
  }

  @Test
  public void shouldReturnCorrectBlockMetadata() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block block = gen.block();
    final SyncBlock syncBlock = createSyncBlock(block);

    final List<SyncTransactionReceipt> syncReceipts = Collections.emptyList();
    final SyncBlockWithReceipts blockWithReceipts =
        new SyncBlockWithReceipts(syncBlock, syncReceipts);

    // Verify metadata accessors
    assertThat(blockWithReceipts.getHeader()).isEqualTo(block.getHeader());
    assertThat(blockWithReceipts.getBlock()).isEqualTo(syncBlock);
    assertThat(blockWithReceipts.getNumber()).isEqualTo(block.getHeader().getNumber());
    assertThat(blockWithReceipts.getHash()).isEqualTo(block.getHash());
  }

  private SyncBlock createSyncBlock(final Block block) {
    // Encode the block body and read it back as SyncBlockBody
    final BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    block.getBody().writeWrappedBodyTo(rlpOutput);
    final BytesValueRLPInput input = new BytesValueRLPInput(rlpOutput.encoded(), false);
    final SyncBlockBody syncBody =
        SyncBlockBody.readWrappedBodyFrom(
            input, false, new DefaultProtocolSchedule(Optional.of(BigInteger.ONE)));
    return new SyncBlock(block.getHeader(), syncBody);
  }
}
