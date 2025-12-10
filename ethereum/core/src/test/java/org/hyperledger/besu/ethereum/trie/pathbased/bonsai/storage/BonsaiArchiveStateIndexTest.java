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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ARCHIVE_STATE_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BonsaiArchiveStateIndexTest {

  @Mock private SegmentedKeyValueStorage storage;
  @Mock private SegmentedKeyValueStorageTransaction transaction;

  private BonsaiArchiveStateIndex index;
  private Hash testAccountHash;
  private Hash testSlotHash;

  @BeforeEach
  public void setup() {
    index = new BonsaiArchiveStateIndex();
    testAccountHash = Hash.hash(Bytes32.random());
    testSlotHash = Hash.hash(Bytes32.random());
  }

  @Test
  public void testAddAccountModification_SingleBlock() {
    // Add a single account modification
    index.addAccountModification(storage, transaction, testAccountHash, 100L);

    // Verify the data was stored
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(transaction).put(eq(ARCHIVE_STATE_INDEX), keyCaptor.capture(), valueCaptor.capture());

    assertThat(keyCaptor.getValue()).isEqualTo(testAccountHash.toArrayUnsafe());
  }

  @Test
  public void testAddAccountModification_MultipleBlocks() {
    // Simulate multiple modifications
    when(storage.get(eq(ARCHIVE_STATE_INDEX), eq(testAccountHash.toArrayUnsafe())))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(serializeSingleBlock(100L)))
        .thenReturn(Optional.of(serializeTwoBlocks(100L, 200L)));

    index.addAccountModification(storage, transaction, testAccountHash, 100L);
    index.addAccountModification(storage, transaction, testAccountHash, 200L);
    index.addAccountModification(storage, transaction, testAccountHash, 300L);

    // Verify three puts occurred
    verify(transaction, org.mockito.Mockito.times(3))
        .put(eq(ARCHIVE_STATE_INDEX), eq(testAccountHash.toArrayUnsafe()), any(byte[].class));
  }

  @Test
  public void testAddAccountModification_Deduplication() {
    // Add same block twice
    index.addAccountModification(storage, transaction, testAccountHash, 100L);

    when(storage.get(eq(ARCHIVE_STATE_INDEX), eq(testAccountHash.toArrayUnsafe())))
        .thenReturn(Optional.of(serializeSingleBlock(100L)));

    index.addAccountModification(storage, transaction, testAccountHash, 100L);

    // Should only store once (second add sees it already exists)
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(transaction)
        .put(eq(ARCHIVE_STATE_INDEX), eq(testAccountHash.toArrayUnsafe()), valueCaptor.capture());
  }

  @Test
  public void testAddStorageModification() {
    // Add storage modification
    index.addStorageModification(storage, transaction, testAccountHash, testSlotHash, 150L);

    // Verify the compound key was used
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(transaction).put(eq(ARCHIVE_STATE_INDEX), keyCaptor.capture(), any(byte[].class));

    byte[] expectedKey =
        org.apache.tuweni.bytes.Bytes.concatenate(testAccountHash, testSlotHash).toArrayUnsafe();
    assertThat(keyCaptor.getValue()).isEqualTo(expectedKey);
  }

  @Test
  public void testFindAccountModificationBlockNumber_ExactMatch() {
    // Setup: account modified at blocks 100, 200, 300
    byte[] serializedData = serializeThreeBlocks(100L, 200L, 300L);
    when(storage.get(ARCHIVE_STATE_INDEX, testAccountHash.toArrayUnsafe()))
        .thenReturn(Optional.of(serializedData));

    // Query for exact block 200
    Optional<Long> result =
        index.findAccountModificationBlockNumber(storage, testAccountHash, 200L);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(200L);
  }

  @Test
  public void testFindAccountModificationBlockNumber_BetweenBlocks() {
    // Setup: account modified at blocks 100, 200, 300
    byte[] serializedData = serializeThreeBlocks(100L, 200L, 300L);
    when(storage.get(ARCHIVE_STATE_INDEX, testAccountHash.toArrayUnsafe()))
        .thenReturn(Optional.of(serializedData));

    // Query for block 250 (between 200 and 300) - should return 200
    Optional<Long> result =
        index.findAccountModificationBlockNumber(storage, testAccountHash, 250L);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(200L);
  }

  @Test
  public void testFindAccountModificationBlockNumber_BeforeFirst() {
    // Setup: account modified at blocks 100, 200, 300
    byte[] serializedData = serializeThreeBlocks(100L, 200L, 300L);
    when(storage.get(ARCHIVE_STATE_INDEX, testAccountHash.toArrayUnsafe()))
        .thenReturn(Optional.of(serializedData));

    // Query for block 50 (before first modification) - should return empty
    Optional<Long> result = index.findAccountModificationBlockNumber(storage, testAccountHash, 50L);

    assertThat(result).isEmpty();
  }

  @Test
  public void testFindAccountModificationBlockNumber_AfterLast() {
    // Setup: account modified at blocks 100, 200, 300
    byte[] serializedData = serializeThreeBlocks(100L, 200L, 300L);
    when(storage.get(ARCHIVE_STATE_INDEX, testAccountHash.toArrayUnsafe()))
        .thenReturn(Optional.of(serializedData));

    // Query for block 400 (after last modification) - should return 300
    Optional<Long> result =
        index.findAccountModificationBlockNumber(storage, testAccountHash, 400L);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(300L);
  }

  @Test
  public void testFindAccountModificationBlockNumber_NotFound() {
    // Setup: no index entry exists
    when(storage.get(ARCHIVE_STATE_INDEX, testAccountHash.toArrayUnsafe()))
        .thenReturn(Optional.empty());

    // Query should return empty
    Optional<Long> result =
        index.findAccountModificationBlockNumber(storage, testAccountHash, 100L);

    assertThat(result).isEmpty();
  }

  @Test
  public void testFindStorageModificationBlockNumber() {
    // Setup: storage modified at blocks 150, 250
    byte[] serializedData = serializeTwoBlocks(150L, 250L);
    byte[] compoundKey =
        org.apache.tuweni.bytes.Bytes.concatenate(testAccountHash, testSlotHash).toArrayUnsafe();
    when(storage.get(ARCHIVE_STATE_INDEX, compoundKey)).thenReturn(Optional.of(serializedData));

    // Query for block 200 (between 150 and 250) - should return 150
    Optional<Long> result =
        index.findStorageModificationBlockNumber(storage, testAccountHash, testSlotHash, 200L);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(150L);
  }

  @Test
  public void testIsIndexBuilt_NotBuilt() {
    when(storage.get(eq(ARCHIVE_STATE_INDEX), any())).thenReturn(Optional.empty());

    assertThat(index.isIndexBuilt(storage)).isFalse();
  }

  @Test
  public void testIsIndexBuilt_Built() {
    when(storage.get(eq(ARCHIVE_STATE_INDEX), any())).thenReturn(Optional.of(new byte[] {1}));

    assertThat(index.isIndexBuilt(storage)).isTrue();
  }

  @Test
  public void testMarkIndexBuilt() {
    index.markIndexBuilt(transaction, 1000L);

    // Verify both INDEX_BUILT and LATEST_INDEXED_BLOCK were written (2 put calls)
    verify(transaction, org.mockito.Mockito.times(2))
        .put(eq(ARCHIVE_STATE_INDEX), any(byte[].class), any(byte[].class));
  }

  @Test
  public void testGetLatestIndexedBlock() {
    byte[] blockNumberBytes = longToBytes(500L);
    when(storage.get(eq(ARCHIVE_STATE_INDEX), any())).thenReturn(Optional.of(blockNumberBytes));

    Optional<Long> result = index.getLatestIndexedBlock(storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(500L);
  }

  @Test
  public void testUpdateLatestIndexedBlock() {
    index.updateLatestIndexedBlock(transaction, 750L);

    verify(transaction).put(eq(ARCHIVE_STATE_INDEX), any(byte[].class), eq(longToBytes(750L)));
  }

  @Test
  public void testLargeBlockNumbers() {
    // Test with very large block numbers (near Long.MAX_VALUE)
    long largeBlock1 = Long.MAX_VALUE - 1000;
    long largeBlock2 = Long.MAX_VALUE - 500;

    byte[] serializedData = serializeTwoBlocks(largeBlock1, largeBlock2);
    when(storage.get(ARCHIVE_STATE_INDEX, testAccountHash.toArrayUnsafe()))
        .thenReturn(Optional.of(serializedData));

    Optional<Long> result =
        index.findAccountModificationBlockNumber(storage, testAccountHash, Long.MAX_VALUE);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(largeBlock2);
  }

  @Test
  public void testManyModifications() {
    // Test with many modifications to ensure delta encoding works correctly
    java.util.List<Long> blocks = new java.util.ArrayList<>();
    for (long i = 0; i < 100; i++) {
      blocks.add(i * 1000); // 0, 1000, 2000, ... 99000
    }

    byte[] serializedData = serializeBlockList(blocks);
    when(storage.get(ARCHIVE_STATE_INDEX, testAccountHash.toArrayUnsafe()))
        .thenReturn(Optional.of(serializedData));

    // Query for block 55500 (between 55000 and 56000) - should return 55000
    Optional<Long> result =
        index.findAccountModificationBlockNumber(storage, testAccountHash, 55500L);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(55000L);
  }

  // Helper methods to manually serialize block numbers for testing

  private byte[] serializeSingleBlock(final long block) {
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(12);
    buffer.putInt(1); // count
    buffer.putLong(block); // first block
    return buffer.array();
  }

  private byte[] serializeTwoBlocks(final long block1, final long block2) {
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(20);
    buffer.putInt(2); // count
    buffer.putLong(block1); // first block
    buffer.putLong(block2 - block1); // delta
    return buffer.array();
  }

  private byte[] serializeThreeBlocks(final long block1, final long block2, final long block3) {
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(28);
    buffer.putInt(3); // count
    buffer.putLong(block1); // first block
    buffer.putLong(block2 - block1); // delta to second
    buffer.putLong(block3 - block2); // delta to third
    return buffer.array();
  }

  private byte[] serializeBlockList(final java.util.List<Long> blocks) {
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(4 + blocks.size() * 8);
    buffer.putInt(blocks.size());
    buffer.putLong(blocks.get(0));
    for (int i = 1; i < blocks.size(); i++) {
      buffer.putLong(blocks.get(i) - blocks.get(i - 1));
    }
    return buffer.array();
  }

  private byte[] longToBytes(final long value) {
    return java.nio.ByteBuffer.allocate(8).putLong(value).array();
  }
}
