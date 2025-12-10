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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ARCHIVE_STATE_INDEX;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Index for tracking which blocks modified which accounts and storage slots in Bonsai Archive mode.
 * This enables O(1) lookups instead of expensive seekForPrev operations.
 *
 * <p>Index entries are delta-encoded to minimize storage overhead. For each account or storage
 * slot, we store a sorted list of block numbers where modifications occurred.
 *
 * <p>Index Key Format: - Account: [32-byte accountHash] - Storage: [32-byte accountHash][32-byte
 * slotHash]
 *
 * <p>Index Value Format (delta-encoded): - [4 bytes: count] - [8 bytes: first block number] - [8
 * bytes: delta to second block] - [8 bytes: delta to third block] - ...
 */
public class BonsaiArchiveStateIndex {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveStateIndex.class);

  // Special key to track whether index is fully built
  private static final byte[] INDEX_BUILT_KEY = "INDEX_BUILT".getBytes(StandardCharsets.UTF_8);

  // Special key to track the latest block indexed
  private static final byte[] LATEST_INDEXED_BLOCK_KEY =
      "LATEST_INDEXED_BLOCK".getBytes(StandardCharsets.UTF_8);

  /**
   * Adds an account modification entry to the index.
   *
   * @param storage the storage for reading existing data
   * @param tx the storage transaction for writing
   * @param accountHash the account hash
   * @param blockNumber the block number where the modification occurred
   */
  public void addAccountModification(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction tx,
      final Hash accountHash,
      final long blockNumber) {

    byte[] key = accountHash.toArrayUnsafe();
    addModification(storage, tx, key, blockNumber);
  }

  /**
   * Adds a storage modification entry to the index.
   *
   * @param storage the storage for reading existing data
   * @param tx the storage transaction for writing
   * @param accountHash the account hash
   * @param slotHash the storage slot hash
   * @param blockNumber the block number where the modification occurred
   */
  public void addStorageModification(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction tx,
      final Hash accountHash,
      final Hash slotHash,
      final long blockNumber) {

    byte[] key = Bytes.concatenate(accountHash, slotHash).toArrayUnsafe();
    addModification(storage, tx, key, blockNumber);
  }

  /**
   * Finds the block number where an account was last modified at or before the target block.
   *
   * @param storage the key-value storage
   * @param accountHash the account hash
   * @param targetBlock the target block number
   * @return the block number where the account was last modified, or empty if not found
   */
  public Optional<Long> findAccountModificationBlockNumber(
      final SegmentedKeyValueStorage storage, final Hash accountHash, final long targetBlock) {

    byte[] key = accountHash.toArrayUnsafe();
    return findModificationBlockNumber(storage, key, targetBlock);
  }

  /**
   * Finds the block number where a storage slot was last modified at or before the target block.
   *
   * @param storage the key-value storage
   * @param accountHash the account hash
   * @param slotHash the storage slot hash
   * @param targetBlock the target block number
   * @return the block number where the storage was last modified, or empty if not found
   */
  public Optional<Long> findStorageModificationBlockNumber(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Hash slotHash,
      final long targetBlock) {

    byte[] key = Bytes.concatenate(accountHash, slotHash).toArrayUnsafe();
    return findModificationBlockNumber(storage, key, targetBlock);
  }

  /**
   * Checks if the index has been fully built.
   *
   * @param storage the key-value storage
   * @return true if the index is built, false otherwise
   */
  public boolean isIndexBuilt(final SegmentedKeyValueStorage storage) {
    return storage.get(ARCHIVE_STATE_INDEX, INDEX_BUILT_KEY).isPresent();
  }

  /**
   * Marks the index as fully built.
   *
   * @param tx the storage transaction
   * @param latestBlock the latest block number indexed
   */
  public void markIndexBuilt(final SegmentedKeyValueStorageTransaction tx, final long latestBlock) {
    tx.put(ARCHIVE_STATE_INDEX, INDEX_BUILT_KEY, longToBytes(latestBlock));
    updateLatestIndexedBlock(tx, latestBlock);
  }

  /**
   * Gets the latest block number that has been indexed.
   *
   * @param storage the key-value storage
   * @return the latest indexed block number, or empty if not available
   */
  public Optional<Long> getLatestIndexedBlock(final SegmentedKeyValueStorage storage) {
    return storage.get(ARCHIVE_STATE_INDEX, LATEST_INDEXED_BLOCK_KEY).map(this::bytesToLong);
  }

  /**
   * Updates the latest indexed block number.
   *
   * @param tx the storage transaction
   * @param blockNumber the block number
   */
  public void updateLatestIndexedBlock(
      final SegmentedKeyValueStorageTransaction tx, final long blockNumber) {
    tx.put(ARCHIVE_STATE_INDEX, LATEST_INDEXED_BLOCK_KEY, longToBytes(blockNumber));
  }

  /**
   * Internal method to add a modification entry for any key. Note: This method needs access to both
   * storage (for reading) and transaction (for writing). The storage parameter should be passed
   * from the caller.
   *
   * @param storage the storage for reading existing data
   * @param tx the storage transaction for writing
   * @param key the index key (account hash or account+slot hash)
   * @param blockNumber the block number
   */
  private void addModification(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction tx,
      final byte[] key,
      final long blockNumber) {

    // Get existing block list or create new one
    Optional<byte[]> existingData = storage.get(ARCHIVE_STATE_INDEX, key);
    List<Long> blockNumbers;

    if (existingData.isPresent()) {
      blockNumbers = deserializeBlockNumbers(existingData.get());
    } else {
      blockNumbers = new ArrayList<>();
    }

    // Add new block number if not already present (deduplication)
    if (blockNumbers.isEmpty() || blockNumbers.get(blockNumbers.size() - 1) != blockNumber) {
      blockNumbers.add(blockNumber);
      // Keep list sorted (should already be sorted if blocks are processed in order)
      Collections.sort(blockNumbers);

      // Serialize and store
      byte[] serialized = serializeBlockNumbers(blockNumbers);
      tx.put(ARCHIVE_STATE_INDEX, key, serialized);
    }
  }

  /**
   * Internal method to find modification block number for any key.
   *
   * @param storage the key-value storage
   * @param key the index key
   * @param targetBlock the target block number
   * @return the block number, or empty if not found
   */
  private Optional<Long> findModificationBlockNumber(
      final SegmentedKeyValueStorage storage, final byte[] key, final long targetBlock) {

    Optional<byte[]> data = storage.get(ARCHIVE_STATE_INDEX, key);
    if (data.isEmpty()) {
      return Optional.empty();
    }

    List<Long> blockNumbers = deserializeBlockNumbers(data.get());
    return binarySearchNearest(blockNumbers, targetBlock);
  }

  /**
   * Serializes a list of block numbers using delta encoding.
   *
   * <p>Format: [4 bytes: count][8 bytes: first block][8 bytes: delta1][8 bytes: delta2]...
   *
   * @param blockNumbers sorted list of block numbers
   * @return serialized bytes
   */
  private byte[] serializeBlockNumbers(final List<Long> blockNumbers) {
    if (blockNumbers.isEmpty()) {
      return new byte[4]; // Just count=0
    }

    // Calculate size: 4 bytes for count + 8 bytes per block number
    int size = 4 + (blockNumbers.size() * 8);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Write count
    buffer.putInt(blockNumbers.size());

    // Write first block number as absolute value
    long previousBlock = blockNumbers.get(0);
    buffer.putLong(previousBlock);

    // Write remaining block numbers as deltas
    for (int i = 1; i < blockNumbers.size(); i++) {
      long currentBlock = blockNumbers.get(i);
      long delta = currentBlock - previousBlock;
      buffer.putLong(delta);
      previousBlock = currentBlock;
    }

    return buffer.array();
  }

  /**
   * Deserializes a list of block numbers from delta-encoded format.
   *
   * @param data the serialized data
   * @return list of block numbers
   */
  private List<Long> deserializeBlockNumbers(final byte[] data) {
    if (data.length < 4) {
      LOG.warn("Invalid block number data: too short");
      return new ArrayList<>();
    }

    ByteBuffer buffer = ByteBuffer.wrap(data);
    int count = buffer.getInt();

    if (count == 0) {
      return new ArrayList<>();
    }

    if (data.length < 4 + (count * 8)) {
      LOG.warn("Invalid block number data: count mismatch");
      return new ArrayList<>();
    }

    List<Long> blockNumbers = new ArrayList<>(count);

    // Read first block number (absolute)
    long previousBlock = buffer.getLong();
    blockNumbers.add(previousBlock);

    // Read remaining block numbers (deltas)
    for (int i = 1; i < count; i++) {
      long delta = buffer.getLong();
      long currentBlock = previousBlock + delta;
      blockNumbers.add(currentBlock);
      previousBlock = currentBlock;
    }

    return blockNumbers;
  }

  /**
   * Binary search to find the largest block number <= target.
   *
   * @param blockNumbers sorted list of block numbers
   * @param target the target block number
   * @return the nearest block number <= target, or empty if all blocks are > target
   */
  private Optional<Long> binarySearchNearest(final List<Long> blockNumbers, final long target) {
    if (blockNumbers.isEmpty()) {
      return Optional.empty();
    }

    // If target is before first modification, return empty
    if (target < blockNumbers.get(0)) {
      return Optional.empty();
    }

    // If target is after or at last modification, return last
    int lastIndex = blockNumbers.size() - 1;
    if (target >= blockNumbers.get(lastIndex)) {
      return Optional.of(blockNumbers.get(lastIndex));
    }

    // Binary search for largest value <= target
    int left = 0;
    int right = lastIndex;
    int result = 0;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      long midValue = blockNumbers.get(mid);

      if (midValue == target) {
        return Optional.of(midValue);
      } else if (midValue < target) {
        result = mid;
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }

    return Optional.of(blockNumbers.get(result));
  }

  /**
   * Converts a long to bytes.
   *
   * @param value the long value
   * @return byte array
   */
  private byte[] longToBytes(final long value) {
    return ByteBuffer.allocate(8).putLong(value).array();
  }

  /**
   * Converts bytes to a long.
   *
   * @param bytes the byte array
   * @return the long value
   */
  private long bytesToLong(final byte[] bytes) {
    return ByteBuffer.wrap(bytes).getLong();
  }
}
