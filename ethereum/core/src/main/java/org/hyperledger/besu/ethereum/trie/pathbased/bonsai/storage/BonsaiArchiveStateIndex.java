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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ARCHIVE_STATE_INDEX;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BonsaiArchiveStateIndex provides an index-based access mechanism for historical state lookups.
 * Instead of using expensive seekForPrev operations on versioned keys, this index tracks which
 * blocks modified specific accounts and storage slots, allowing for O(1) direct lookups.
 *
 * <p>Index Schema: - Account Index Key: [accountHash (32 bytes)] - Account Index Value: [sorted
 * list of block numbers where account changed] - Storage Index Key: [accountHash (32 bytes) +
 * slotHash (32 bytes)] - Storage Index Value: [sorted list of block numbers where storage slot
 * changed]
 *
 * <p>This is inspired by Geth's path-based archive implementation which uses a similar index to
 * quickly find which blocks modified specific state elements.
 */
public class BonsaiArchiveStateIndex {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveStateIndex.class);

  private final SegmentedKeyValueStorage storage;

  public BonsaiArchiveStateIndex(final SegmentedKeyValueStorage storage) {
    this.storage = storage;
  }

  /**
   * Add an account modification to the index.
   *
   * @param transaction the storage transaction to use
   * @param accountHash the hash of the account that was modified
   * @param blockNumber the block number where the modification occurred
   */
  public void addAccountModification(
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final long blockNumber) {

    byte[] key = accountHash.toArrayUnsafe();
    List<Long> blockNumbers = getBlockNumbersForKey(key);

    // Add the new block number if not already present
    if (blockNumbers.isEmpty() || blockNumbers.get(blockNumbers.size() - 1) != blockNumber) {
      blockNumbers.add(blockNumber);

      // Keep list sorted and deduplicated
      if (blockNumbers.size() > 1
          && blockNumbers.get(blockNumbers.size() - 1) < blockNumbers.get(blockNumbers.size() - 2)) {
        Collections.sort(blockNumbers);
      }

      try {
        byte[] value = serializeBlockNumbers(blockNumbers);
        transaction.put(ARCHIVE_STATE_INDEX, key, value);
      } catch (IOException e) {
        LOG.error("Failed to serialize block numbers for account {}", accountHash, e);
      }
    }
  }

  /**
   * Add a storage slot modification to the index.
   *
   * @param transaction the storage transaction to use
   * @param accountHash the hash of the account
   * @param slotKey the storage slot key that was modified
   * @param blockNumber the block number where the modification occurred
   */
  public void addStorageModification(
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final StorageSlotKey slotKey,
      final long blockNumber) {

    byte[] key = Bytes.concatenate(accountHash, slotKey.getSlotHash()).toArrayUnsafe();
    List<Long> blockNumbers = getBlockNumbersForKey(key);

    // Add the new block number if not already present
    if (blockNumbers.isEmpty() || blockNumbers.get(blockNumbers.size() - 1) != blockNumber) {
      blockNumbers.add(blockNumber);

      // Keep list sorted and deduplicated
      if (blockNumbers.size() > 1
          && blockNumbers.get(blockNumbers.size() - 1) < blockNumbers.get(blockNumbers.size() - 2)) {
        Collections.sort(blockNumbers);
      }

      try {
        byte[] value = serializeBlockNumbers(blockNumbers);
        transaction.put(ARCHIVE_STATE_INDEX, key, value);
      } catch (IOException e) {
        LOG.error(
            "Failed to serialize block numbers for storage slot {} in account {}",
            slotKey,
            accountHash,
            e);
      }
    }
  }

  /**
   * Find the block number at or before the target block where an account was last modified.
   *
   * @param accountHash the hash of the account to query
   * @param targetBlockNumber the target block number to search for
   * @return Optional containing the block number where the account was last modified at or before
   *     targetBlockNumber, or empty if not found
   */
  public Optional<Long> findAccountModificationBlockNumber(
      final Hash accountHash, final long targetBlockNumber) {

    byte[] key = accountHash.toArrayUnsafe();
    List<Long> blockNumbers = getBlockNumbersForKey(key);

    if (blockNumbers.isEmpty()) {
      return Optional.empty();
    }

    // Binary search for the largest block number <= targetBlockNumber
    return findBlockNumberBinarySearch(blockNumbers, targetBlockNumber);
  }

  /**
   * Find the block number at or before the target block where a storage slot was last modified.
   *
   * @param accountHash the hash of the account
   * @param slotKey the storage slot key to query
   * @param targetBlockNumber the target block number to search for
   * @return Optional containing the block number where the storage slot was last modified at or
   *     before targetBlockNumber, or empty if not found
   */
  public Optional<Long> findStorageModificationBlockNumber(
      final Hash accountHash, final StorageSlotKey slotKey, final long targetBlockNumber) {

    byte[] key = Bytes.concatenate(accountHash, slotKey.getSlotHash()).toArrayUnsafe();
    List<Long> blockNumbers = getBlockNumbersForKey(key);

    if (blockNumbers.isEmpty()) {
      return Optional.empty();
    }

    // Binary search for the largest block number <= targetBlockNumber
    return findBlockNumberBinarySearch(blockNumbers, targetBlockNumber);
  }

  /**
   * Get all block numbers for a given key.
   *
   * @param key the key to query
   * @return list of block numbers, sorted in ascending order
   */
  private List<Long> getBlockNumbersForKey(final byte[] key) {
    return storage
        .get(ARCHIVE_STATE_INDEX, key)
        .map(
            bytes -> {
              try {
                return deserializeBlockNumbers(bytes);
              } catch (IOException e) {
                LOG.error("Failed to deserialize block numbers for key {}", Bytes.wrap(key), e);
                return new ArrayList<Long>();
              }
            })
        .orElseGet(ArrayList::new);
  }

  /**
   * Binary search to find the largest block number <= target.
   *
   * @param blockNumbers sorted list of block numbers
   * @param targetBlockNumber the target block number
   * @return Optional containing the found block number, or empty if not found
   */
  private Optional<Long> findBlockNumberBinarySearch(
      final List<Long> blockNumbers, final long targetBlockNumber) {

    if (blockNumbers.isEmpty()) {
      return Optional.empty();
    }

    // If target is before the first block, no match
    if (targetBlockNumber < blockNumbers.get(0)) {
      return Optional.empty();
    }

    // If target is after or equal to the last block, return the last block
    if (targetBlockNumber >= blockNumbers.get(blockNumbers.size() - 1)) {
      return Optional.of(blockNumbers.get(blockNumbers.size() - 1));
    }

    // Binary search for the largest value <= target
    int left = 0;
    int right = blockNumbers.size() - 1;
    int result = -1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      long midValue = blockNumbers.get(mid);

      if (midValue <= targetBlockNumber) {
        result = mid;
        left = mid + 1; // Look for a larger value
      } else {
        right = mid - 1;
      }
    }

    return result >= 0 ? Optional.of(blockNumbers.get(result)) : Optional.empty();
  }

  /**
   * Serialize a list of block numbers to bytes. Uses a compact variable-length encoding.
   *
   * @param blockNumbers the list of block numbers to serialize
   * @return serialized bytes
   * @throws IOException if serialization fails
   */
  private byte[] serializeBlockNumbers(final List<Long> blockNumbers) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    // Write the count
    dos.writeInt(blockNumbers.size());

    // Write block numbers using delta encoding for better compression
    long previousBlock = 0;
    for (long blockNumber : blockNumbers) {
      long delta = blockNumber - previousBlock;
      dos.writeLong(delta);
      previousBlock = blockNumber;
    }

    dos.flush();
    return baos.toByteArray();
  }

  /**
   * Deserialize a list of block numbers from bytes.
   *
   * @param bytes the serialized bytes
   * @return list of block numbers
   * @throws IOException if deserialization fails
   */
  private List<Long> deserializeBlockNumbers(final byte[] bytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);

    // Read the count
    int count = dis.readInt();
    List<Long> blockNumbers = new ArrayList<>(count);

    // Read block numbers using delta decoding
    long previousBlock = 0;
    for (int i = 0; i < count; i++) {
      long delta = dis.readLong();
      long blockNumber = previousBlock + delta;
      blockNumbers.add(blockNumber);
      previousBlock = blockNumber;
    }

    return blockNumbers;
  }

  /**
   * Check if the index has been built for a given block range.
   *
   * @return true if the index has been built, false otherwise
   */
  public boolean isIndexBuilt() {
    // Check for a special marker key that indicates index build completion
    byte[] markerKey = "INDEX_BUILT".getBytes(UTF_8);
    return storage.get(ARCHIVE_STATE_INDEX, markerKey).isPresent();
  }

  /**
   * Mark the index as built.
   *
   * @param transaction the storage transaction to use
   * @param latestBlockNumber the latest block number indexed
   */
  public void markIndexBuilt(
      final SegmentedKeyValueStorageTransaction transaction, final long latestBlockNumber) {
    byte[] markerKey = "INDEX_BUILT".getBytes(UTF_8);
    byte[] markerValue = Bytes.ofUnsignedLong(latestBlockNumber).toArrayUnsafe();
    transaction.put(ARCHIVE_STATE_INDEX, markerKey, markerValue);
  }

  /**
   * Get the latest block number that has been indexed.
   *
   * @return Optional containing the latest indexed block number, or empty if not built
   */
  public Optional<Long> getLatestIndexedBlock() {
    byte[] markerKey = "INDEX_BUILT".getBytes(UTF_8);
    return storage.get(ARCHIVE_STATE_INDEX, markerKey).map(bytes -> Bytes.wrap(bytes).toLong());
  }

  /**
   * Clear all index data. Use with caution.
   */
  public void clear() {
    storage.clear(ARCHIVE_STATE_INDEX);
  }
}
