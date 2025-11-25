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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiArchiveStateIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.bouncycastle.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An indexed flat database strategy for Bonsai Archive that uses an index to perform O(1) lookups
 * instead of expensive seekForPrev operations.
 *
 * <p>This strategy maintains an index that tracks which blocks modified which accounts and storage
 * slots. When looking up historical state, it first queries the index to find the exact block
 * number where the state was last modified, then performs a direct O(1) get() operation.
 *
 * <p>This approach eliminates the need for seekForPrev operations and significantly improves query
 * performance, especially for frequently accessed historical state.
 */
public class BonsaiArchiveIndexedFlatDbStrategy extends BonsaiArchiveFlatDbStrategy {
  private static final Logger LOG =
      LoggerFactory.getLogger(BonsaiArchiveIndexedFlatDbStrategy.class);

  private final BonsaiArchiveStateIndex stateIndex;
  private final Counter indexHitCounter;
  private final Counter indexMissCounter;

  public BonsaiArchiveIndexedFlatDbStrategy(
      final MetricsSystem metricsSystem,
      final CodeStorageStrategy codeStorageStrategy,
      final BonsaiArchiveStateIndex stateIndex) {
    super(metricsSystem, codeStorageStrategy);
    this.stateIndex = stateIndex;

    this.indexHitCounter =
        metricsSystem.createCounter(
            org.hyperledger.besu.metrics.BesuMetricCategory.BLOCKCHAIN,
            "archive_index_hit_counter",
            "Total number of successful index lookups for archived state");

    this.indexMissCounter =
        metricsSystem.createCounter(
            org.hyperledger.besu.metrics.BesuMetricCategory.BLOCKCHAIN,
            "archive_index_miss_counter",
            "Total number of index misses that fell back to seekForPrev");
  }

  @Override
  public Optional<Bytes> getFlatAccount(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final SegmentedKeyValueStorage storage) {

    getAccountCounter.inc();

    // Get the block context for this query
    Optional<BonsaiContext> contextOpt = getStateArchiveContextForRead(storage);
    if (contextOpt.isEmpty()) {
      LOG.warn("No archive context available for account lookup");
      return Optional.empty();
    }

    long targetBlockNumber = contextOpt.get().getBlockNumber().orElse(Long.MAX_VALUE);

    // Try index-based lookup first
    Optional<Long> modificationBlock =
        stateIndex.findAccountModificationBlockNumber(accountHash, targetBlockNumber);

    if (modificationBlock.isPresent()) {
      // Index hit - perform direct O(1) lookup
      indexHitCounter.inc();

      long blockNumber = modificationBlock.get();
      byte[] key =
          calculateArchiveKeyWithMinSuffix(new BonsaiContext(blockNumber), accountHash.toArrayUnsafe());

      // Try primary segment first
      Optional<byte[]> value = storage.get(ACCOUNT_INFO_STATE, key);
      if (value.isPresent()) {
        getAccountFoundInFlatDatabaseCounter.inc();
        return filterDeletedValue(value.get(), DELETED_ACCOUNT_VALUE);
      }

      // Try archive segment
      value = storage.get(ACCOUNT_INFO_STATE_ARCHIVE, key);
      if (value.isPresent()) {
        getAccountFromArchiveCounter.inc();
        return filterDeletedValue(value.get(), DELETED_ACCOUNT_VALUE);
      }

      LOG.trace(
          "Index indicated block {} for account {} but value not found, falling back to seekForPrev",
          blockNumber,
          accountHash);
    } else {
      LOG.trace("No index entry found for account {}, falling back to seekForPrev", accountHash);
    }

    // Index miss - fall back to original seekForPrev logic
    indexMissCounter.inc();
    return super.getFlatAccount(worldStateRootHashSupplier, nodeLoader, accountHash, storage);
  }

  @Override
  public Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final Supplier<Optional<Hash>> storageRootSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey,
      final SegmentedKeyValueStorage storage) {

    getStorageValueCounter.inc();

    // Get the block context for this query
    Optional<BonsaiContext> contextOpt = getStateArchiveContextForRead(storage);
    if (contextOpt.isEmpty()) {
      LOG.warn("No archive context available for storage lookup");
      return Optional.empty();
    }

    long targetBlockNumber = contextOpt.get().getBlockNumber().orElse(Long.MAX_VALUE);

    // Try index-based lookup first
    Optional<Long> modificationBlock =
        stateIndex.findStorageModificationBlockNumber(
            accountHash, storageSlotKey, targetBlockNumber);

    if (modificationBlock.isPresent()) {
      // Index hit - perform direct O(1) lookup
      indexHitCounter.inc();

      long blockNumber = modificationBlock.get();
      byte[] naturalKey = calculateNaturalSlotKey(accountHash, storageSlotKey.getSlotHash());
      byte[] key = calculateArchiveKeyWithMinSuffix(new BonsaiContext(blockNumber), naturalKey);

      // Try primary segment first
      Optional<byte[]> value = storage.get(ACCOUNT_STORAGE_STORAGE, key);
      if (value.isPresent()) {
        getStorageValueFlatDatabaseCounter.inc();
        return filterDeletedValue(value.get(), DELETED_STORAGE_VALUE);
      }

      // Try archive segment
      value = storage.get(ACCOUNT_STORAGE_ARCHIVE, key);
      if (value.isPresent()) {
        getStorageFromArchiveCounter.inc();
        return filterDeletedValue(value.get(), DELETED_STORAGE_VALUE);
      }

      LOG.trace(
          "Index indicated block {} for storage {}/{} but value not found, falling back to seekForPrev",
          blockNumber,
          accountHash,
          storageSlotKey);
    } else {
      LOG.trace(
          "No index entry found for storage {}/{}, falling back to seekForPrev",
          accountHash,
          storageSlotKey);
    }

    // Index miss - fall back to original seekForPrev logic
    indexMissCounter.inc();
    return super.getFlatStorageValueByStorageSlotKey(
        worldStateRootHashSupplier,
        storageRootSupplier,
        nodeLoader,
        accountHash,
        storageSlotKey,
        storage);
  }

  /**
   * Filter out deleted value markers.
   *
   * @param value the value to check
   * @param deletedMarker the deleted marker to compare against
   * @return Optional containing the value if not deleted, empty otherwise
   */
  private Optional<Bytes> filterDeletedValue(final byte[] value, final byte[] deletedMarker) {
    if (Arrays.areEqual(value, deletedMarker)) {
      return Optional.empty();
    }
    return Optional.of(Bytes.wrap(value));
  }

}
