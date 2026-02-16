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
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveFlatDbStrategy extends BonsaiFullFlatDbStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveFlatDbStrategy.class);

  protected final Counter getAccountFromArchiveCounter;
  protected final Counter getStorageFromArchiveCounter;

  public BonsaiArchiveFlatDbStrategy(
      final MetricsSystem metricsSystem, final CodeStorageStrategy codeStorageStrategy) {
    super(metricsSystem, codeStorageStrategy);

    getAccountFromArchiveCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_from_archive_counter",
            "Total number of calls to get account that were from archived state");

    getStorageFromArchiveCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storage_from_archive_counter",
            "Total number of calls to get storage that were from archived state");
  }

  static final byte[] MAX_BLOCK_SUFFIX = Bytes.ofUnsignedLong(Long.MAX_VALUE).toArrayUnsafe();
  static final byte[] MIN_BLOCK_SUFFIX = Bytes.ofUnsignedLong(0L).toArrayUnsafe();
  public static final byte[] DELETED_ACCOUNT_VALUE = new byte[0];
  public static final byte[] DELETED_STORAGE_VALUE = new byte[0];

  @Override
  public Optional<Bytes> getFlatAccount(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final SegmentedKeyValueStorage storage,
      final Supplier<Optional<BonsaiContext>> readContextSupplier) {

    getAccountCounter.inc();

    // Get read context - if not available, use MAX_SUFFIX to get latest value
    // This handles cases like snapshot storage where we want the most recent value
    Optional<BonsaiContext> readContext = readContextSupplier.get();

    // keyNearest, use MAX_BLOCK_SUFFIX when no block context is available:
    Bytes keyNearest =
        calculateArchiveKeyWithMaxSuffix(readContext, accountHash.getBytes().toArrayUnsafe());

    // Find the nearest account state for this address and block context
    // First try the primary segment, then fall back to archive segment
    Optional<SegmentedKeyValueStorage.NearestKeyValue> nearestAccount =
        storage
            .getNearestBefore(ACCOUNT_INFO_STATE, keyNearest)
            .filter(
                found ->
                    accountHash.getBytes().commonPrefixLength(found.key())
                        >= accountHash.getBytes().size());

    if (nearestAccount.isPresent()) {
      getAccountFoundInFlatDatabaseCounter.inc();
    } else {
      // Try archive DB segment
      nearestAccount =
          storage
              .getNearestBefore(ACCOUNT_INFO_STATE_ARCHIVE, keyNearest)
              .filter(
                  found ->
                      accountHash.getBytes().commonPrefixLength(found.key())
                          >= accountHash.getBytes().size());
      if (nearestAccount.isPresent()) {
        getAccountFromArchiveCounter.inc();
      } else {
        getAccountNotFoundInFlatDatabaseCounter.inc();
      }
    }

    // Check if the found entry is a deletion marker
    // A deletion marker means the account was deleted at that block - return empty
    Optional<SegmentedKeyValueStorage.NearestKeyValue> accountFound = nearestAccount;
    if (nearestAccount.isPresent()
        && Arrays.areEqual(
            DELETED_ACCOUNT_VALUE, nearestAccount.get().value().orElse(DELETED_ACCOUNT_VALUE))) {
      // This is a deletion marker - account doesn't exist at this context
      accountFound = Optional.empty();
    }

    LOG.info(
        "getFlatAccount: hash={}, readContext={}, found={}, value={}",
        accountHash,
        readContext,
        accountFound.isPresent(),
        accountFound
            .flatMap(SegmentedKeyValueStorage.NearestKeyValue::value)
            .map(Bytes::of)
            .map(Bytes::toHexString)
            .orElse("empty"));

    return accountFound.flatMap(SegmentedKeyValueStorage.NearestKeyValue::wrapBytes);
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash, final Bytes32 endKeyHash) {
    final Stream<Pair<Bytes32, Bytes>> stream =
        storage
            .streamFromKey(
                ACCOUNT_INFO_STATE,
                calculateArchiveKeyNoContextMinSuffix(startKeyHash.toArrayUnsafe()),
                calculateArchiveKeyNoContextMaxSuffix(endKeyHash.toArrayUnsafe()))
            .map(e -> Bytes.of(calculateArchiveKeyNoContextMaxSuffix(trimSuffix(e.getKey()))))
            .distinct()
            .map(
                e ->
                    new Pair<>(
                        Bytes32.wrap(trimSuffix(e.toArrayUnsafe())),
                        Bytes.of(
                            storage.getNearestBefore(ACCOUNT_INFO_STATE, e).get().value().get())));
    return stream;
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash) {
    final Stream<Pair<Bytes32, Bytes>> stream =
        storage
            .streamFromKey(
                ACCOUNT_INFO_STATE,
                calculateArchiveKeyNoContextMinSuffix(startKeyHash.toArrayUnsafe()))
            .map(e -> Bytes.of(calculateArchiveKeyNoContextMaxSuffix(trimSuffix(e.getKey()))))
            .distinct()
            .map(
                e ->
                    new Pair<Bytes32, Bytes>(
                        Bytes32.wrap(trimSuffix(e.toArrayUnsafe())),
                        Bytes.of(
                            storage.getNearestBefore(ACCOUNT_INFO_STATE, e).get().value().get())));
    return stream;
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    return storage
        .streamFromKey(
            ACCOUNT_STORAGE_STORAGE,
            calculateArchiveKeyNoContextMinSuffix(
                calculateNaturalSlotKey(accountHash, Hash.wrap(Bytes32.wrap(startKeyHash)))))
        .map(e -> Bytes.of(calculateArchiveKeyNoContextMaxSuffix(trimSuffix(e.getKey()))))
        .takeWhile(pair -> pair.slice(0, Bytes32.SIZE).equals(accountHash.getBytes()))
        .distinct()
        .map(
            key ->
                new Pair<>(
                    Bytes32.wrap(trimSuffix(key.slice(Bytes32.SIZE).toArrayUnsafe())),
                    valueMapper.apply(
                        Bytes.of(
                                storage
                                    .getNearestBefore(ACCOUNT_STORAGE_STORAGE, key)
                                    .get()
                                    .value()
                                    .get())
                            .trimLeadingZeros())));
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    return storage
        .streamFromKey(
            ACCOUNT_STORAGE_STORAGE,
            calculateArchiveKeyNoContextMinSuffix(
                calculateNaturalSlotKey(accountHash, Hash.wrap(Bytes32.wrap(startKeyHash)))),
            calculateArchiveKeyNoContextMaxSuffix(
                calculateNaturalSlotKey(accountHash, Hash.wrap(endKeyHash))))
        .map(e -> Bytes.of(calculateArchiveKeyNoContextMaxSuffix(trimSuffix(e.getKey()))))
        .takeWhile(pair -> pair.slice(0, Bytes32.SIZE).equals(accountHash.getBytes()))
        .distinct()
        .map(
            key ->
                new Pair<>(
                    Bytes32.wrap(trimSuffix(key.slice(Bytes32.SIZE).toArrayUnsafe())),
                    valueMapper.apply(
                        Bytes.of(
                                storage
                                    .getNearestBefore(ACCOUNT_STORAGE_STORAGE, key)
                                    .get()
                                    .value()
                                    .get())
                            .trimLeadingZeros())));
  }

  /*
   * Puts the account data for the given account hash and block context.
   */
  @Override
  public void putFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes accountValue,
      final Supplier<Optional<BonsaiContext>> writeContextSupplier) {

    // Get write context or default to genesis
    BonsaiContext writeContext = writeContextSupplier.get().orElse(new BonsaiContext(0L));

    // key suffixed with block context, or MIN_BLOCK_SUFFIX if we have no context:
    byte[] keySuffixed =
        calculateArchiveKeyWithMinSuffix(
            Optional.of(writeContext), accountHash.getBytes().toArrayUnsafe());

    LOG.info(
        "putFlatAccount: hash={}, writeContext={}, value={}",
        accountHash,
        writeContext,
        accountValue);

    transaction.put(ACCOUNT_INFO_STATE, keySuffixed, accountValue.toArrayUnsafe());
  }

  @Override
  public void removeFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Supplier<Optional<BonsaiContext>> writeContextSupplier) {

    // Get write context or default to genesis
    BonsaiContext writeContext = writeContextSupplier.get().orElse(new BonsaiContext(0L));

    // insert a key suffixed with block context, with 'deleted account' value
    byte[] keySuffixed =
        calculateArchiveKeyWithMinSuffix(
            Optional.of(writeContext), accountHash.getBytes().toArrayUnsafe());

    LOG.info("removeFlatAccount: hash={}, writeContext={}", accountHash, writeContext);

    transaction.put(ACCOUNT_INFO_STATE, keySuffixed, DELETED_ACCOUNT_VALUE);
  }

  /**
   * Deletes an orphaned account entry at the given block context. Used during archive mode rollback
   * to remove stale chain data that would otherwise mask valid data at earlier block numbers.
   *
   * <p>Unlike {@link #removeFlatAccount} which writes a deletion marker, this method actually
   * removes the key-value entry from the database. This is necessary during reorg handling to
   * prevent orphaned data from the old chain from masking valid data from the new chain.
   *
   * @param transaction the storage transaction
   * @param accountHash the hash of the account to delete
   * @param blockNumber the block number context of the orphaned entry
   */
  public void deleteFlatAccountAtBlock(
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final long blockNumber) {

    byte[] keySuffixed =
        calculateArchiveKeyWithMinSuffix(
            Optional.of(new BonsaiContext(blockNumber)), accountHash.getBytes().toArrayUnsafe());

    LOG.info("deleteFlatAccountAtBlock: hash={}, blockNumber={}", accountHash, blockNumber);

    transaction.remove(ACCOUNT_INFO_STATE, keySuffixed);
  }

  private byte[] trimSuffix(final byte[] suffixedAddress) {
    return Arrays.copyOfRange(suffixedAddress, 0, suffixedAddress.length - 8);
  }

  /*
   * Retrieves the storage value for the given account hash and storage slot key, using the world state root hash supplier, storage root supplier, and node loader.
   */
  @Override
  public Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final Supplier<Optional<Hash>> storageRootSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey,
      final SegmentedKeyValueStorage storage,
      final Supplier<Optional<BonsaiContext>> readContextSupplier) {

    Optional<SegmentedKeyValueStorage.NearestKeyValue> storageFound;
    getStorageValueCounter.inc();

    // Get read context - if not available, use MAX_SUFFIX to get latest value
    // This handles cases like snapshot storage where we want the most recent value
    Optional<BonsaiContext> readContext = readContextSupplier.get();

    // get natural key from account hash and slot key
    byte[] naturalKey = calculateNaturalSlotKey(accountHash, storageSlotKey.getSlotHash());
    // keyNearest, use MAX_BLOCK_SUFFIX when no block context is available:
    Bytes keyNearest = calculateArchiveKeyWithMaxSuffix(readContext, naturalKey);

    // Find the nearest storage for this address, slot key hash, and block context
    Optional<SegmentedKeyValueStorage.NearestKeyValue> nearestStorage =
        storage
            .getNearestBefore(ACCOUNT_STORAGE_STORAGE, keyNearest)
            .filter(
                found -> Bytes.of(naturalKey).commonPrefixLength(found.key()) >= naturalKey.length);

    // If there isn't a match look in the archive DB segment
    if (nearestStorage.isEmpty()) {
      // Check the archived storage as old state is moved out of the primary DB segment
      storageFound =
          storage
              .getNearestBefore(ACCOUNT_STORAGE_ARCHIVE, keyNearest)
              // don't return accounts that do not have a matching account hash
              .filter(
                  found ->
                      Bytes.of(naturalKey).commonPrefixLength(found.key()) >= naturalKey.length);

      if (storageFound.isPresent()) {
        getStorageFromArchiveCounter.inc();
      } else {
        getStorageValueNotFoundInFlatDatabaseCounter.inc();
      }
    } else {
      storageFound = nearestStorage;
      getStorageValueFlatDatabaseCounter.inc();
    }

    LOG.info(
        "getFlatStorageValueByStorageSlotKey: hash={}, readContext={}, found={}, value={}",
        accountHash,
        readContext,
        storageFound.isPresent(),
        storageFound
            .flatMap(SegmentedKeyValueStorage.NearestKeyValue::value)
            .map(Bytes::of)
            .map(Bytes::toHexString)
            .orElse("empty"));

    // The entry exists (so metrics are still incremented) but we don't return deleted values
    if (storageFound.isPresent()) {
      return storageFound
          // return empty when we find a "deleted value key"
          .filter(
              found ->
                  !Arrays.areEqual(
                      DELETED_STORAGE_VALUE, found.value().orElse(DELETED_STORAGE_VALUE)))
          // map NearestKey to Bytes-wrapped value
          .flatMap(SegmentedKeyValueStorage.NearestKeyValue::wrapBytes);
    }

    return Optional.empty();
  }

  /*
   * Puts the storage value for the given account hash and storage slot key, using the world state root hash supplier, storage root supplier, and node loader.
   */
  @Override
  public void putFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final Bytes storageValue,
      final Supplier<Optional<BonsaiContext>> writeContextSupplier) {

    // Get write context or default to genesis
    BonsaiContext writeContext = writeContextSupplier.get().orElse(new BonsaiContext(0L));

    // get natural key from account hash and slot key
    byte[] naturalKey = calculateNaturalSlotKey(accountHash, slotHash);
    // keyNearest, use MIN_BLOCK_SUFFIX in the absence of a block context:
    byte[] keyNearest = calculateArchiveKeyWithMinSuffix(Optional.of(writeContext), naturalKey);

    transaction.put(ACCOUNT_STORAGE_STORAGE, keyNearest, storageValue.toArrayUnsafe());
  }

  /*
   * Removes the storage value for the given account hash and storage slot key, using the world state root hash supplier, storage root supplier, and node loader.
   */
  @Override
  public void removeFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final Supplier<Optional<BonsaiContext>> writeContextSupplier) {

    // Get write context or default to genesis
    BonsaiContext writeContext = writeContextSupplier.get().orElse(new BonsaiContext(0L));

    // get natural key from account hash and slot key
    byte[] naturalKey = calculateNaturalSlotKey(accountHash, slotHash);
    // insert a key suffixed with block context, with 'deleted account' value
    byte[] keySuffixed = calculateArchiveKeyWithMinSuffix(Optional.of(writeContext), naturalKey);

    LOG.info(
        "removeFlatAccountStorageValueByStorageSlotHash: hash={}, writeContext={}",
        accountHash,
        writeContext);

    transaction.put(ACCOUNT_STORAGE_STORAGE, keySuffixed, DELETED_STORAGE_VALUE);
  }

  /**
   * Deletes an orphaned storage entry at the given block context. Used during archive mode rollback
   * to remove stale chain data that would otherwise mask valid data at earlier block numbers.
   *
   * @param transaction the storage transaction
   * @param accountHash the hash of the account
   * @param slotHash the hash of the storage slot to delete
   * @param blockNumber the block number context of the orphaned entry
   */
  public void deleteFlatStorageAtBlock(
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final long blockNumber) {

    byte[] naturalKey = calculateNaturalSlotKey(accountHash, slotHash);
    byte[] keySuffixed =
        calculateArchiveKeyWithMinSuffix(Optional.of(new BonsaiContext(blockNumber)), naturalKey);

    LOG.info(
        "deleteFlatStorageAtBlock: accountHash={}, slotHash={}, blockNumber={}",
        accountHash,
        slotHash,
        blockNumber);

    transaction.remove(ACCOUNT_STORAGE_STORAGE, keySuffixed);
  }

  /**
   * Checks if legitimate (non-marker) account data exists before the given block number. Used by
   * smart detection to decide between deletion (reveal history) vs marker (barrier).
   *
   * <p>Package-private for testing.
   *
   * @param storage the key-value storage
   * @param accountHash the account hash to check
   * @param blockNumber the block number to search before
   * @return true if non-marker data exists at any block < blockNumber
   */
  boolean hasHistoricalAccountDataBefore(
      final SegmentedKeyValueStorage storage, final Hash accountHash, final long blockNumber) {

    if (blockNumber == 0) {
      return false; // No blocks before genesis
    }

    // Reuse getFlatAccount with readContext = blockNumber - 1
    // This searches for the nearest non-marker entry before blockNumber
    Supplier<Optional<BonsaiContext>> readContext =
        () -> Optional.of(new BonsaiContext(blockNumber - 1));

    Optional<Bytes> historicalData =
        getFlatAccount(
            () -> Optional.empty(), // worldStateRootHash not needed for this check
            null, // nodeLoader not needed
            accountHash,
            storage,
            readContext);

    return historicalData.isPresent();
  }

  /**
   * Checks if legitimate (non-marker) storage data exists before the given block number. Used by
   * smart detection to decide between deletion (reveal history) vs marker (barrier).
   *
   * <p>Package-private for testing.
   *
   * @param storage the key-value storage
   * @param accountHash the account hash
   * @param slotHash the storage slot hash
   * @param blockNumber the block number to search before
   * @return true if non-marker data exists at any block < blockNumber
   */
  boolean hasHistoricalStorageDataBefore(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Hash slotHash,
      final long blockNumber) {

    if (blockNumber == 0) {
      return false; // No blocks before genesis
    }

    Supplier<Optional<BonsaiContext>> readContext =
        () -> Optional.of(new BonsaiContext(blockNumber - 1));

    Optional<Bytes> historicalData =
        getFlatStorageValueByStorageSlotKey(
            () -> Optional.empty(), // worldStateRootHash not needed for this check
            () -> Optional.empty(), // storageRoot not needed
            null, // nodeLoader not needed
            accountHash,
            new StorageSlotKey(slotHash, Optional.empty()),
            storage,
            readContext);

    return historicalData.isPresent();
  }

  public static byte[] calculateNaturalSlotKey(final Hash accountHash, final Hash slotHash) {
    return Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe();
  }

  public static byte[] calculateArchiveKeyWithMinSuffix(
      final Optional<BonsaiContext> context, final byte[] naturalKey) {
    return calculateArchiveKeyWithSuffix(context, naturalKey, MIN_BLOCK_SUFFIX);
  }

  public static byte[] calculateArchiveKeyNoContextMinSuffix(final byte[] naturalKey) {
    return Arrays.concatenate(naturalKey, MIN_BLOCK_SUFFIX);
  }

  public static byte[] calculateArchiveKeyNoContextMaxSuffix(final byte[] naturalKey) {
    return Arrays.concatenate(naturalKey, MAX_BLOCK_SUFFIX);
  }

  public static Bytes calculateArchiveKeyWithMaxSuffix(
      final Optional<BonsaiContext> context, final byte[] naturalKey) {
    return Bytes.of(calculateArchiveKeyWithSuffix(context, naturalKey, MAX_BLOCK_SUFFIX));
  }

  // TODO JF: move this out of this class so can be used with ArchiveCodeStorageStrategy without
  // being static
  public static byte[] calculateArchiveKeyWithSuffix(
      final Optional<BonsaiContext> context, final byte[] naturalKey, final byte[] orElseSuffix) {
    // TODO: this can be optimized, just for PoC now
    return Arrays.concatenate(
        naturalKey,
        context
            .flatMap(BonsaiContext::getBlockNumber)
            .map(Bytes::ofUnsignedLong)
            .map(Bytes::toArrayUnsafe)
            .orElseGet(
                () -> {
                  // TODO: remove or rate limit these warnings
                  LOG.atWarn().setMessage("Block context not present, using default suffix").log();
                  return orElseSuffix;
                }));
  }
}
