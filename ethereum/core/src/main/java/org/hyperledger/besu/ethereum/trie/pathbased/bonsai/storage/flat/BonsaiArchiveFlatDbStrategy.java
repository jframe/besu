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

  // Mutable context holders for archive operations - allows callers to explicitly set the block
  // context instead of relying on database state
  private final BonsaiContext writeContext = new BonsaiContext();
  private final BonsaiContext readContext = new BonsaiContext();

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

  private Optional<BonsaiContext> getStateArchiveContextForWrite() {
    // Use the held write context - callers must explicitly set this before write operations
    if (writeContext.getBlockNumber().isPresent()) {
      LOG.info(
          "[DIAG] getStateArchiveContextForWrite: using writeContext={}",
          writeContext.getBlockNumber().get());
      return Optional.of(writeContext);
    }

    // No context set - this is likely genesis block (block 0) or an error
    // For genesis, we use suffix 0
    LOG.info("[DIAG] getStateArchiveContextForWrite: no writeContext set, using context 0");
    return Optional.of(new BonsaiContext(0L));
  }

  /**
   * Sets the write context for archive operations. This context determines the block number suffix
   * used when writing entries to the archive flat DB.
   *
   * @param blockNumber the block number to use as the write context
   */
  public void setWriteContext(final long blockNumber) {
    LOG.info("[DIAG] setWriteContext: setting writeContext to {}", blockNumber);
    writeContext.setBlockNumber(blockNumber);
  }

  /** Clears the write context, reverting to default behavior (genesis block = 0). */
  public void clearWriteContext() {
    LOG.info("[DIAG] clearWriteContext: clearing writeContext");
    writeContext.setBlockNumber(null);
  }

  /**
   * Sets the read context for archive operations. This context determines the block number used
   * when searching for entries in the archive flat DB.
   *
   * @param blockNumber the block number to use as the read context
   */
  public void setReadContext(final long blockNumber) {
    LOG.info("[DIAG] setReadContext: setting readContext to {}", blockNumber);
    readContext.setBlockNumber(blockNumber);
  }

  /** Clears the read context, reverting to using MAX_BLOCK_SUFFIX for reads. */
  public void clearReadContext() {
    LOG.info("[DIAG] clearReadContext: clearing readContext");
    readContext.setBlockNumber(null);
  }

  /**
   * Checks if the write context is currently set.
   *
   * @return true if the write context has a block number set, false otherwise
   */
  public boolean hasWriteContext() {
    return writeContext.getBlockNumber().isPresent();
  }

  private Optional<BonsaiContext> getStateArchiveContextForRead() {
    // Use the held read context if set - callers can explicitly set this for historical queries
    if (readContext.getBlockNumber().isPresent()) {
      LOG.info(
          "[DIAG] getStateArchiveContextForRead: using readContext={}",
          readContext.getBlockNumber().get());
      return Optional.of(readContext);
    }

    // No context set - use empty which results in MAX_BLOCK_SUFFIX (most recent data)
    LOG.info("[DIAG] getStateArchiveContextForRead: no readContext set, using MAX_BLOCK_SUFFIX");
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getFlatAccount(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final SegmentedKeyValueStorage storage) {

    getAccountCounter.inc();
    Optional<SegmentedKeyValueStorage.NearestKeyValue> accountFound;

    // keyNearest, use MAX_BLOCK_SUFFIX in the absence of a block context:
    Bytes keyNearest =
        calculateArchiveKeyWithMaxSuffix(
            getStateArchiveContextForRead(), accountHash.toArrayUnsafe());

    // Find the nearest account state for this address and block context
    Optional<SegmentedKeyValueStorage.NearestKeyValue> nearestAccount =
        storage
            .getNearestBefore(ACCOUNT_INFO_STATE, keyNearest)
            .filter(found -> accountHash.commonPrefixLength(found.key()) >= accountHash.size());

    // If there isn't a match look in the archive DB segment
    if (nearestAccount.isEmpty()) {
      accountFound =
          storage
              .getNearestBefore(ACCOUNT_INFO_STATE_ARCHIVE, keyNearest)
              .filter(found -> accountHash.commonPrefixLength(found.key()) >= accountHash.size());

      if (accountFound.isPresent()) {
        getAccountFromArchiveCounter.inc();
      } else {
        // Fallback to non-archive lookup
        final Optional<Bytes> nonArchiveAccount =
            storage.get(ACCOUNT_INFO_STATE, accountHash.toArrayUnsafe()).map(Bytes::wrap);
        if (nonArchiveAccount.isPresent()) {
          getAccountFoundInFlatDatabaseCounter.inc();
          return nonArchiveAccount;
        }
        getAccountNotFoundInFlatDatabaseCounter.inc();
      }
    } else {

      accountFound = nearestAccount;
      getAccountFoundInFlatDatabaseCounter.inc();
    }

    if (accountFound.isPresent()) {
      // The entry exists (so metrics are still incremented) but we don't return deleted values
      return accountFound
          .filter(
              found ->
                  !Arrays.areEqual(
                      DELETED_ACCOUNT_VALUE, found.value().orElse(DELETED_ACCOUNT_VALUE)))
          // return empty when we find a "deleted value key"
          .flatMap(SegmentedKeyValueStorage.NearestKeyValue::wrapBytes);
    }

    return Optional.empty();
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
        .takeWhile(pair -> pair.slice(0, Hash.SIZE).equals(accountHash))
        .distinct()
        .map(
            key ->
                new Pair<>(
                    Bytes32.wrap(trimSuffix(key.slice(Hash.SIZE).toArrayUnsafe())),
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
        .takeWhile(pair -> pair.slice(0, Hash.SIZE).equals(accountHash))
        .distinct()
        .map(
            key ->
                new Pair<>(
                    Bytes32.wrap(trimSuffix(key.slice(Hash.SIZE).toArrayUnsafe())),
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
      final Bytes accountValue) {

    // key suffixed with block context, or MIN_BLOCK_SUFFIX if we have no context:
    final BonsaiContext context = getStateArchiveContextForWrite().get();
    byte[] keySuffixed = calculateArchiveKeyWithMinSuffix(context, accountHash.toArrayUnsafe());

    LOG.info(
        "[DIAG] putFlatAccount: writing account {} with value size {} at block suffix {}",
        accountHash.toShortHexString(),
        accountValue.size(),
        context.getBlockNumber().orElse(-1L));

    transaction.put(ACCOUNT_INFO_STATE, keySuffixed, accountValue.toArrayUnsafe());
  }

  @Override
  public void removeFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash) {

    // insert a key suffixed with block context, with 'deleted account' value
    final BonsaiContext context = getStateArchiveContextForWrite().get();
    byte[] keySuffixed = calculateArchiveKeyWithMinSuffix(context, accountHash.toArrayUnsafe());

    LOG.info(
        "[DIAG] removeFlatAccount: marking account {} as DELETED at block suffix {}",
        accountHash.toShortHexString(),
        context.getBlockNumber().orElse(-1L));

    transaction.put(ACCOUNT_INFO_STATE, keySuffixed, DELETED_ACCOUNT_VALUE);
  }

  /**
   * Removes a flat account with an explicit block context.
   *
   * @param transaction the transaction to write to
   * @param context the block context for versioning
   * @param accountHash the account hash
   */
  public void removeFlatAccountWithContext(
      final SegmentedKeyValueStorageTransaction transaction,
      final BonsaiContext context,
      final Hash accountHash) {

    LOG.info(
        "[DIAG] removeFlatAccountWithContext: marking account {} as DELETED at block suffix {}",
        accountHash.toShortHexString(),
        context.getBlockNumber().orElse(-1L));

    byte[] keySuffixed = calculateArchiveKeyWithMinSuffix(context, accountHash.toArrayUnsafe());
    transaction.put(ACCOUNT_INFO_STATE, keySuffixed, DELETED_ACCOUNT_VALUE);
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
      final SegmentedKeyValueStorage storage) {

    Optional<SegmentedKeyValueStorage.NearestKeyValue> storageFound;
    getStorageValueCounter.inc();

    // get natural key from account hash and slot key
    byte[] naturalKey = calculateNaturalSlotKey(accountHash, storageSlotKey.getSlotHash());
    // keyNearest, use MAX_BLOCK_SUFFIX in the absence of a block context:
    Bytes keyNearest =
        calculateArchiveKeyWithMaxSuffix(getStateArchiveContextForRead(), naturalKey);

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
        // Fallback to non-archive lookup
        final Optional<Bytes> nonArchiveStorage =
            storage.get(ACCOUNT_STORAGE_STORAGE, naturalKey).map(Bytes::wrap);
        if (nonArchiveStorage.isPresent()) {
          getStorageValueFlatDatabaseCounter.inc();
          return nonArchiveStorage;
        }
        getStorageValueNotFoundInFlatDatabaseCounter.inc();
      }
    } else {
      storageFound = nearestStorage;
      getStorageValueFlatDatabaseCounter.inc();
    }

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
      final Bytes storageValue) {

    // get natural key from account hash and slot key
    byte[] naturalKey = calculateNaturalSlotKey(accountHash, slotHash);
    // keyNearest, use MIN_BLOCK_SUFFIX in the absence of a block context:
    final BonsaiContext context = getStateArchiveContextForWrite().get();
    byte[] keyNearest = calculateArchiveKeyWithMinSuffix(context, naturalKey);

    LOG.info(
        "[DIAG] putFlatAccountStorageValueByStorageSlotHash: writing storage slot {} for account {} with value {} at block suffix {}",
        slotHash.toShortHexString(),
        accountHash.toShortHexString(),
        storageValue.toShortHexString(),
        context.getBlockNumber().orElse(-1L));

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
      final Hash slotHash) {

    // get natural key from account hash and slot key
    byte[] naturalKey = calculateNaturalSlotKey(accountHash, slotHash);
    // insert a key suffixed with block context, with 'deleted account' value
    final BonsaiContext context = getStateArchiveContextForWrite().get();
    byte[] keySuffixed = calculateArchiveKeyWithMinSuffix(context, naturalKey);

    LOG.info(
        "[DIAG] removeFlatAccountStorageValueByStorageSlotHash: marking storage slot {} for account {} as DELETED at block suffix {}",
        slotHash.toShortHexString(),
        accountHash.toShortHexString(),
        context.getBlockNumber().orElse(-1L));

    transaction.put(ACCOUNT_STORAGE_STORAGE, keySuffixed, DELETED_STORAGE_VALUE);
  }

  /**
   * Removes a flat account storage value with an explicit block context.
   *
   * @param transaction the transaction to write to
   * @param context the block context for versioning
   * @param accountHash the account hash
   * @param slotHash the storage slot hash
   */
  public void removeFlatAccountStorageValueByStorageSlotHashWithContext(
      final SegmentedKeyValueStorageTransaction transaction,
      final BonsaiContext context,
      final Hash accountHash,
      final Hash slotHash) {

    LOG.info(
        "[DIAG] removeFlatAccountStorageValueByStorageSlotHashWithContext: marking storage slot {} for account {} as DELETED at block suffix {}",
        slotHash.toShortHexString(),
        accountHash.toShortHexString(),
        context.getBlockNumber().orElse(-1L));

    byte[] naturalKey = calculateNaturalSlotKey(accountHash, slotHash);
    byte[] keySuffixed = calculateArchiveKeyWithMinSuffix(context, naturalKey);
    transaction.put(ACCOUNT_STORAGE_STORAGE, keySuffixed, DELETED_STORAGE_VALUE);
  }

  public static byte[] calculateNaturalSlotKey(final Hash accountHash, final Hash slotHash) {
    return Bytes.concatenate(accountHash, slotHash).toArrayUnsafe();
  }

  public static byte[] calculateArchiveKeyWithMinSuffix(
      final BonsaiContext context, final byte[] naturalKey) {
    return calculateArchiveKeyWithSuffix(Optional.of(context), naturalKey, MIN_BLOCK_SUFFIX);
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

  /**
   * Writes an account value to the archive storage with an explicit block context.
   *
   * @param transaction the transaction to write to
   * @param context the block context for versioning
   * @param accountHash the account hash
   * @param accountValue the serialized account value
   */
  public static void putFlatAccountWithContext(
      final SegmentedKeyValueStorageTransaction transaction,
      final BonsaiContext context,
      final Hash accountHash,
      final Bytes accountValue) {
    byte[] archiveKey = calculateArchiveKeyWithMinSuffix(context, accountHash.toArrayUnsafe());
    transaction.put(ACCOUNT_INFO_STATE, archiveKey, accountValue.toArrayUnsafe());
  }

  /**
   * Writes a storage value to the archive storage with an explicit block context.
   *
   * @param transaction the transaction to write to
   * @param context the block context for versioning
   * @param accountHash the account hash
   * @param slotHash the storage slot hash
   * @param storageValue the storage value
   */
  public static void putFlatAccountStorageValueWithContext(
      final SegmentedKeyValueStorageTransaction transaction,
      final BonsaiContext context,
      final Hash accountHash,
      final Hash slotHash,
      final Bytes storageValue) {
    byte[] naturalKey = calculateNaturalSlotKey(accountHash, slotHash);
    byte[] archiveKey = calculateArchiveKeyWithMinSuffix(context, naturalKey);
    transaction.put(ACCOUNT_STORAGE_STORAGE, archiveKey, storageValue.toArrayUnsafe());
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
