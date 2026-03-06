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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Hybrid flat DB strategy that routes reads based on block age and delegates writes to either the
 * Bonsai or Archive strategy.
 *
 * <p>Used as the active strategy in FlatDbMode.ARCHIVE after the archive migration completes.
 *
 * <p><b>Reads:</b> Recent blocks (target > HEAD - archiveBoundary) are served from the Bonsai layer
 * via O(1) direct [hash] lookup. Historical blocks use seekForPrev on [hash+blockN] keys via the
 * archive strategy.
 *
 * <p><b>Writes:</b> Non-context write methods (used for new block imports) delegate to the Bonsai
 * strategy and write simple [hash] keys. Explicit-context write methods delegate to the archive
 * strategy and write [hash+blockN] keys; these are used only by the migrator.
 */
public class BonsaiHybridFlatDbStrategy extends BonsaiFlatDbStrategy {

  private final BonsaiFullFlatDbStrategy bonsaiStrategy;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final LongSupplier headBlockSupplier;
  private final int archiveBoundary;

  public BonsaiHybridFlatDbStrategy(
      final BonsaiFullFlatDbStrategy bonsaiStrategy,
      final BonsaiArchiveFlatDbStrategy archiveStrategy,
      final LongSupplier headBlockSupplier,
      final int archiveBoundary,
      final CodeStorageStrategy codeStorageStrategy) {
    // Sub-strategies register and track their own metrics; use NoOp here to avoid duplicate
    // counter registration in the metrics system.
    super(new NoOpMetricsSystem(), codeStorageStrategy);
    this.bonsaiStrategy = bonsaiStrategy;
    this.archiveStrategy = archiveStrategy;
    this.headBlockSupplier = headBlockSupplier;
    this.archiveBoundary = archiveBoundary;
  }

  @VisibleForTesting
  boolean isRecentBlock(final long targetBlock, final long headBlock, final int boundary) {
    return targetBlock > headBlock - boundary;
  }

  @Override
  public Optional<Bytes> getFlatAccount(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final SegmentedKeyValueStorage storage) {
    final long targetBlock = getTargetBlock(storage);
    if (isRecentBlock(targetBlock, headBlockSupplier.getAsLong(), archiveBoundary)) {
      return bonsaiStrategy.getFlatAccount(
          worldStateRootHashSupplier, nodeLoader, accountHash, storage);
    }
    return archiveStrategy.getFlatAccount(
        worldStateRootHashSupplier, nodeLoader, accountHash, storage);
  }

  @Override
  public Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final Supplier<Optional<Hash>> storageRootSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey,
      final SegmentedKeyValueStorage storage) {
    final long targetBlock = getTargetBlock(storage);
    if (isRecentBlock(targetBlock, headBlockSupplier.getAsLong(), archiveBoundary)) {
      return bonsaiStrategy.getFlatStorageValueByStorageSlotKey(
          worldStateRootHashSupplier,
          storageRootSupplier,
          nodeLoader,
          accountHash,
          storageSlotKey,
          storage);
    }
    return archiveStrategy.getFlatStorageValueByStorageSlotKey(
        worldStateRootHashSupplier,
        storageRootSupplier,
        nodeLoader,
        accountHash,
        storageSlotKey,
        storage);
  }

  private Long getTargetBlock(final SegmentedKeyValueStorage storage) {
    return archiveStrategy
        .getStateArchiveContextForRead(storage)
        .flatMap(BonsaiContext::getBlockNumber)
        .orElseThrow(
            () -> new IllegalStateException("No block number in read context for hybrid strategy"));
  }

  @Override
  public void putFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes accountValue) {
    bonsaiStrategy.putFlatAccount(storage, transaction, accountHash, accountValue);
  }

  @Override
  public void removeFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash) {
    bonsaiStrategy.removeFlatAccount(storage, transaction, accountHash);
  }

  @Override
  public void putFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final Bytes storageValue) {
    bonsaiStrategy.putFlatAccountStorageValueByStorageSlotHash(
        storage, transaction, accountHash, slotHash, storageValue);
  }

  @Override
  public void removeFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash) {
    bonsaiStrategy.removeFlatAccountStorageValueByStorageSlotHash(
        storage, transaction, accountHash, slotHash);
  }

  // ======================== Context writes (migrator → Archive layer) ========================

  public void putFlatAccount(
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes accountValue) {
    archiveStrategy.putFlatAccount(context, transaction, accountHash, accountValue);
  }

  public void removeFlatAccount(
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash) {
    archiveStrategy.removeFlatAccount(context, transaction, accountHash);
  }

  public void putFlatAccountStorageValueByStorageSlotHash(
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final Bytes storageValue) {
    archiveStrategy.putFlatAccountStorageValueByStorageSlotHash(
        context, transaction, accountHash, slotHash, storageValue);
  }

  public void removeFlatAccountStorageValueByStorageSlotHash(
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash) {
    archiveStrategy.removeFlatAccountStorageValueByStorageSlotHash(
        context, transaction, accountHash, slotHash);
  }

  // ======================== Storage management ========================

  @Override
  public void clearAll(final SegmentedKeyValueStorage storage) {
    bonsaiStrategy.clearAll(storage);
  }

  @Override
  public void resetOnResync(final SegmentedKeyValueStorage storage) {
    bonsaiStrategy.resetOnResync(storage);
  }

  // ======================== Streaming (routes by block age, same as reads)
  // ========================

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash, final Bytes32 endKeyHash) {
    if (isRecentBlock(getTargetBlock(storage), headBlockSupplier.getAsLong(), archiveBoundary)) {
      return bonsaiStrategy.accountsToPairStream(storage, startKeyHash, endKeyHash);
    }
    return archiveStrategy.accountsToPairStream(storage, startKeyHash, endKeyHash);
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash) {
    if (isRecentBlock(getTargetBlock(storage), headBlockSupplier.getAsLong(), archiveBoundary)) {
      return bonsaiStrategy.accountsToPairStream(storage, startKeyHash);
    }
    return archiveStrategy.accountsToPairStream(storage, startKeyHash);
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    if (isRecentBlock(getTargetBlock(storage), headBlockSupplier.getAsLong(), archiveBoundary)) {
      return bonsaiStrategy.storageToPairStream(storage, accountHash, startKeyHash, valueMapper);
    }
    return archiveStrategy.storageToPairStream(storage, accountHash, startKeyHash, valueMapper);
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    if (isRecentBlock(getTargetBlock(storage), headBlockSupplier.getAsLong(), archiveBoundary)) {
      return bonsaiStrategy.storageToPairStream(
          storage, accountHash, startKeyHash, endKeyHash, valueMapper);
    }
    return archiveStrategy.storageToPairStream(
        storage, accountHash, startKeyHash, endKeyHash, valueMapper);
  }
}
