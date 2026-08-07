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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeKey;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * FULL-only inline archive capture. On each trie-node put, delegates the live write to the base
 * strategy, then (if the capture gate is open for the current block) writes the full node RLP into
 * the SAME transaction. No prior-node read, no diff, no pool, no tombstones (roadmap §2.2). The
 * gate is initial-sync-only in PR1: supplier returns MAX_VALUE while initial-syncing, MIN_VALUE
 * otherwise; block 0 (genesis) is always captured.
 *
 * <p><strong>Tx-ownership guard (fix 657cf447d9):</strong> the strategy instance is shared by every
 * {@code Updater} on the same storage. {@code TrieLogManager.saveTrieLog} opens a second updater
 * mid-block and calls {@code commitTrieLogOnly()} which triggers {@code onDiscard}. Without the
 * guard that foreign call would wipe the import block's capture state, leaving the archive silently
 * un-advanced on every block.
 */
public class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeStrategy base;
  private final ArchiveNodeHistoryStore historyStore;
  private final ArchiveNodeHistoryProgress historyProgress;
  private volatile LongSupplier highestSafeBlockSupplier;

  private long cachedBlockNumber = Long.MIN_VALUE;
  private boolean blockNumberCached = false;
  private long capturedBlock = Long.MIN_VALUE;
  // Identity of the transaction whose puts filled the capture state. Flush/discard from any
  // non-owning transaction are ignored (fix 657cf447d9).
  private SegmentedKeyValueStorageTransaction owningTransaction;

  public BonsaiArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveNodeHistoryProgress historyProgress,
      final LongSupplier highestSafeBlockSupplier) {
    this.base = Objects.requireNonNull(base);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.historyProgress = Objects.requireNonNull(historyProgress);
    this.highestSafeBlockSupplier = Objects.requireNonNull(highestSafeBlockSupplier);
  }

  public void setHighestSafeBlockSupplier(final LongSupplier supplier) {
    this.highestSafeBlockSupplier = Objects.requireNonNull(supplier);
  }

  private boolean shouldCapture(final long block) {
    return block == 0L || block <= highestSafeBlockSupplier.getAsLong();
  }

  private long currentBlockNumber(final SegmentedKeyValueStorage storage) {
    if (!blockNumberCached) {
      cachedBlockNumber =
          storage
              .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
              .map(b -> Bytes.wrap(b).toLong() + 1L)
              .orElse(0L);
      blockNumberCached = true;
    }
    return cachedBlockNumber;
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return base.getFlatAccountTrieNode(location, nodeHash, storage);
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return base.getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    base.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    final long block = currentBlockNumber(storage);
    if (shouldCapture(block)) {
      historyStore.put(transaction, ArchiveNodeKey.account(location), block, node);
      capturedBlock = block;
      owningTransaction = transaction;
    }
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    base.putFlatStorageTrieNode(storage, transaction, accountHash, location, nodeHash, node);
    final long block = currentBlockNumber(storage);
    if (shouldCapture(block)) {
      historyStore.put(
          transaction, ArchiveNodeKey.storage(accountHash.getBytes(), location), block, node);
      capturedBlock = block;
      owningTransaction = transaction;
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    // FULL-only: removals are not captured (roadmap §2.2). Live delete only.
    base.removeFlatAccountStateTrieNode(storage, transaction, location);
  }

  @Override
  public void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {
    if (owningTransaction != null && owningTransaction != transaction) {
      return; // another updater committing its own transaction — not ours to flush (657cf447d9)
    }
    blockNumberCached = false; // this commit advances WORLD_BLOCK_NUMBER_KEY
    if (capturedBlock == Long.MIN_VALUE) {
      return;
    }
    historyProgress.setIndexStartBlock(capturedBlock);
    historyProgress.setLastIndexedBlock(capturedBlock);
    historyProgress.save(transaction);
    capturedBlock = Long.MIN_VALUE;
    owningTransaction = null;
  }

  @Override
  public void onDiscard(final SegmentedKeyValueStorageTransaction transaction) {
    if (owningTransaction != null && owningTransaction != transaction) {
      return; // another updater's rollback / trie-log-only commit — not ours to discard
      // (657cf447d9)
    }
    blockNumberCached = false;
    capturedBlock = Long.MIN_VALUE;
    owningTransaction = null;
  }
}
