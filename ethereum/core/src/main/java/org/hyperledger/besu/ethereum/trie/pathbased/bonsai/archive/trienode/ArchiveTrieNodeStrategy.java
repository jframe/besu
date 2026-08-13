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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link TrieNodeStrategy} that archives every trie-node write into {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE} so historical {@code eth_getProof} requests don't need trie-log
 * replay.
 *
 * <p>Each put delegates to {@code base} (live flat DB) first, then — if the archive gate is open —
 * writes an {@link ArchiveTrieNodeCodec} entry keyed by {@link ArchiveNodeKey} in the same
 * transaction: a DIFF against the prior version, a FULL entry every {@link
 * ArchiveHistoryReader#CHECKPOINT_INTERVAL}th write to bound the reader's backward walk, or a
 * DELETION tombstone on removal. Progress is recorded once per transaction.
 *
 * <p>The diff base is read from committed {@code storage} (still block N-1 while block N's {@code
 * transaction} is in flight). An archiving gap — gate closed then reopened, or a restart — forces
 * the next block to write FULL, since the newest archive entry no longer matches the flat DB.
 *
 * <p>The gate is {@code true} while behind the network head ({@code !syncState.isInSync()}),
 * keeping live reorg-window blocks out of the archive; block 0 is always archived regardless.
 */
public class ArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private final TrieNodeStrategy base;
  private final ArchiveNodeHistoryStore historyStore;
  private final ArchiveNodeHistoryProgress historyProgress;
  private final BooleanSupplier archiveGate;

  private SegmentedKeyValueStorageTransaction lastRecordedTx;
  private long lastArchivedBlock = -1L;
  private boolean chainContiguous;
  private final Object archiveStateLock = new Object();

  public ArchiveTrieNodeStrategy(
      final TrieNodeStrategy base,
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveNodeHistoryProgress historyProgress,
      final BooleanSupplier archiveGate) {
    this.base = Objects.requireNonNull(base);
    this.historyStore = Objects.requireNonNull(historyStore);
    this.historyProgress = Objects.requireNonNull(historyProgress);
    this.archiveGate = Objects.requireNonNull(archiveGate);
  }

  private boolean shouldArchive(final long block) {
    return block == 0L || archiveGate.getAsBoolean();
  }

  private long currentBlockNumber(final SegmentedKeyValueStorage storage) {
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(b -> Bytes.wrap(b).toLong() + 1L)
        .orElse(0L);
  }

  private void beginArchiveBlock(
      final SegmentedKeyValueStorageTransaction transaction, final long block) {
    if (lastRecordedTx != transaction) {
      chainContiguous = block == lastArchivedBlock + 1L;
      historyProgress.record(transaction, block);
      lastRecordedTx = transaction;
      lastArchivedBlock = block;
    }
  }

  private Bytes encodeNodeWrite(
      final Bytes naturalKey,
      final Bytes location,
      final Bytes priorFlat,
      final Bytes node,
      final long block) {
    if (priorFlat == null) {
      // Node created at this block: encodeDiff emits an ENTRY_FULL | CREATION entry.
      return ArchiveNodeHistoryStore.encodeStoredValue(
          0, ArchiveTrieNodeCodec.encodeDiff(null, node));
    }
    if (location.isEmpty() || block == 0L || !chainContiguous) {
      return ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(node));
    }
    final Optional<ArchiveNodeHistoryStore.HistoryEntry> prior =
        historyStore.getLatestBefore(naturalKey, block - 1L);
    if (prior.isEmpty() || prior.get().codecEntry().isDeletion()) {
      return ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(node));
    }
    final int counter = prior.get().counter() + 1;
    if (counter >= ArchiveHistoryReader.CHECKPOINT_INTERVAL) {
      return ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(node));
    }
    return ArchiveNodeHistoryStore.encodeStoredValue(
        counter, ArchiveTrieNodeCodec.encodeDiff(priorFlat, node));
  }

  private void archiveNodeWrite(
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes naturalKey,
      final Bytes location,
      final Bytes priorFlat,
      final Bytes node,
      final long block) {
    final Bytes archiveValue;
    synchronized (archiveStateLock) {
      beginArchiveBlock(transaction, block);
      archiveValue = encodeNodeWrite(naturalKey, location, priorFlat, node, block);
    }
    historyStore.putEncoded(
        transaction, ArchiveNodeKey.historyKey(naturalKey, block), archiveValue);
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
    final long block = currentBlockNumber(storage);
    final boolean archiving = shouldArchive(block);
    final Bytes priorFlat =
        archiving ? base.getFlatAccountTrieNode(location, nodeHash, storage).orElse(null) : null;
    base.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (archiving) {
      final Bytes naturalKey = ArchiveNodeKey.account(location);
      archiveNodeWrite(transaction, naturalKey, location, priorFlat, node, block);
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
    final long block = currentBlockNumber(storage);
    final boolean archiving = shouldArchive(block);
    final Bytes priorFlat =
        archiving
            ? base.getFlatStorageTrieNode(accountHash, location, nodeHash, storage).orElse(null)
            : null;
    base.putFlatStorageTrieNode(storage, transaction, accountHash, location, nodeHash, node);
    if (archiving) {
      final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), location);
      archiveNodeWrite(transaction, naturalKey, location, priorFlat, node, block);
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    final long block = currentBlockNumber(storage);
    // Read before delegating: base.remove() clears the value we need to know existed.
    final Optional<Bytes> priorFlat =
        shouldArchive(block)
            ? base.getFlatAccountTrieNode(location, Bytes32.ZERO, storage)
            : Optional.empty();
    base.removeFlatAccountStateTrieNode(storage, transaction, location);
    if (priorFlat.isPresent()) {
      final Bytes naturalKey = ArchiveNodeKey.account(location);
      synchronized (archiveStateLock) {
        beginArchiveBlock(transaction, block);
      }
      historyStore.putEncoded(
          transaction,
          ArchiveNodeKey.historyKey(naturalKey, block),
          ArchiveNodeHistoryStore.encodeStoredValue(
              0, ArchiveTrieNodeCodec.encodeDiff(priorFlat.get(), null)));
    }
  }
}
