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
package org.hyperledger.besu.ethereum.trie.forest.storage;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;
import org.hyperledger.besu.util.Subscribers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class ForestWorldStateKeyValueStorage implements WorldStateKeyValueStorage {

  private final Subscribers<NodesAddedListener> nodeAddedListeners = Subscribers.create();
  private final KeyValueStorage keyValueStorage;
  private final ReentrantLock lock = new ReentrantLock();

  public ForestWorldStateKeyValueStorage(final KeyValueStorage keyValueStorage) {
    this.keyValueStorage = keyValueStorage;
  }

  @Override
  public DataStorageFormat getDataStorageFormat() {
    return DataStorageFormat.FOREST;
  }

  public Optional<Bytes> getCode(final Hash codeHash) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    } else {
      return keyValueStorage.get(codeHash.getBytes().toArrayUnsafe()).map(Bytes::wrap);
    }
  }

  public Optional<Bytes> getAccountStateTrieNode(final Bytes32 nodeHash) {
    return getTrieNode(nodeHash);
  }

  public Optional<Bytes> getAccountStorageTrieNode(final Bytes32 nodeHash) {
    return getTrieNode(nodeHash);
  }

  private Optional<Bytes> getTrieNode(final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      return keyValueStorage.get(nodeHash.toArrayUnsafe()).map(Bytes::wrap);
    }
  }

  /**
   * Batch-fetches trie nodes by their hashes in a single RocksDB call. When the underlying storage
   * is a {@link SegmentedKeyValueStorageAdapter} its {@code multiGet} delegates to {@link
   * org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage#multiGet}, which on
   * RocksDB-backed storage issues a single {@code multiGetAsList} with {@code async_io=true} so all
   * block reads are submitted to io_uring together rather than as sequential pread64 calls. Falls
   * back to sequential single-key lookups otherwise.
   *
   * <p>The special {@link MerkleTrie#EMPTY_TRIE_NODE_HASH} is resolved inline without a storage
   * lookup; callers should still handle it but need not exclude it from the input list.
   *
   * @param hashes the node hashes to fetch, in order
   * @return per-hash Optional results in the same order as {@code hashes}
   */
  public List<Optional<Bytes>> getTrieNodes(final List<Bytes32> hashes) {
    final int n = hashes.size();
    final List<byte[]> realKeys = new ArrayList<>(n);
    final int[] positions = new int[n];
    int realCount = 0;

    for (int i = 0; i < n; i++) {
      if (!hashes.get(i).equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        realKeys.add(hashes.get(i).toArrayUnsafe());
        positions[realCount++] = i;
      }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    final Optional<Bytes>[] results = new Optional[n];
    for (int i = 0; i < n; i++) {
      if (hashes.get(i).equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        results[i] = Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
      }
    }

    if (realCount == 0) {
      return List.of(results);
    }

    final List<byte[]> keysToFetch = realKeys.subList(0, realCount);
    final List<Optional<byte[]>> raw;
    if (keyValueStorage instanceof SegmentedKeyValueStorageAdapter adapter) {
      raw = adapter.multiGet(keysToFetch);
    } else {
      raw = new ArrayList<>(realCount);
      for (final byte[] key : keysToFetch) {
        raw.add(keyValueStorage.get(key));
      }
    }

    for (int k = 0; k < realCount; k++) {
      results[positions[k]] = raw.get(k).map(Bytes::wrap);
    }
    return List.of(results);
  }

  public boolean isWorldStateAvailable(final Bytes32 rootHash) {
    return getAccountStateTrieNode(rootHash).isPresent();
  }

  @Override
  public void clear() {
    keyValueStorage.clear();
  }

  @Override
  public Updater updater() {
    return new Updater(lock, keyValueStorage.startTransaction(), nodeAddedListeners);
  }

  /**
   * Returns an updater whose writes optionally bypass the write-ahead log. A WAL-bypassing updater
   * is only safe for idempotent, resumable bulk loads (e.g. the Bonsai-to-Forest conversion), where
   * data lost on a crash is simply re-derived on resume and skipping the WAL frees write bandwidth.
   *
   * @param disableWAL when true, the updater's transaction bypasses the WAL
   * @return the updater
   */
  public Updater updater(final boolean disableWAL) {
    return new Updater(
        lock,
        disableWAL ? keyValueStorage.startNoWALTransaction() : keyValueStorage.startTransaction(),
        nodeAddedListeners);
  }

  public long prune(final Predicate<byte[]> inUseCheck) {
    final AtomicInteger prunedKeys = new AtomicInteger(0);
    try (final Stream<byte[]> entry = keyValueStorage.streamKeys()) {
      entry.forEach(
          key -> {
            lock.lock();
            try {
              if (!inUseCheck.test(key) && keyValueStorage.tryDelete(key)) {
                prunedKeys.incrementAndGet();
              }
            } finally {
              lock.unlock();
            }
          });
    }

    return prunedKeys.get();
  }

  public long addNodeAddedListener(final NodesAddedListener listener) {
    return nodeAddedListeners.subscribe(listener);
  }

  public void removeNodeAddedListener(final long id) {
    nodeAddedListeners.unsubscribe(id);
  }

  public static class Updater implements WorldStateKeyValueStorage.Updater {

    private final KeyValueStorageTransaction transaction;
    private final Subscribers<NodesAddedListener> nodeAddedListeners;
    private final Set<Bytes32> addedNodes = new HashSet<>();
    private final Lock lock;

    public Updater(
        final Lock lock,
        final KeyValueStorageTransaction transaction,
        final Subscribers<NodesAddedListener> nodeAddedListeners) {
      this.lock = lock;
      this.transaction = transaction;
      this.nodeAddedListeners = nodeAddedListeners;
    }

    public Updater putCode(final Bytes code) {
      // Skip the hash calculation for empty code
      final Hash codeHash = code.size() == 0 ? Hash.EMPTY : Hash.hash(code);
      return putCode(Bytes32.wrap(codeHash.getBytes()), code);
    }

    public Updater putCode(final Bytes32 codeHash, final Bytes code) {
      if (code.size() == 0) {
        // Don't save empty values
        return this;
      }

      addedNodes.add(codeHash);
      transaction.put(codeHash.toArrayUnsafe(), code.toArrayUnsafe());
      return this;
    }

    public Updater saveWorldState(final Bytes32 nodeHash, final Bytes node) {
      return putAccountStateTrieNode(nodeHash, node);
    }

    public Updater putAccountStateTrieNode(final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      addedNodes.add(nodeHash);
      transaction.put(nodeHash.toArrayUnsafe(), node.toArrayUnsafe());
      return this;
    }

    public WorldStateKeyValueStorage.Updater removeAccountStateTrieNode(final Bytes32 nodeHash) {
      transaction.remove(nodeHash.toArrayUnsafe());
      return this;
    }

    public Updater putAccountStorageTrieNode(final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      addedNodes.add(nodeHash);
      transaction.put(nodeHash.toArrayUnsafe(), node.toArrayUnsafe());
      return this;
    }

    @Override
    public void commit() {
      lock.lock();
      try {
        nodeAddedListeners.forEach(listener -> listener.onNodesAdded(addedNodes));
        transaction.commit();
      } finally {
        lock.unlock();
      }
    }

    public void rollback() {
      addedNodes.clear();
      transaction.rollback();
    }
  }
}
