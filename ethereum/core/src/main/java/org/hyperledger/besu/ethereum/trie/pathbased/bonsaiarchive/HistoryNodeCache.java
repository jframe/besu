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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.tuweni.bytes.Bytes;

/**
 * State shared by every {@link HistoryNodeLoader} and by {@code ArchiveTrieBuilder}'s capture step
 * during one builder's lifetime. Resolution order for a read: {@link #pendingBatchWrites} (nodes
 * written by the current uncommitted batch -- unbounded and never evicted, because the {@link
 * TrieNodeHistoryReader} cannot see uncommitted entries and a silent live-fallthrough for such a
 * node returns future-state bytes) -> {@link #committedNodeValues} LRU (500K entries) -> {@link
 * TrieNodeHistoryReader} read at {@link #lastMigratedBlock} (seekForPrev + bounded backward walk)
 * -> live {@code TRIE_BRANCH_STORAGE} fallthrough (sound only for nodes never written by the
 * migration, i.e. byte-identical at HEAD). {@link #onBatchCommitted} moves the pending map into the
 * LRU and advances {@link #lastMigratedBlock} so evicted entries remain recoverable via the reader.
 * The in-memory decoded trie node objects themselves are NOT cached here -- that's the JVM object
 * graph owned by the {@code StoredMerklePatriciaTrie} instances in {@code ArchiveTrieBuilder},
 * dropped at batch end.
 */
public final class HistoryNodeCache {

  static final int MAX_CACHE_ENTRIES = 500_000;

  private final SegmentedKeyValueStorage storage;
  private final TrieNodeHistoryReader historyReader;
  private long lastMigratedBlock;
  private final LinkedHashMap<Bytes, NodeState> committedNodeValues;
  private final Map<Bytes, NodeState> pendingBatchWrites = new HashMap<>();
  private long pendingBatchBytes = 0;
  private BloomFilter<byte[]> freshMigrationBloom;

  public HistoryNodeCache(final SegmentedKeyValueStorage storage, final long lastMigratedBlock) {
    this.storage = storage;
    this.historyReader = new TrieNodeHistoryReader(storage);
    this.lastMigratedBlock = lastMigratedBlock;
    this.committedNodeValues =
        new LinkedHashMap<>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(final Map.Entry<Bytes, NodeState> eldest) {
            return size() > MAX_CACHE_ENTRIES;
          }
        };
  }

  /**
   * Enable on a from-genesis migration only -- lets first-ever touches of a key skip the history
   * read entirely (see design section 4.2, mirrors the bloom-filter optimisation in the V2
   * append-only writer).
   */
  public void enableFreshMigrationBloom() {
    freshMigrationBloom = BloomFilter.create(Funnels.byteArrayFunnel(), 30_000_000, 0.01);
  }

  public Optional<Bytes> get(final byte domain, final Bytes naturalKey) {
    final Bytes cacheKey = HistoryKey.prefix(domain, naturalKey);
    final NodeState pending = pendingBatchWrites.get(cacheKey);
    if (pending != null) {
      return Optional.of(pending.value());
    }
    final NodeState cached = committedNodeValues.get(cacheKey);
    if (cached != null) {
      return Optional.of(cached.value());
    }
    if (freshMigrationBloom != null
        && !freshMigrationBloom.mightContain(cacheKey.toArrayUnsafe())) {
      return fallThroughToLive(naturalKey, cacheKey);
    }
    final Optional<TrieNodeHistoryReader.Hit> hit =
        historyReader.nodeAtWithMeta(domain, naturalKey, lastMigratedBlock);
    if (hit.isEmpty()) {
      return fallThroughToLive(naturalKey, cacheKey);
    }
    committedNodeValues.put(
        cacheKey, new NodeState(hit.get().nodeRlp(), hit.get().countSinceFull()));
    return Optional.of(hit.get().nodeRlp());
  }

  /**
   * Prior value/countSinceFull for the capture decision -- reads only the in-memory maps, never
   * storage, because by the time capture runs for a node its prior value was already resolved by
   * {@link #get}.
   */
  public Optional<NodeState> priorState(final byte domain, final Bytes naturalKey) {
    final Bytes cacheKey = HistoryKey.prefix(domain, naturalKey);
    final NodeState pending = pendingBatchWrites.get(cacheKey);
    if (pending != null) {
      return Optional.of(pending);
    }
    return Optional.ofNullable(committedNodeValues.get(cacheKey));
  }

  public void recordWrite(
      final byte domain, final Bytes naturalKey, final Bytes value, final int countSinceFull) {
    final Bytes cacheKey = HistoryKey.prefix(domain, naturalKey);
    final NodeState previous =
        pendingBatchWrites.put(cacheKey, new NodeState(value, countSinceFull));
    pendingBatchBytes +=
        value.size() + (previous == null ? cacheKey.size() : -previous.value().size());
    if (freshMigrationBloom != null) {
      freshMigrationBloom.put(cacheKey.toArrayUnsafe());
    }
  }

  /**
   * Number of distinct node locations written by the current uncommitted batch. The migrator uses
   * this to end a batch before the pending map's heap footprint grows unbounded.
   */
  public int pendingWriteCount() {
    return pendingBatchWrites.size();
  }

  /** Approximate heap bytes (keys + latest values) held by the current batch's pending writes. */
  public long pendingWriteBytes() {
    return pendingBatchBytes;
  }

  /**
   * Called after the migrator durably commits a batch ending at {@code block}. Moves the batch's
   * pinned writes into the bounded LRU (safe to evict now -- the {@link TrieNodeHistoryReader} can
   * recover them from the committed {@code TRIE_NODE_HISTORY_ARCHIVE_V2} entries) and advances the
   * reader watermark so those recoveries actually see the new entries.
   */
  public void onBatchCommitted(final long block) {
    committedNodeValues.putAll(pendingBatchWrites);
    pendingBatchWrites.clear();
    pendingBatchBytes = 0;
    lastMigratedBlock = block;
  }

  private Optional<Bytes> fallThroughToLive(final Bytes naturalKey, final Bytes cacheKey) {
    final Optional<byte[]> live = storage.get(TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe());
    live.ifPresent(bytes -> committedNodeValues.put(cacheKey, new NodeState(Bytes.wrap(bytes), 0)));
    return live.map(Bytes::wrap);
  }

  /** Cached node value plus the countSinceFull it was last written/read with. */
  public record NodeState(Bytes value, int countSinceFull) {}
}
