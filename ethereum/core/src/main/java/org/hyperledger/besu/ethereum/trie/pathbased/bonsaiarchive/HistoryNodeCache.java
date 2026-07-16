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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReaderV2;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.tuweni.bytes.Bytes;

/**
 * Bounded state shared by every {@link HistoryNodeLoader} and by {@code ArchiveTrieBuilder}'s
 * capture step during one builder's lifetime (one batch, or the ongoing-migration catch-up loop).
 * Resolution order for a read: {@link #committedNodeValues} LRU (500K entries) -> {@link
 * TrieNodeHistoryReaderV2} first-touch read (seekForPrev + bounded backward walk) -> live {@code
 * TRIE_BRANCH_STORAGE} fallthrough (unchanged nodes are byte-identical at HEAD). The in-memory
 * decoded trie node objects themselves are NOT cached here -- that's the JVM object graph owned by
 * the {@code StoredMerklePatriciaTrie} instances in {@code ArchiveTrieBuilder}, dropped at batch
 * end.
 */
public final class HistoryNodeCache {

  static final int MAX_CACHE_ENTRIES = 500_000;

  private final SegmentedKeyValueStorage storage;
  private final TrieNodeHistoryReaderV2 historyReader;
  private final long lastMigratedBlock;
  private final LinkedHashMap<Bytes, NodeState> committedNodeValues;
  private BloomFilter<byte[]> freshMigrationBloom;

  public HistoryNodeCache(final SegmentedKeyValueStorage storage, final long lastMigratedBlock) {
    this.storage = storage;
    this.historyReader = new TrieNodeHistoryReaderV2(storage);
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
   * read entirely (see design section 4.2, mirrors {@code TrieNodeChangeIndex#sessionWrittenKeys}).
   */
  public void enableFreshMigrationBloom() {
    freshMigrationBloom = BloomFilter.create(Funnels.byteArrayFunnel(), 30_000_000, 0.01);
  }

  public Optional<Bytes> get(final byte domain, final Bytes naturalKey) {
    final Bytes cacheKey = HistoryKey.prefix(domain, naturalKey);
    final NodeState cached = committedNodeValues.get(cacheKey);
    if (cached != null) {
      return Optional.of(cached.value());
    }
    if (freshMigrationBloom != null
        && !freshMigrationBloom.mightContain(cacheKey.toArrayUnsafe())) {
      return fallThroughToLive(naturalKey, cacheKey);
    }
    final Optional<TrieNodeHistoryReaderV2.Hit> hit =
        historyReader.nodeAtWithMeta(domain, naturalKey, lastMigratedBlock);
    if (hit.isEmpty()) {
      return fallThroughToLive(naturalKey, cacheKey);
    }
    committedNodeValues.put(
        cacheKey, new NodeState(hit.get().nodeRlp(), hit.get().countSinceFull()));
    return Optional.of(hit.get().nodeRlp());
  }

  /**
   * Prior value/countSinceFull for the capture decision -- reads only the LRU, never storage,
   * because by the time capture runs for a node its prior value was already resolved by {@link
   * #get}.
   */
  public Optional<NodeState> priorState(final byte domain, final Bytes naturalKey) {
    return Optional.ofNullable(committedNodeValues.get(HistoryKey.prefix(domain, naturalKey)));
  }

  public void recordWrite(
      final byte domain, final Bytes naturalKey, final Bytes value, final int countSinceFull) {
    final Bytes cacheKey = HistoryKey.prefix(domain, naturalKey);
    committedNodeValues.put(cacheKey, new NodeState(value, countSinceFull));
    if (freshMigrationBloom != null) {
      freshMigrationBloom.put(cacheKey.toArrayUnsafe());
    }
  }

  private Optional<Bytes> fallThroughToLive(final Bytes naturalKey, final Bytes cacheKey) {
    final Optional<byte[]> live = storage.get(TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe());
    live.ifPresent(bytes -> committedNodeValues.put(cacheKey, new NodeState(Bytes.wrap(bytes), 0)));
    return live.map(Bytes::wrap);
  }

  /** Cached node value plus the countSinceFull it was last written/read with. */
  public record NodeState(Bytes value, int countSinceFull) {}
}
