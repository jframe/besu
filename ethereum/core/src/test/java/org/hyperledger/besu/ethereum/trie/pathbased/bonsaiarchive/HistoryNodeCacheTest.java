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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE_V2;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryEntryCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeDiffCodec;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HistoryNodeCacheTest {

  private static final Bytes LEAF_PATH = Bytes.fromHexString("0x20ab");

  private SegmentedKeyValueStorage storage;
  private final Bytes naturalKey = Bytes.fromHexString("0x0a");

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_NODE_HISTORY_ARCHIVE_V2, TRIE_BRANCH_STORAGE));
  }

  /**
   * Builds a minimal valid 2-item short (leaf) node RLP so real {@link TrieNodeDiffCodec} arity
   * checks (which require an actual 2-item or 17-item RLP list, not arbitrary bytes) succeed. Only
   * the value byte varies between fixtures so distinct "versions" of the same node are
   * distinguishable. Mirrors the fixture helper in {@code TrieNodeHistoryReaderTest}.
   */
  private static Bytes leafNode(final int valueByte) {
    final Bytes value = Bytes.of((byte) valueByte);
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(LEAF_PATH);
          out.writeRaw(RLP.encodeOne(value));
          out.endList();
        });
  }

  private void putHistoryEntry(final Bytes nodeRlp, final long block) {
    final var tx = storage.startTransaction();
    tx.put(
        TRIE_NODE_HISTORY_ARCHIVE_V2,
        HistoryKey.encode(HistoryKey.DOMAIN_ACCOUNT, naturalKey, block).toArrayUnsafe(),
        HistoryEntryCodec.encode(
                HistoryEntryCodec.EntryType.FULL, 0, TrieNodeDiffCodec.encodeFull(nodeRlp))
            .toArrayUnsafe());
    tx.commit();
  }

  @Test
  void fallsThroughToLiveStorageWhenNoHistoryEntryExists() {
    final Bytes liveNode = Bytes.fromHexString("0xc0");
    final var tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe(), liveNode.toArrayUnsafe());
    tx.commit();

    final HistoryNodeCache cache = new HistoryNodeCache(storage, 0L);
    assertThat(cache.get(HistoryKey.DOMAIN_ACCOUNT, naturalKey)).contains(liveNode);
  }

  @Test
  void prefersHistoryEntryOverLiveWhenBothExist() {
    final Bytes historyNode = leafNode(0xaa);
    final Bytes liveNode = Bytes.fromHexString("0xc1bb");
    final var tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe(), liveNode.toArrayUnsafe());
    tx.commit();
    putHistoryEntry(historyNode, 5L);

    final HistoryNodeCache cache = new HistoryNodeCache(storage, 10L);
    assertThat(cache.get(HistoryKey.DOMAIN_ACCOUNT, naturalKey)).contains(historyNode);
  }

  @Test
  void priorStateIsATrueMissBeforeAnyWriteAndReflectsTheRecordedValueAfter() {
    final HistoryNodeCache cache = new HistoryNodeCache(storage, 0L);
    // Nothing recorded yet -- priorState must be empty even though get() was never called either,
    // i.e. priorState never itself triggers a storage read that could populate the cache.
    assertThat(cache.priorState(HistoryKey.DOMAIN_ACCOUNT, naturalKey)).isEmpty();

    final Bytes value = Bytes.fromHexString("0xc0");
    cache.recordWrite(HistoryKey.DOMAIN_ACCOUNT, naturalKey, value, 3);
    final var recorded = cache.priorState(HistoryKey.DOMAIN_ACCOUNT, naturalKey);
    assertThat(recorded).isPresent();
    assertThat(recorded.get().value()).isEqualTo(value);
    assertThat(recorded.get().countSinceFull()).isEqualTo(3);
  }

  @Test
  void getCachesHistoryResultSoALaterStorageMutationIsNotObserved() {
    final Bytes originalNode = leafNode(0xaa);
    putHistoryEntry(originalNode, 5L);

    final HistoryNodeCache cache = new HistoryNodeCache(storage, 10L);
    assertThat(cache.get(HistoryKey.DOMAIN_ACCOUNT, naturalKey)).contains(originalNode);

    // Mutate the underlying entry directly in storage. A correctly-caching implementation must
    // not notice this on the second lookup; if get() were not caching, this assertion would fail
    // because the second read would pick up the mutated value instead.
    final Bytes mutatedNode = leafNode(0xff);
    putHistoryEntry(mutatedNode, 5L);

    assertThat(cache.get(HistoryKey.DOMAIN_ACCOUNT, naturalKey)).contains(originalNode);
  }

  @Test
  void freshMigrationBloomSkipsHistoryReadWhenKeyNeverWritten() {
    // A history entry genuinely exists and would be found by a real reconstruction read. With the
    // bloom enabled and naturalKey never having gone through recordWrite this migration, get()
    // must report "definitely not touched" and fall through to the (empty) live CF instead of
    // returning the history value -- proving the bloom short-circuit actually ran, not merely
    // that both paths happen to return empty.
    final Bytes historyNode = leafNode(0xaa);
    putHistoryEntry(historyNode, 5L);

    final HistoryNodeCache cache = new HistoryNodeCache(storage, 10L);
    cache.enableFreshMigrationBloom();
    assertThat(cache.get(HistoryKey.DOMAIN_ACCOUNT, naturalKey)).isEmpty();
  }

  @Test
  void freshMigrationBloomAllowsHistoryReadAfterRecordWrite() {
    // Sanity counterpart to the test above: once recordWrite has marked the key as touched this
    // migration, the bloom must not suppress subsequent reads for it via priorState.
    final HistoryNodeCache cache = new HistoryNodeCache(storage, 10L);
    cache.enableFreshMigrationBloom();
    final Bytes value = Bytes.fromHexString("0xc2cc");
    cache.recordWrite(HistoryKey.DOMAIN_ACCOUNT, naturalKey, value, 0);
    assertThat(cache.priorState(HistoryKey.DOMAIN_ACCOUNT, naturalKey))
        .contains(new HistoryNodeCache.NodeState(value, 0));
  }

  @Test
  void uncommittedBatchWriteSurvivesLruScaleChurnAndNeverFallsThroughToLive() {
    // Regression for the QBFT block-222 migration failure: a node written by the current
    // (uncommitted) batch was evicted from the bounded LRU by later same-batch writes; the reader
    // cannot see uncommitted entries, so the read silently fell through to TRIE_BRANCH_STORAGE and
    // returned the chain-HEAD version of the node -> wrong storage root. Pending-batch writes must
    // be pinned regardless of how many other writes the batch makes.
    final Bytes batchValue = leafNode(0x11);
    final Bytes livePoison = Bytes.fromHexString("0xdead");
    final var tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe(), livePoison.toArrayUnsafe());
    tx.commit();

    final HistoryNodeCache cache = new HistoryNodeCache(storage, 0L);
    cache.recordWrite(HistoryKey.DOMAIN_ACCOUNT, naturalKey, batchValue, 0);
    // Churn far past the LRU capacity within the same batch.
    for (int i = 0; i < HistoryNodeCache.MAX_CACHE_ENTRIES + 10; i++) {
      cache.recordWrite(HistoryKey.DOMAIN_STORAGE, Bytes.ofUnsignedInt(i), Bytes.of((byte) i), 0);
    }

    assertThat(cache.get(HistoryKey.DOMAIN_ACCOUNT, naturalKey)).contains(batchValue);
  }

  @Test
  void committedBatchWriteEvictedFromLruIsRecoveredViaReaderNotLive() {
    // Second half of the same regression: once a batch commits, its writes may be evicted from the
    // LRU -- but onBatchCommitted must advance the reader watermark so the eviction is recoverable
    // from the committed TRIE_NODE_HISTORY_ARCHIVE_V2 entries instead of falling through to the
    // chain-HEAD TRIE_BRANCH_STORAGE value.
    final Bytes committedValue = leafNode(0x22);
    final Bytes livePoison = Bytes.fromHexString("0xdead");
    final var tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, naturalKey.toArrayUnsafe(), livePoison.toArrayUnsafe());
    tx.commit();

    final HistoryNodeCache cache = new HistoryNodeCache(storage, 0L);
    // Batch 1 writes the node (both through the cache and, as the migrator's tx would, to the CF).
    cache.recordWrite(HistoryKey.DOMAIN_ACCOUNT, naturalKey, committedValue, 0);
    putHistoryEntry(committedValue, 5L);
    cache.onBatchCommitted(5L);

    // Batch 2 churns enough distinct keys that committing it evicts the batch-1 entry from the
    // LRU.
    for (int i = 0; i < HistoryNodeCache.MAX_CACHE_ENTRIES + 10; i++) {
      cache.recordWrite(HistoryKey.DOMAIN_STORAGE, Bytes.ofUnsignedInt(i), Bytes.of((byte) i), 0);
    }
    cache.onBatchCommitted(6L);

    assertThat(cache.get(HistoryKey.DOMAIN_ACCOUNT, naturalKey)).contains(committedValue);
  }

  @Test
  void pendingWriteCountAndBytesTrackTheBatchAndResetOnCommit() {
    final HistoryNodeCache cache = new HistoryNodeCache(storage, 0L);
    assertThat(cache.pendingWriteCount()).isZero();
    assertThat(cache.pendingWriteBytes()).isZero();

    cache.recordWrite(HistoryKey.DOMAIN_ACCOUNT, naturalKey, Bytes.fromHexString("0xc0c1"), 0);
    assertThat(cache.pendingWriteCount()).isEqualTo(1);
    assertThat(cache.pendingWriteBytes()).isGreaterThan(0);

    // Overwriting the same location must not grow the count (it is a distinct-locations bound).
    cache.recordWrite(HistoryKey.DOMAIN_ACCOUNT, naturalKey, Bytes.fromHexString("0xc2c3c4"), 1);
    assertThat(cache.pendingWriteCount()).isEqualTo(1);

    cache.onBatchCommitted(7L);
    assertThat(cache.pendingWriteCount()).isZero();
    assertThat(cache.pendingWriteBytes()).isZero();
  }
}
