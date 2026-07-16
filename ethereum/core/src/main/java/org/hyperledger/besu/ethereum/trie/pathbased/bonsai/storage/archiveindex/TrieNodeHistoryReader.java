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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE_V2;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage.NearestKeyValue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Answers "what was this trie node's value at or before block T?" against {@code
 * TRIE_NODE_HISTORY_ARCHIVE_V2}. One {@link SegmentedKeyValueStorage#getNearestBeforeMatchLength}
 * call finds the newest entry &lt;= T; if it is a DIFF, walks backward one entry at a time (each
 * step is another getNearestBeforeMatchLength call at {@code foundBlock - 1}, since the plugin
 * storage API exposes only one-shot nearest-key lookups, not a reusable iterator handle) until a
 * FULL is found, bounded by {@link #MAX_BACKWARD_WALK_STEPS}. Collected DIFF payloads are folded
 * with the unchanged {@link TrieNodeDiffCodec#reconstruct}.
 */
public final class TrieNodeHistoryReader {

  static final int MAX_BACKWARD_WALK_STEPS = 64;

  private final SegmentedKeyValueStorage storage;

  public TrieNodeHistoryReader(final SegmentedKeyValueStorage storage) {
    this.storage = storage;
  }

  /** Node bytes only — the proof read path doesn't need countSinceFull. */
  public Optional<Bytes> nodeAt(final byte domain, final Bytes naturalKey, final long targetBlock) {
    return nodeAtWithMeta(domain, naturalKey, targetBlock).map(Hit::nodeRlp);
  }

  /**
   * Node bytes plus the newest entry's countSinceFull — the migration node loader needs both so it
   * can seed its own checkpoint decision without a second read.
   */
  public Optional<Hit> nodeAtWithMeta(
      final byte domain, final Bytes naturalKey, final long targetBlock) {
    final Optional<NearestKeyValue> newestOpt =
        storage.getNearestBeforeMatchLength(
            TRIE_NODE_HISTORY_ARCHIVE_V2, HistoryKey.encode(domain, naturalKey, targetBlock));
    if (newestOpt.isEmpty() || !HistoryKey.matchesNode(newestOpt.get().key(), domain, naturalKey)) {
      return Optional.empty();
    }

    final NearestKeyValue newest = newestOpt.get();
    final HistoryEntryCodec.Decoded newestDecoded =
        HistoryEntryCodec.decode(Bytes.wrap(newest.value().orElseThrow()));
    final int newestCountSinceFull = newestDecoded.countSinceFull();

    final Deque<Bytes> diffPayloadsOldestFirst = new ArrayDeque<>();
    HistoryEntryCodec.Decoded current = newestDecoded;
    long walkBlock = HistoryKey.blockOf(newest.key());
    int steps = 0;
    while (!current.isFull()) {
      final Bytes diffPayload = current.diffCodecPayload();
      if (TrieNodeDiffCodec.decode(diffPayload).isDeletion()) {
        // The writer (ArchiveTrieBuilder) never emits DELETION-tagged entries -- deleted nodes
        // are simply never referenced again, no tombstone needed. If one is ever encountered
        // here it means corrupt/legacy data; fail closed with a clear error naming the node and
        // block, rather than let an unrelated IllegalArgumentException surface out of
        // TrieNodeDiffCodec#reconstruct three calls away.
        throw new IllegalStateException(
            "history entry for naturalKey="
                + naturalKey
                + " at block "
                + walkBlock
                + " is a DELETION tombstone; cannot reconstruct through a deletion -- corrupt"
                + " history");
      }
      diffPayloadsOldestFirst.addFirst(diffPayload);
      if (++steps > MAX_BACKWARD_WALK_STEPS) {
        throw new IllegalStateException(
            "history chain for naturalKey="
                + naturalKey
                + " exceeded "
                + MAX_BACKWARD_WALK_STEPS
                + " steps without a FULL entry -- corrupt history");
      }
      walkBlock -= 1;
      if (walkBlock < 0) {
        throw new IllegalStateException(
            "history chain for naturalKey="
                + naturalKey
                + " ran past block 0 without finding a FULL entry -- corrupt history (earliest"
                + " entry mis-typed as DIFF instead of FULL/FULL_CREATION?)");
      }
      final Optional<NearestKeyValue> stepOpt =
          storage.getNearestBeforeMatchLength(
              TRIE_NODE_HISTORY_ARCHIVE_V2, HistoryKey.encode(domain, naturalKey, walkBlock));
      if (stepOpt.isEmpty() || !HistoryKey.matchesNode(stepOpt.get().key(), domain, naturalKey)) {
        throw new IllegalStateException(
            "broken DIFF chain for naturalKey="
                + naturalKey
                + " walking back from block "
                + walkBlock);
      }
      current = HistoryEntryCodec.decode(Bytes.wrap(stepOpt.get().value().orElseThrow()));
      walkBlock = HistoryKey.blockOf(stepOpt.get().key());
    }

    final Bytes reconstructed =
        TrieNodeDiffCodec.reconstruct(
            current.diffCodecPayload(), new ArrayList<>(diffPayloadsOldestFirst));
    return Optional.of(new Hit(reconstructed, newestCountSinceFull));
  }

  /** Reconstructed node RLP plus the newest entry's countSinceFull. */
  public record Hit(Bytes nodeRlp, int countSinceFull) {}
}
