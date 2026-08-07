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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Point store for {@code TRIE_BRANCH_STORAGE_ARCHIVE}. FULL-only: the value is the bare node RLP.
 * (PR2 replaces the value with a codec entry; PR6 adds an index over these keys.)
 */
public final class ArchiveNodeHistoryStore {

  private final SegmentedKeyValueStorage storage;

  public ArchiveNodeHistoryStore(final SegmentedKeyValueStorage storage) {
    this.storage = Objects.requireNonNull(storage, "storage must not be null");
  }

  public void put(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final long block,
      final Bytes nodeRlp) {
    final Bytes key = ArchiveNodeKey.historyKey(naturalKey, block);
    tx.put(TRIE_BRANCH_STORAGE_ARCHIVE, key.toArrayUnsafe(), nodeRlp.toArrayUnsafe());
  }

  public Optional<Bytes> getLatestBefore(final Bytes naturalKey, final long block) {
    final Bytes seekKey = ArchiveNodeKey.historyKey(naturalKey, block);
    return storage
        .getNearestBefore(TRIE_BRANCH_STORAGE_ARCHIVE, seekKey)
        .filter(nearest -> naturalKeyMatches(naturalKey, nearest.key()))
        .flatMap(nearest -> nearest.value().map(Bytes::wrap));
  }

  /**
   * {@code getNearestBefore} scans the whole CF, so the returned key may belong to a different
   * natural key; reject those. Known limitation: variable-length natural keys mean one can be a
   * strict byte-prefix of another; a rare interleaving can make this return empty for a key that
   * does have an earlier entry — a missed reconstruction, never a wrong one (the caller's
   * fail-closed hash check rejects any bad node). Fixing it needs a fixed-width key length field
   * (out of scope for PR1).
   */
  private static boolean naturalKeyMatches(final Bytes naturalKey, final Bytes foundKey) {
    return foundKey.size() >= naturalKey.size()
        && ArchiveNodeKey.naturalKeyFromHistoryKey(foundKey).equals(naturalKey);
  }
}
