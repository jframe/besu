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

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Stores and retrieves diff-codec entries keyed by {@code naturalKey ‖ block(8 bytes BE)} in the
 * {@code TRIE_NODE_HISTORY_ARCHIVE} column family (Design 5, Task 3.1).
 *
 * <p>This is a simple point-access layer: it writes and reads individual entries and does no
 * scanning, reconstruction, or business logic. Tasks 3.2 and 3.3 build on top of this class.
 *
 * <h3>Key layout</h3>
 *
 * <p>Every storage key is constructed by {@link ArchiveNodeKey#historyKey(Bytes, long)}:
 *
 * <pre>
 * naturalKey ‖ block(8 bytes big-endian)
 * </pre>
 *
 * <h3>Value layout</h3>
 *
 * <p>Values are opaque {@link Bytes} produced by {@link TrieNodeDiffCodec}: a FULL entry, a DIFF
 * entry, or a deletion tombstone.
 *
 * <h3>Read vs. write semantics</h3>
 *
 * <p>{@link #get} reads from committed storage. {@link #put} and {@link #delete} issue writes on
 * the caller-supplied transaction; the caller is responsible for committing the transaction.
 */
public final class TrieNodeHistoryStore {

  private final SegmentedKeyValueStorage storage;

  /**
   * Constructs a new store backed by the given segmented KV storage.
   *
   * @param storage the underlying key-value storage (must contain {@code
   *     TRIE_NODE_HISTORY_ARCHIVE})
   * @throws NullPointerException if {@code storage} is {@code null}
   */
  public TrieNodeHistoryStore(final SegmentedKeyValueStorage storage) {
    this.storage = Objects.requireNonNull(storage, "storage must not be null");
  }

  // ---------------------------------------------------------------------------
  // Write path
  // ---------------------------------------------------------------------------

  /**
   * Stores a diff-codec entry for {@code (naturalKey, block)} in the given transaction.
   *
   * <p>The storage key is {@code naturalKey ‖ block(8 bytes BE)}. The entry may be a FULL entry, a
   * DIFF entry, or a deletion tombstone as produced by {@link TrieNodeDiffCodec}.
   *
   * @param tx the transaction on which to issue the write; must not be {@code null}
   * @param naturalKey the account or storage natural key (from {@link ArchiveNodeKey}); must not be
   *     {@code null}
   * @param block the block number at which the node state was recorded
   * @param entry the encoded diff-codec entry; must not be {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  public void put(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final long block,
      final Bytes entry) {
    Objects.requireNonNull(tx, "tx must not be null");
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    Objects.requireNonNull(entry, "entry must not be null");
    final Bytes key = ArchiveNodeKey.historyKey(naturalKey, block);
    tx.put(
        KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE,
        key.toArrayUnsafe(),
        entry.toArrayUnsafe());
  }

  /**
   * Removes the entry for {@code (naturalKey, block)} from the given transaction.
   *
   * <p>If no entry exists for this key/block combination, this is a no-op.
   *
   * @param tx the transaction on which to issue the remove; must not be {@code null}
   * @param naturalKey the account or storage natural key; must not be {@code null}
   * @param block the block number
   * @throws NullPointerException if {@code tx} or {@code naturalKey} is {@code null}
   */
  public void delete(
      final SegmentedKeyValueStorageTransaction tx, final Bytes naturalKey, final long block) {
    Objects.requireNonNull(tx, "tx must not be null");
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    final Bytes key = ArchiveNodeKey.historyKey(naturalKey, block);
    tx.remove(KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE, key.toArrayUnsafe());
  }

  // ---------------------------------------------------------------------------
  // Read path
  // ---------------------------------------------------------------------------

  /**
   * Retrieves the diff-codec entry for {@code (naturalKey, block)} from committed storage.
   *
   * @param naturalKey the account or storage natural key; must not be {@code null}
   * @param block the block number
   * @return the entry bytes if present, or {@link Optional#empty()} if not stored
   * @throws NullPointerException if {@code naturalKey} is {@code null}
   */
  public Optional<Bytes> get(final Bytes naturalKey, final long block) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    final Bytes key = ArchiveNodeKey.historyKey(naturalKey, block);
    return storage
        .get(KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE, key.toArrayUnsafe())
        .map(Bytes::wrap);
  }
}
