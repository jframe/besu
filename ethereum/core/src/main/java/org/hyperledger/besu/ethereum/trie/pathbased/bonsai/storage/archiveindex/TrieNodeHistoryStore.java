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

import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(TrieNodeHistoryStore.class);

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

  /**
   * Retrieves the diff-codec entries for {@code (naturalKey, blocks[i])} in a single batched read.
   *
   * <p>The returned list has the same size and ordering as {@code blocks}; a block with no stored
   * entry maps to {@link Optional#empty()}. On a RocksDB backend this issues one {@code
   * multiGetAsList} call, so the N sequential point reads that reconstruction would otherwise
   * perform collapse into a single storage round-trip.
   *
   * @param naturalKey the account or storage natural key; must not be {@code null}
   * @param blocks the block numbers to fetch, in the desired result order
   * @return one {@code Optional<Bytes>} per requested block, in the same order
   * @throws NullPointerException if {@code naturalKey} is {@code null}
   */
  public List<Optional<Bytes>> getAll(final Bytes naturalKey, final long[] blocks) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    if (blocks.length == 0) {
      return List.of();
    }
    final List<byte[]> keys = new ArrayList<>(blocks.length);
    for (final long block : blocks) {
      keys.add(ArchiveNodeKey.historyKey(naturalKey, block).toArrayUnsafe());
    }
    final List<Optional<byte[]>> raw =
        storage.multiGet(KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE, keys);
    final List<Optional<Bytes>> results = new ArrayList<>(raw.size());
    for (final Optional<byte[]> value : raw) {
      results.add(value.map(Bytes::wrap));
    }
    return results;
  }

  // ---------------------------------------------------------------------------
  // Content-addressed body store (TRIE_NODE_CAS_ARCHIVE)
  // ---------------------------------------------------------------------------

  /**
   * Stores a FULL node body in the content-addressed store, keyed by its keccak256.
   *
   * <p>Blind and idempotent: the same hash always maps to the same bytes, so re-puts are harmless
   * and no read-before-write is performed.
   *
   * @param tx the transaction on which to issue the write; must not be {@code null}
   * @param nodeHash the keccak256 of {@code body}; must not be {@code null}
   * @param body the raw node RLP; must not be {@code null}
   */
  public void putCasBody(
      final SegmentedKeyValueStorageTransaction tx, final Bytes32 nodeHash, final Bytes body) {
    Objects.requireNonNull(tx, "tx must not be null");
    Objects.requireNonNull(nodeHash, "nodeHash must not be null");
    Objects.requireNonNull(body, "body must not be null");
    tx.put(
        KeyValueSegmentIdentifier.TRIE_NODE_CAS_ARCHIVE,
        nodeHash.toArrayUnsafe(),
        body.toArrayUnsafe());
  }

  /**
   * Retrieves a FULL node body from the content-addressed store, verifying {@code keccak256(body)
   * == nodeHash} (the key IS the content hash, so corruption is detectable at this layer).
   *
   * @param nodeHash the content hash to resolve; must not be {@code null}
   * @return the body if present and self-consistent; empty if missing or corrupt (corruption is
   *     logged at WARN)
   */
  public Optional<Bytes> getCasBody(final Bytes32 nodeHash) {
    Objects.requireNonNull(nodeHash, "nodeHash must not be null");
    final Optional<Bytes> body =
        storage
            .get(KeyValueSegmentIdentifier.TRIE_NODE_CAS_ARCHIVE, nodeHash.toArrayUnsafe())
            .map(Bytes::wrap);
    if (body.isPresent() && !keccak256(body.get()).equals(nodeHash)) {
      LOG.warn(
          "TrieNodeHistoryStore: CAS body for {} fails keccak self-verification — treating as"
              + " missing (corruption)",
          nodeHash);
      return Optional.empty();
    }
    return body;
  }
}
