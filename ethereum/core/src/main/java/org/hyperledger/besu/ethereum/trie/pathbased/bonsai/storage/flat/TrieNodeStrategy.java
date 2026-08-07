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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Pluggable strategy for trie-node reads and writes against {@code TRIE_BRANCH_STORAGE}. The
 * default implementation ({@link BonsaiTrieNodeStrategy}) is format-identical to the legacy inline
 * code; the archive implementation ({@link BonsaiArchiveTrieNodeStrategy}) additionally captures
 * full node RLP into {@code TRIE_BRANCH_STORAGE_ARCHIVE} for historical proof serving.
 *
 * <p>The two lifecycle hooks {@link #onBeforeCommit} and {@link #onDiscard} are default no-ops so
 * non-archive strategies impose no overhead. The {@code Updater} calls them unconditionally (no
 * {@code instanceof} check) — the default no-ops make this safe.
 *
 * <p><strong>Tx-ownership contract (fix 657cf447d9):</strong> the strategy instance is shared by
 * every {@code Updater} on the same storage object. {@code TrieLogManager.saveTrieLog} opens a
 * <em>second</em> updater mid-block and calls {@code commitTrieLogOnly()} which triggers {@code
 * onDiscard}. Implementations that hold per-transaction state MUST guard both hooks against calls
 * from non-owning transactions (compare by identity, not equality).
 */
public interface TrieNodeStrategy {

  Optional<Bytes> getFlatAccountTrieNode(
      Bytes location, Bytes32 nodeHash, SegmentedKeyValueStorage storage);

  Optional<Bytes> getFlatStorageTrieNode(
      Hash accountHash, Bytes location, Bytes32 nodeHash, SegmentedKeyValueStorage storage);

  void putFlatAccountTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Bytes location,
      Bytes32 nodeHash,
      Bytes node);

  void putFlatStorageTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Hash accountHash,
      Bytes location,
      Bytes32 nodeHash,
      Bytes node);

  void removeFlatAccountStateTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Bytes location);

  /**
   * Called immediately before the composed world-state transaction commits. An archive strategy
   * uses this to persist the covered-window progress marker. Default: no-op.
   */
  default void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {}

  /**
   * Called when the composed transaction is discarded (rollback or {@code commitTrieLogOnly}). An
   * archive strategy uses this to reset per-transaction capture state. Default: no-op.
   */
  default void onDiscard(final SegmentedKeyValueStorageTransaction transaction) {}
}
