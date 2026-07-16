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

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryKey;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Applies one block's trie-log changes to in-memory tries, verifies roots, and captures every dirty
 * node as a write-once entry in {@code TRIE_NODE_HISTORY_ARCHIVE_V2}. Owned by {@code
 * BonsaiFlatDbToArchiveMigrator}; single-threaded (migrator thread), same as {@code
 * parallelStateRootComputationEnabled(false)} today.
 */
public final class ArchiveTrieBuilder {

  static final int FULL_ABOVE_DEPTH = 2;
  static final int CHECKPOINT_INTERVAL = 16;

  @SuppressWarnings("UnusedVariable") // wired up for capture writes in Task 8
  private final SegmentedKeyValueStorage storage;

  private final HistoryNodeCache nodeCache;
  private final Map<Address, StoredMerklePatriciaTrie<Bytes, Bytes>> storageTrieCache =
      new HashMap<>();

  public ArchiveTrieBuilder(final SegmentedKeyValueStorage storage, final long lastMigratedBlock) {
    this.storage = storage;
    this.nodeCache = new HistoryNodeCache(storage, lastMigratedBlock);
  }

  /**
   * Applies one account's storage-slot changes, returns the account's new storage root, and drops
   * the account's storage trie from the batch-scoped cache if the account itself was deleted this
   * block (per design section 3.4, its storage entries become unreachable, not wrong -- no cleanup
   * needed).
   */
  Hash applyStorageChanges(
      final Address address,
      final Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>> slotChanges,
      final TrieLog trieLog,
      final long block,
      final SegmentedKeyValueStorageTransaction tx) {

    final TrieLog.LogTuple<?> accountChange = trieLog.getAccountChanges().get(address);
    if (accountChange != null && accountChange.getUpdated() == null) {
      storageTrieCache.remove(address);
      return Hash.EMPTY_TRIE_HASH;
    }

    final Hash priorStorageRoot = resolvePriorStorageRoot(address, accountChange);
    final Bytes accountHash = address.addressHash().getBytes();
    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        storageTrieCache.computeIfAbsent(
            address,
            addr ->
                new StoredMerklePatriciaTrie<>(
                    new HistoryNodeLoader(nodeCache, HistoryKey.DOMAIN_STORAGE, accountHash),
                    Bytes32.wrap(priorStorageRoot.getBytes()),
                    Function.identity(),
                    Function.identity()));

    slotChanges.forEach(
        (slotKey, change) -> {
          final Bytes trieKey = slotKey.getSlotHash().getBytes();
          final UInt256 updated = change.getUpdated();
          if (updated == null || updated.isZero()) {
            storageTrie.remove(trieKey);
          } else {
            storageTrie.put(trieKey, encodeSlotValue(updated));
          }
        });

    storageTrie.commit(
        (location, hash, value) ->
            captureNode(
                HistoryKey.DOMAIN_STORAGE,
                Bytes.concatenate(accountHash, location),
                location,
                hash,
                value,
                block,
                tx));

    return Hash.wrap(storageTrie.getRootHash());
  }

  private Hash resolvePriorStorageRoot(
      final Address address, final TrieLog.LogTuple<?> accountChange) {
    if (accountChange != null && accountChange.getPrior() != null) {
      return ((AccountValue) accountChange.getPrior()).getStorageRoot();
    }
    // New-account creation (accountChange present but getPrior() is null), or no account change at
    // all: reuse whatever root this builder last computed for the account within the current batch,
    // if any, else start empty.
    //
    // For new-account creation specifically, the cached-non-null case below is unreachable on the
    // account's first storage touch (nothing could have been cached for it yet). But for the "no
    // account change recorded" sub-case, a non-null cached trie is the normal outcome once the
    // account was touched by an earlier block in the same batch -- see
    // removingTheOnlySlotReturnsToTheEmptyTrieRoot, which exercises exactly that path across two
    // blocks.
    final StoredMerklePatriciaTrie<Bytes, Bytes> cached = storageTrieCache.get(address);
    return cached == null ? Hash.EMPTY_TRIE_HASH : Hash.wrap(cached.getRootHash());
  }

  private static Bytes encodeSlotValue(final UInt256 value) {
    return RLP.encode(out -> out.writeBytes(value.trimLeadingZeros()));
  }

  @SuppressWarnings("UnusedVariable") // stub -- parameters wired up in Task 8
  private void captureNode(
      final byte domain,
      final Bytes naturalKey,
      final Bytes location,
      final Bytes32 hash,
      final Bytes value,
      final long block,
      final SegmentedKeyValueStorageTransaction tx) {
    // Implemented in Task 8.
    throw new UnsupportedOperationException("captureNode implemented in Task 8");
  }
}
