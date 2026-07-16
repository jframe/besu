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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE_V2;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryEntryCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeDiffCodec;
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

  @SuppressWarnings("UnusedVariable") // retained for future use; wired through nodeCache
  private final SegmentedKeyValueStorage storage;

  private final HistoryNodeCache nodeCache;
  private final Map<Address, StoredMerklePatriciaTrie<Bytes, Bytes>> storageTrieCache =
      new HashMap<>();

  /**
   * Cached account trie, shared across all blocks within a single batch. Keeping the trie object
   * alive preserves its internal decoded-node cache, preventing LRU eviction of in-batch nodes from
   * {@link HistoryNodeCache} from causing stale reads when later blocks in the same batch need to
   * re-traverse those nodes. Nulled by {@link #resetBatchState()} at batch end so the next batch
   * re-roots from committed history.
   */
  private StoredMerklePatriciaTrie<Bytes, Bytes> accountTrie;

  private Hash accountRoot = Hash.EMPTY_TRIE_HASH;

  public ArchiveTrieBuilder(final SegmentedKeyValueStorage storage, final long lastMigratedBlock) {
    this.storage = storage;
    this.nodeCache = new HistoryNodeCache(storage, lastMigratedBlock);
  }

  public ArchiveTrieBuilder(
      final SegmentedKeyValueStorage storage,
      final long lastMigratedBlock,
      final Hash startingAccountRoot) {
    this(storage, lastMigratedBlock);
    this.accountRoot = startingAccountRoot;
  }

  public void applyBlock(
      final TrieLog trieLog,
      final BlockHeader header,
      final SegmentedKeyValueStorageTransaction tx) {

    final long block = header.getNumber();

    trieLog
        .getStorageChanges()
        .forEach(
            (address, slotChanges) -> {
              final Hash computedRoot =
                  applyStorageChanges(address, slotChanges, trieLog, block, tx);
              // Cross-check the computed storage root against the trie-log's expected value.
              // The account change for this address carries the updated AccountValue, which
              // embeds the storage root that the EVM computed for this block.
              final TrieLog.LogTuple<?> accountChange = trieLog.getAccountChanges().get(address);
              if (accountChange != null && accountChange.getUpdated() != null) {
                final Hash expectedRoot =
                    ((AccountValue) accountChange.getUpdated()).getStorageRoot();
                // Only check when the trie-log provides a real expected storage root.
                // Synthetic trie-logs (tests) may not compute storage roots for AccountValues,
                // leaving them at EMPTY_TRIE_HASH even when storage changes exist.
                if (!expectedRoot.equals(Hash.EMPTY_TRIE_HASH)
                    && !computedRoot.equals(expectedRoot)) {
                  throw new IllegalStateException(
                      "ArchiveTrieBuilder computed storage root "
                          + computedRoot
                          + " for address "
                          + address
                          + " at block "
                          + block
                          + " but trie-log expects "
                          + expectedRoot);
                }
              }
            });

    final Hash newAccountRoot = applyAccountChanges(trieLog, accountRoot, block, tx);

    if (!newAccountRoot.equals(header.getStateRoot())) {
      throw new IllegalStateException(
          "ArchiveTrieBuilder computed state root "
              + newAccountRoot
              + " but header at block "
              + block
              + " expects "
              + header.getStateRoot());
    }
    accountRoot = newAccountRoot;
  }

  public Hash currentAccountRoot() {
    return accountRoot;
  }

  /**
   * Drops decoded trie-node objects (both the account trie's internal cache and every open storage
   * trie) so the batch's Java heap footprint is bounded; the next {@link #applyBlock} call re-roots
   * lazily through {@link HistoryNodeCache} / {@link HistoryNodeLoader}, which is backed by
   * already-committed history, so no correctness is lost -- see design section 4.2's stated
   * lifetime ("dropped at batch end, re-root from hash").
   */
  public void resetBatchState() {
    storageTrieCache.clear();
    accountTrie = null;
  }

  /**
   * Enables the bloom-filter optimisation for a from-genesis migration. When enabled, first-ever
   * touches of a key skip the history read entirely. Must be called once, immediately after
   * construction, before any {@link #applyBlock} calls. Delegates to {@link
   * HistoryNodeCache#enableFreshMigrationBloom()}.
   */
  public void enableFreshMigrationBloom() {
    nodeCache.enableFreshMigrationBloom();
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

  Hash applyAccountChanges(
      final TrieLog trieLog,
      final Hash priorAccountRoot,
      final long block,
      final SegmentedKeyValueStorageTransaction tx) {

    // Reuse the cached trie across blocks within a batch so that its internal decoded-node cache
    // stays alive. Constructing a fresh trie each block would discard those decoded objects; if
    // the HistoryNodeCache LRU then evicts the corresponding raw-bytes entry before a later block
    // in the same uncommitted batch needs to re-traverse those nodes, the re-root would read stale
    // data and produce a wrong root → migration abort. See design section 4.2.
    if (accountTrie == null) {
      accountTrie =
          new StoredMerklePatriciaTrie<>(
              new HistoryNodeLoader(nodeCache, HistoryKey.DOMAIN_ACCOUNT, null),
              Bytes32.wrap(priorAccountRoot.getBytes()),
              Function.identity(),
              Function.identity());
    }

    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              final Bytes accountHash = address.addressHash().getBytes();
              final var updated = change.getUpdated();
              if (updated == null) {
                accountTrie.remove(accountHash);
              } else {
                accountTrie.put(accountHash, RLP.encode(updated::writeTo));
              }
            });

    accountTrie.commit(
        (location, hash, value) ->
            captureNode(HistoryKey.DOMAIN_ACCOUNT, location, location, hash, value, block, tx));

    return Hash.wrap(accountTrie.getRootHash());
  }

  private void captureNode(
      final byte domain,
      final Bytes naturalKey,
      final Bytes location,
      final Bytes32 hash,
      final Bytes value,
      final long block,
      final SegmentedKeyValueStorageTransaction tx) {

    if (hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return; // mirrors BonsaiWorldStateKeyValueStorage.Updater's existing empty-node skip
    }

    final var priorState = nodeCache.priorState(domain, naturalKey);

    final HistoryEntryCodec.EntryType type;
    final Bytes diffCodecPayload;
    final int countSinceFull;

    if (priorState.isEmpty()) {
      type = HistoryEntryCodec.EntryType.FULL_CREATION;
      diffCodecPayload = TrieNodeDiffCodec.encodeDiff(null, value);
      countSinceFull = 0;
    } else if (location.size() <= FULL_ABOVE_DEPTH
        || priorState.get().countSinceFull() + 1 >= CHECKPOINT_INTERVAL) {
      type = HistoryEntryCodec.EntryType.FULL;
      diffCodecPayload = TrieNodeDiffCodec.encodeFull(value);
      countSinceFull = 0;
    } else {
      final Bytes encodedDiff = TrieNodeDiffCodec.encodeDiff(priorState.get().value(), value);
      if (TrieNodeDiffCodec.decode(encodedDiff).isFull()) {
        // Type change (Case 4 in TrieNodeDiffCodec): old and new node have different arities
        // (branch ↔ short node). encodeDiff returns a FULL payload in this case to avoid
        // encoding a structurally incompatible delta. Promote the outer entry to FULL so that
        // TrieNodeHistoryReader never sees a FULL inner payload while walking back a DIFF chain,
        // which would cause reconstruct() to throw IllegalArgumentException.
        type = HistoryEntryCodec.EntryType.FULL;
        diffCodecPayload = encodedDiff;
        countSinceFull = 0;
      } else {
        type = HistoryEntryCodec.EntryType.DIFF;
        diffCodecPayload = encodedDiff;
        countSinceFull = priorState.get().countSinceFull() + 1;
      }
    }

    final Bytes entry = HistoryEntryCodec.encode(type, countSinceFull, diffCodecPayload);
    tx.put(
        TRIE_NODE_HISTORY_ARCHIVE_V2,
        HistoryKey.encode(domain, naturalKey, block).toArrayUnsafe(),
        entry.toArrayUnsafe());
    nodeCache.recordWrite(domain, naturalKey, value, countSinceFull);
  }
}
