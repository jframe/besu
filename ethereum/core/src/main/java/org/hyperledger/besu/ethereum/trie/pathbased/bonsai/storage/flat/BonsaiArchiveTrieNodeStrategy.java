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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeChangeIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeDiffCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Archive trie node strategy for the Design-5 differential index approach. Reads delegate to the
 * base strategy (live trie reads); writes capture diff-codec entries in {@code
 * TRIE_NODE_HISTORY_ARCHIVE} and change-block records in {@code TRIE_NODE_INDEX_ARCHIVE}.
 *
 * <p>After each block, callers must invoke {@link
 * #advanceIndexProgress(SegmentedKeyValueStorageTransaction, SegmentedKeyValueStorage)} to persist
 * coverage-progress metadata.
 */
public class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveTrieNodeStrategy.class);

  /**
   * Every {@code CHECKPOINT_INTERVAL}-th mutation for a node emits a FULL entry instead of a DIFF.
   * This bounds the backward walk in {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReader}
   * to at most {@code CHECKPOINT_INTERVAL - 1} steps.
   */
  static final int CHECKPOINT_INTERVAL = 16;

  /**
   * Trie nodes at locations with at most this many nibble bytes (i.e. near the trie root) always
   * store a FULL entry rather than a DIFF. Root-adjacent nodes change frequently and are small, so
   * storing them as FULL is cheaper than computing and applying diffs.
   *
   * <p>A location of 0 bytes is the root; 1 byte = depth 2 (two nibbles). {@code FULL_ABOVE_DEPTH =
   * 2} means locations with 0, 1, or 2 bytes are stored as FULL.
   */
  static final int FULL_ABOVE_DEPTH = 2;

  /**
   * Plain point-lookup strategy used for the "current trie" reads/writes. Defaults to {@link
   * BonsaiTrieNodeStrategy} over TRIE_BRANCH_STORAGE; the migrator subclass supplies one over its
   * migration column family.
   */
  protected final TrieNodeStrategy baseStrategy;

  private final BonsaiCachedMerkleTrieLoader trieLoader;

  // --- Differential-index fields (flag-gated) ---

  private final boolean trieNodeIndexEnabled;
  private final TrieNodeHistoryStore historyStore;
  private final TrieNodeChangeIndex changeIndex;

  /**
   * Coverage-progress tracker. Non-null only when {@link #trieNodeIndexEnabled} is {@code true}.
   * Updated in {@link #advanceIndexProgress(SegmentedKeyValueStorageTransaction,
   * SegmentedKeyValueStorage)} after each block.
   */
  private final TrieNodeIndexProgress progress;

  /**
   * Cached block number for the current block's trie-node writes. {@code Long.MIN_VALUE} means
   * uninitialised. Populated on the first {@link #getCurrentBlockNumber} call within a block and
   * invalidated by {@link #advanceIndexProgress} at the end of each block.
   */
  private volatile long cachedCurrentBlockNumber = Long.MIN_VALUE;

  public BonsaiArchiveTrieNodeStrategy() {
    this(null, new BonsaiTrieNodeStrategy());
  }

  public BonsaiArchiveTrieNodeStrategy(final BonsaiCachedMerkleTrieLoader trieLoader) {
    this(trieLoader, new BonsaiTrieNodeStrategy());
  }

  protected BonsaiArchiveTrieNodeStrategy(
      final BonsaiCachedMerkleTrieLoader trieLoader, final TrieNodeStrategy baseStrategy) {
    this(trieLoader, baseStrategy, false, null, null);
  }

  /**
   * Full constructor used when the trie-node differential index is enabled.
   *
   * @param trieLoader optional trie loader for cache warming
   * @param baseStrategy underlying strategy for live-trie reads/writes
   * @param trieNodeIndexEnabled whether to capture diffs to the differential index
   * @param historyStore the diff-entry store; must not be null if {@code trieNodeIndexEnabled}
   * @param changeIndex the change-block index; must not be null if {@code trieNodeIndexEnabled}
   */
  public BonsaiArchiveTrieNodeStrategy(
      final BonsaiCachedMerkleTrieLoader trieLoader,
      final TrieNodeStrategy baseStrategy,
      final boolean trieNodeIndexEnabled,
      final TrieNodeHistoryStore historyStore,
      final TrieNodeChangeIndex changeIndex) {
    this(trieLoader, baseStrategy, trieNodeIndexEnabled, historyStore, changeIndex, null);
  }

  /**
   * Full constructor used when the trie-node differential index is enabled and coverage progress
   * tracking is required.
   *
   * @param trieLoader optional trie loader for cache warming
   * @param baseStrategy underlying strategy for live-trie reads/writes
   * @param trieNodeIndexEnabled whether to capture diffs to the differential index
   * @param historyStore the diff-entry store; must not be null if {@code trieNodeIndexEnabled}
   * @param changeIndex the change-block index; must not be null if {@code trieNodeIndexEnabled}
   * @param progress the coverage-progress tracker to advance on each block flush; may be {@code
   *     null} if coverage tracking is not needed (e.g. tests or migrator without progress wiring)
   */
  public BonsaiArchiveTrieNodeStrategy(
      final BonsaiCachedMerkleTrieLoader trieLoader,
      final TrieNodeStrategy baseStrategy,
      final boolean trieNodeIndexEnabled,
      final TrieNodeHistoryStore historyStore,
      final TrieNodeChangeIndex changeIndex,
      final TrieNodeIndexProgress progress) {
    this.trieLoader = trieLoader;
    this.baseStrategy = baseStrategy;
    this.trieNodeIndexEnabled = trieNodeIndexEnabled;
    this.historyStore = historyStore;
    this.changeIndex = changeIndex;
    this.progress = progress;
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return baseStrategy.getFlatAccountTrieNode(location, nodeHash, storage);
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return baseStrategy.getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    // Capture prior node BEFORE the base write overwrites TRIE_BRANCH_STORAGE.
    final Optional<Bytes> priorNode =
        trieNodeIndexEnabled
            ? storage.get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe()).map(Bytes::wrap)
            : Optional.empty();

    baseStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);

    if (trieLoader != null) {
      trieLoader.putAccountNode(nodeHash, node);
    }

    if (trieNodeIndexEnabled) {
      final long block = getCurrentBlockNumber(storage);
      final Bytes naturalKey = ArchiveNodeKey.account(location);
      captureTrieNodeDiff(
          transaction, naturalKey, location, block, priorNode.orElse(null), node, storage);
    }
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final Bytes accountHashLocation = Bytes.concatenate(accountHash.getBytes(), location);

    // Capture prior node BEFORE the base write overwrites TRIE_BRANCH_STORAGE.
    final Optional<Bytes> priorNode =
        trieNodeIndexEnabled
            ? storage.get(TRIE_BRANCH_STORAGE, accountHashLocation.toArrayUnsafe()).map(Bytes::wrap)
            : Optional.empty();

    baseStrategy.putFlatStorageTrieNode(
        storage, transaction, accountHash, location, nodeHash, node);

    if (trieLoader != null) {
      trieLoader.putStorageNode(nodeHash, node);
    }

    if (trieNodeIndexEnabled) {
      final long block = getCurrentBlockNumber(storage);
      final Bytes naturalKey = ArchiveNodeKey.storage(accountHash.getBytes(), location);
      captureTrieNodeDiff(
          transaction, naturalKey, location, block, priorNode.orElse(null), node, storage);
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
  }

  /**
   * Returns the actual block number currently being written (not the window-start used for suffix
   * keying). Falls back in order: cached value → {@code ARCHIVE_PROOF_BLOCK_NUMBER_KEY} → {@code
   * WORLD_BLOCK_NUMBER_KEY + 1}.
   *
   * <p>The result is cached for the duration of a single block's trie-node writes and invalidated
   * by {@link #advanceIndexProgress} at the end of each block, so storage is accessed at most once
   * per block rather than once per trie-node write.
   */
  private long getCurrentBlockNumber(final SegmentedKeyValueStorage storage) {
    // Fast path: reuse cached value for the duration of the current block's writes.
    // advanceIndexProgress() clears this at the end of each block, so the cache is
    // accessed at most once per block per storage lookup path.
    final long cached = cachedCurrentBlockNumber;
    if (cached != Long.MIN_VALUE) {
      return cached;
    }
    // Per-block override (used by tests and migration checkpoint seeding).
    // Result is cached: subsequent writes within the same block use the cache.
    final Optional<byte[]> proofBlock =
        storage.get(TRIE_BRANCH_STORAGE, ARCHIVE_PROOF_BLOCK_NUMBER_KEY);
    if (proofBlock.isPresent()) {
      cachedCurrentBlockNumber = Bytes.wrap(proofBlock.get()).toLong();
      return cachedCurrentBlockNumber;
    }
    // TODO: block 1's trie nodes are indexed at block 0 because WORLD_BLOCK_NUMBER_KEY
    // is written in the same tx as the trie nodes and hasn't committed yet. Callers
    // querying history at block 0 may see block-1 state. Fix: read from a pre-committed
    // block-number key or pass the block number explicitly from the persist() caller.
    final long block =
        storage
            .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
            .map(b -> Bytes.wrap(b).toLong() + 1L)
            .orElse(0L);
    cachedCurrentBlockNumber = block;
    return block;
  }

  // ---------------------------------------------------------------------------
  // Differential-index helpers (flag-gated)
  // ---------------------------------------------------------------------------

  /**
   * Captures a diff-codec entry for {@code (naturalKey, block)} in the given transaction.
   *
   * <p>The entry type is chosen as follows (in priority order):
   *
   * <ol>
   *   <li>Creation ({@code priorNode == null}): {@link TrieNodeDiffCodec#encodeDiff(Bytes, Bytes)}
   *       with old=null → {@code FULL | CREATION}.
   *   <li>Deletion ({@code newNode == null}): tombstone. Not currently wired in (deletions are
   *       handled via {@link TrieNodeStrategy#removeFlatAccountStateTrieNode} which is not yet
   *       hooked); included for completeness.
   *   <li>Upper-trie FULL ({@code location.size() <= FULL_ABOVE_DEPTH}): always FULL for
   *       root-adjacent nodes. The comparison uses the nibble-path {@code location} (not {@code
   *       naturalKey}) so that account and storage trie nodes are treated symmetrically: for
   *       account nodes {@code location == naturalKey}; for storage nodes {@code naturalKey =
   *       accountHash ‖ location} (32+ bytes), so {@code naturalKey.size()} would never be ≤ 2.
   *   <li>Checkpoint FULL ({@code currentMutationCount % CHECKPOINT_INTERVAL == 0}): every {@code
   *       CHECKPOINT_INTERVAL}-th mutation is stored as FULL.
   *   <li>DIFF: structural delta versus the prior node.
   * </ol>
   *
   * @param tx the transaction on which to write the history and index entries
   * @param naturalKey the account or storage natural key from {@link ArchiveNodeKey}
   * @param location the nibble-path {@code location} bytes for this trie node (used for the
   *     FULL_ABOVE_DEPTH depth check; equal to {@code naturalKey} for account nodes)
   * @param block the block number at which the node is being written
   * @param priorNode the prior node RLP from committed storage, or {@code null} if this is a
   *     creation
   * @param newNode the new node RLP being written; must not be {@code null}
   * @param storage the committed storage (used to read the current mutation count from the index)
   */
  // Package-private for reuse by Task 5.1 (migrator replay).
  void captureTrieNodeDiff(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes naturalKey,
      final Bytes location,
      final long block,
      final Bytes priorNode,
      final Bytes newNode,
      final SegmentedKeyValueStorage storage) {

    // Determine entry type, append to the index, and write the history entry.
    //
    // For creation and FULL_ABOVE_DEPTH nodes the entry type is always FULL regardless of mutation
    // count, so we call changeIndex.append directly (1 read). For DIFF/checkpoint nodes we use
    // appendAndGetPreviousCount which returns the pre-write mutation count AND does the append in
    // a single RocksDB read — replacing the old pattern of countMutationsUpTo + separate append
    // (2 reads → 1 read for the common case).
    final Bytes entry;
    if (priorNode == null) {
      // Creation: no prior node → always FULL | CREATION.
      entry = TrieNodeDiffCodec.encodeDiff(null, newNode);
      changeIndex.append(tx, naturalKey, block);
    } else if (location.size() <= FULL_ABOVE_DEPTH) {
      // Upper-trie node: always FULL to keep proof lookups cheap.
      entry = TrieNodeDiffCodec.encodeFull(newNode);
      changeIndex.append(tx, naturalKey, block);
    } else {
      // DIFF or checkpoint FULL: need mutation count to decide. Combined read+append.
      final long previousCount = changeIndex.appendAndGetPreviousCount(tx, naturalKey, block);
      entry =
          (previousCount % CHECKPOINT_INTERVAL == 0)
              ? TrieNodeDiffCodec.encodeFull(newNode)
              : TrieNodeDiffCodec.encodeDiff(priorNode, newNode);
    }

    historyStore.put(tx, naturalKey, block, entry);

    LOG.trace(
        "Diff-index entry captured: key={} block={} entryType=0x{}",
        naturalKey,
        block,
        String.format("%02x", entry.get(0)));
  }

  /**
   * Advances coverage progress for the current block.
   *
   * <p>Progress advancement (when {@code progress} is non-null and the index is enabled) happens on
   * every block:
   *
   * <ol>
   *   <li>Reads the current block number {@code N} from committed storage.
   *   <li>Calls {@link TrieNodeIndexProgress#setLastIndexedBlock(long)} with {@code N}.
   *   <li>Calls {@link TrieNodeIndexProgress#setIndexStartBlock(long)} with the start of {@code
   *       N}'s range.
   *   <li>Persists the updated progress via {@link TrieNodeIndexProgress#save}.
   * </ol>
   *
   * @param tx the transaction on which to write the updated progress bytes
   * @param storage committed storage used to read the current block number for progress advancement
   */
  public void advanceIndexProgress(
      final SegmentedKeyValueStorageTransaction tx, final SegmentedKeyValueStorage storage) {
    if (trieNodeIndexEnabled && progress != null) {
      final long block = getCurrentBlockNumber(storage);
      final long rangeId = block / ArchiveNodeKey.RANGE_SIZE;
      progress.setLastIndexedBlock(block);
      progress.setIndexStartBlock(rangeId * ArchiveNodeKey.RANGE_SIZE);
      progress.save(tx);
      // Invalidate so the next block's first write re-reads the committed block number.
      cachedCurrentBlockNumber = Long.MIN_VALUE;
    }
  }
}
