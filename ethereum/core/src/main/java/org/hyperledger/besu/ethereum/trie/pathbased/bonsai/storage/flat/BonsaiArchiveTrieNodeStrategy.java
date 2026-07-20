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

  /** Trie root (empty location, depth 0) is always stored FULL — interval 1. */
  static final int ROOT_CHECKPOINT_INTERVAL = 1;

  /**
   * Shallow non-root nodes (1–2 location bytes, i.e. trie levels 1–4) checkpoint every {@code
   * SHALLOW_CHECKPOINT_INTERVAL} mutations. These change nearly every block, so a wider interval
   * packs more cheap DIFFs between each full branch. Capped at 32 so it stays within {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReader#RECONSTRUCT_WINDOW}.
   */
  static final int SHALLOW_CHECKPOINT_INTERVAL = 32;

  /**
   * Deep nodes (>= 3 location bytes) checkpoint every {@code DEEP_CHECKPOINT_INTERVAL} mutations.
   * Every {@code N}-th mutation for a node emits a FULL entry instead of a DIFF, bounding the
   * backward reconstruction walk.
   */
  static final int DEEP_CHECKPOINT_INTERVAL = 16;

  /**
   * FULL bodies at least this many bytes (and at depth > 0) are stored once in {@code
   * TRIE_NODE_CAS_ARCHIVE} and referenced by hash from the history entry. Smaller bodies stay
   * inline: a 33-byte ref plus a 32-byte CAS key cannot pay for itself on small nodes unless
   * duplication is extreme (see the CAS design doc §5).
   */
  static final int CAS_INLINE_THRESHOLD = 128;

  /**
   * Returns the checkpoint interval for a node at the given nibble-path depth (in {@code location}
   * bytes). Every {@code interval}-th mutation for the node is stored FULL; the rest are DIFFs.
   *
   * <ul>
   *   <li>depth 0 (root) → {@link #ROOT_CHECKPOINT_INTERVAL} (always FULL)
   *   <li>depth 1–2 → {@link #SHALLOW_CHECKPOINT_INTERVAL}
   *   <li>depth ≥ 3 → {@link #DEEP_CHECKPOINT_INTERVAL}
   * </ul>
   *
   * @param locationSizeBytes the trie node's {@code location.size()} in bytes
   * @return the mutation interval at which a FULL entry is emitted
   */
  static int checkpointIntervalForDepth(final int locationSizeBytes) {
    if (locationSizeBytes == 0) {
      return ROOT_CHECKPOINT_INTERVAL;
    }
    if (locationSizeBytes <= 2) {
      return SHALLOW_CHECKPOINT_INTERVAL;
    }
    return DEEP_CHECKPOINT_INTERVAL;
  }

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
          transaction,
          naturalKey,
          location,
          nodeHash,
          block,
          priorNode.orElse(null),
          node,
          storage);
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
          transaction,
          naturalKey,
          location,
          nodeHash,
          block,
          priorNode.orElse(null),
          node,
          storage);
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
   *   <li>Depth-tiered checkpoint: {@code previousCount %
   *       checkpointIntervalForDepth(location.size()) == 0} → FULL, else DIFF. The root (depth 0)
   *       uses interval 1 and is thus always FULL; depth 1–2 use {@link
   *       #SHALLOW_CHECKPOINT_INTERVAL}; depth ≥ 3 use {@link #DEEP_CHECKPOINT_INTERVAL}.
   * </ol>
   *
   * @param tx the transaction on which to write the history and index entries
   * @param naturalKey the account or storage natural key from {@link ArchiveNodeKey}
   * @param location the nibble-path {@code location} bytes for this trie node (used for the depth
   *     check; equal to {@code naturalKey} for account nodes)
   * @param nodeHash the keccak256 of {@code newNode}, as supplied by the trie commit path; used as
   *     the CAS key when the FULL body is routed to the content-addressed store
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
      final Bytes32 nodeHash,
      final long block,
      final Bytes priorNode,
      final Bytes newNode,
      final SegmentedKeyValueStorage storage) {

    // Creation is always FULL|CREATION (one index append). For an existing node, the depth-tiered
    // interval decides FULL vs DIFF; appendAndGetPreviousCount returns the pre-write mutation count
    // AND appends in a single index read.
    final Bytes entry;
    if (priorNode == null) {
      // Creation: no prior node → always FULL | CREATION (inline or CAS-referenced).
      entry = encodeFullEntry(tx, nodeHash, newNode, location, true);
      changeIndex.append(tx, naturalKey, block);
    } else {
      // Depth-tiered checkpoint: every interval-th mutation is FULL, the rest are DIFFs.
      // The interval is chosen from the node's nibble-path depth (location.size()); the root
      // (depth 0) uses interval 1 and is therefore always FULL. Combined read+append returns the
      // pre-write mutation count in a single index read.
      final int interval = checkpointIntervalForDepth(location.size());
      final long previousCount = changeIndex.appendAndGetPreviousCount(tx, naturalKey, block);
      entry =
          (previousCount % interval == 0)
              ? encodeFullEntry(tx, nodeHash, newNode, location, false)
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
   * Encodes a FULL entry for {@code node}, routing the body through the content-addressed store
   * when it is large enough to pay for a hash reference and not the trie root.
   *
   * <p>Routing predicate: {@code !location.isEmpty() && node.size() >= CAS_INLINE_THRESHOLD}. The
   * root always stays inline (its body never duplicates and root reads stay one-hop). CAS puts are
   * blind, idempotent, and issued on EVERY routed FULL — a write-skip cache would be poisoned by
   * transaction rollback (hash marked as written, body never committed → dangling ref), so
   * duplicate puts are accepted and collapsed by compaction (design doc §3.3).
   *
   * @param tx the transaction carrying this block's writes (CAS body joins it atomically)
   * @param nodeHash keccak256 of {@code node} (the CAS key)
   * @param node the full node RLP
   * @param location the nibble-path location (depth check)
   * @param creation whether this is the node's first appearance
   * @return the encoded history entry: a 33-byte HASH_REF or an inline FULL
   */
  private Bytes encodeFullEntry(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes32 nodeHash,
      final Bytes node,
      final Bytes location,
      final boolean creation) {
    if (!location.isEmpty() && node.size() >= CAS_INLINE_THRESHOLD) {
      historyStore.putCasBody(tx, nodeHash, node);
      return TrieNodeDiffCodec.encodeFullRef(nodeHash, creation);
    }
    return creation
        ? TrieNodeDiffCodec.encodeDiff(null, node) // FULL | CREATION, inline
        : TrieNodeDiffCodec.encodeFull(node);
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
