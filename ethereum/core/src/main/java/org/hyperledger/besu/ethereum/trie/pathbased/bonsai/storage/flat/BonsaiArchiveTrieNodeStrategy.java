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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeChangeIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeDiffCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Archive trie node strategy. Reads from {@code TRIE_BRANCH_STORAGE_ARCHIVE} using suffix-based
 * nearest-before lookup; writes to both {@code TRIE_BRANCH_STORAGE} (via delegate) and {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE}.
 *
 * <p>When the trie-node differential index is enabled ({@link #trieNodeIndexEnabled}), each write
 * also captures a diff-codec entry in {@code TRIE_NODE_HISTORY_ARCHIVE} and appends a change-block
 * record to the per-node index in {@code TRIE_NODE_INDEX_ARCHIVE}. The bloom accumulator for the
 * current block is held in-memory and must be flushed by calling {@link
 * #flushPendingBlooms(SegmentedKeyValueStorageTransaction)} before committing the transaction.
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

  private final Long trieNodeCheckpointInterval;
  private final BonsaiCachedMerkleTrieLoader trieLoader;
  private volatile boolean intervalSeeded = false;

  // --- Differential-index fields (flag-gated) ---

  private final boolean trieNodeIndexEnabled;
  private final TrieNodeHistoryStore historyStore;
  private final TrieNodeChangeIndex changeIndex;

  /**
   * Coverage-progress tracker. Non-null only when {@link #trieNodeIndexEnabled} is {@code true}.
   * Updated in {@link #flushPendingBlooms(SegmentedKeyValueStorageTransaction)} after each block.
   */
  private final TrieNodeIndexProgress progress;

  /**
   * Pending per-range bloom accumulator for the current block. Populated by {@link
   * #captureTrieNodeDiff} and flushed by {@link
   * #flushPendingBlooms(SegmentedKeyValueStorageTransaction)}.
   *
   * <p>Keyed by {@code rangeId}; values are raw bloom bytes (128 KiB each). Access is not
   * synchronised — this class is used single-threaded per block import.
   */
  private final Map<Long, byte[]> pendingBlooms = new HashMap<>();

  public BonsaiArchiveTrieNodeStrategy(final Long trieNodeCheckpointInterval) {
    this(trieNodeCheckpointInterval, null);
  }

  public BonsaiArchiveTrieNodeStrategy(
      final Long trieNodeCheckpointInterval, final BonsaiCachedMerkleTrieLoader trieLoader) {
    this(trieNodeCheckpointInterval, trieLoader, new BonsaiTrieNodeStrategy());
  }

  protected BonsaiArchiveTrieNodeStrategy(
      final Long trieNodeCheckpointInterval,
      final BonsaiCachedMerkleTrieLoader trieLoader,
      final TrieNodeStrategy baseStrategy) {
    this(trieNodeCheckpointInterval, trieLoader, baseStrategy, false, null, null);
  }

  /**
   * Full constructor used when the trie-node differential index is enabled.
   *
   * @param trieNodeCheckpointInterval suffix-archive checkpoint interval (null = proofs disabled)
   * @param trieLoader optional trie loader for cache warming
   * @param baseStrategy underlying strategy for live-trie reads/writes
   * @param trieNodeIndexEnabled whether to capture diffs to the differential index
   * @param historyStore the diff-entry store; must not be null if {@code trieNodeIndexEnabled}
   * @param changeIndex the change-block index; must not be null if {@code trieNodeIndexEnabled}
   * @throws IllegalArgumentException if {@code trieNodeIndexEnabled} is {@code true} but {@code
   *     trieNodeCheckpointInterval} is {@code null} — the index writes are triggered inside the
   *     suffix-archive write path and cannot function without a valid interval
   */
  public BonsaiArchiveTrieNodeStrategy(
      final Long trieNodeCheckpointInterval,
      final BonsaiCachedMerkleTrieLoader trieLoader,
      final TrieNodeStrategy baseStrategy,
      final boolean trieNodeIndexEnabled,
      final TrieNodeHistoryStore historyStore,
      final TrieNodeChangeIndex changeIndex) {
    this(
        trieNodeCheckpointInterval,
        trieLoader,
        baseStrategy,
        trieNodeIndexEnabled,
        historyStore,
        changeIndex,
        null);
  }

  /**
   * Full constructor used when the trie-node differential index is enabled and coverage progress
   * tracking is required.
   *
   * @param trieNodeCheckpointInterval suffix-archive checkpoint interval (null = proofs disabled)
   * @param trieLoader optional trie loader for cache warming
   * @param baseStrategy underlying strategy for live-trie reads/writes
   * @param trieNodeIndexEnabled whether to capture diffs to the differential index
   * @param historyStore the diff-entry store; must not be null if {@code trieNodeIndexEnabled}
   * @param changeIndex the change-block index; must not be null if {@code trieNodeIndexEnabled}
   * @param progress the coverage-progress tracker to advance on each block flush; may be {@code
   *     null} if coverage tracking is not needed (e.g. tests or migrator without progress wiring)
   * @throws IllegalArgumentException if {@code trieNodeIndexEnabled} is {@code true} but {@code
   *     trieNodeCheckpointInterval} is {@code null}
   */
  public BonsaiArchiveTrieNodeStrategy(
      final Long trieNodeCheckpointInterval,
      final BonsaiCachedMerkleTrieLoader trieLoader,
      final TrieNodeStrategy baseStrategy,
      final boolean trieNodeIndexEnabled,
      final TrieNodeHistoryStore historyStore,
      final TrieNodeChangeIndex changeIndex,
      final TrieNodeIndexProgress progress) {
    if (trieNodeIndexEnabled && trieNodeCheckpointInterval == null) {
      throw new IllegalArgumentException(
          "trieNodeCheckpointInterval must not be null when trieNodeIndexEnabled=true");
    }
    this.trieNodeCheckpointInterval = trieNodeCheckpointInterval;
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
    Bytes keyNearest =
        BonsaiArchiveKeyUtil.calculateArchiveKeyWithMaxSuffix(
            BonsaiArchiveKeyUtil.getStateArchiveContextForRead(storage), location.toArrayUnsafe());
    return storage
        .getNearestBeforeMatchLength(TRIE_BRANCH_STORAGE_ARCHIVE, keyNearest)
        .filter(
            found -> found.key().size() == location.size() + BonsaiArchiveKeyUtil.KEY_SUFFIX_LENGTH)
        .filter(found -> location.commonPrefixLength(found.key()) >= location.size())
        .flatMap(SegmentedKeyValueStorage.NearestKeyValue::wrapBytes)
        .or(() -> baseStrategy.getFlatAccountTrieNode(location, nodeHash, storage));
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    Bytes accountHashLocation = Bytes.concatenate(accountHash.getBytes(), location);
    Bytes keyNearest =
        BonsaiArchiveKeyUtil.calculateArchiveKeyWithMaxSuffix(
            BonsaiArchiveKeyUtil.getStateArchiveContextForRead(storage),
            accountHashLocation.toArrayUnsafe());
    return storage
        .getNearestBeforeMatchLength(TRIE_BRANCH_STORAGE_ARCHIVE, keyNearest)
        .filter(
            found ->
                found.key().size()
                    == accountHash.getBytes().size()
                        + location.size()
                        + BonsaiArchiveKeyUtil.KEY_SUFFIX_LENGTH)
        .filter(
            found ->
                accountHashLocation.commonPrefixLength(found.key()) >= accountHashLocation.size())
        .flatMap(SegmentedKeyValueStorage.NearestKeyValue::wrapBytes)
        .or(() -> baseStrategy.getFlatStorageTrieNode(accountHash, location, nodeHash, storage));
  }

  /**
   * Controls whether this strategy writes suffixed nodes to {@code TRIE_BRANCH_STORAGE_ARCHIVE}.
   *
   * <p>When the trie-node differential index is enabled, the suffixed CF is superseded for proofs:
   * {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveProofNodeLoader}
   * reconstructs historical state directly from the index without a {@code seekForPrev} scan. The
   * live block-import path therefore skips suffixed-CF writes to avoid redundant I/O and space.
   *
   * <p>The archive migrator (via {@link BonsaiArchiveMigrationTrieNodeStrategy}) overrides this to
   * return {@code true} unconditionally, because the migrator's checkpoint persist writes are
   * consumed by the same migrator's subsequent checkpoint reads — the index alone is not sufficient
   * during migration replay.
   *
   * @return {@code true} if suffixed nodes should be written to {@code TRIE_BRANCH_STORAGE_ARCHIVE}
   */
  protected boolean shouldWriteSuffixedCf() {
    // Live block-import: skip when the index supersedes the suffixed CF for proofs.
    return !trieNodeIndexEnabled;
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

    if (trieNodeCheckpointInterval != null) {
      if (shouldWriteSuffixedCf()) {
        ensureIntervalSeeded(storage);
        final BonsaiContext ctx = getStateTrieArchiveContextForWrite(storage);
        byte[] keySuffixed =
            BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix(ctx, location.toArrayUnsafe());
        transaction.put(TRIE_BRANCH_STORAGE_ARCHIVE, keySuffixed, node.toArrayUnsafe());
        LOG.trace(
            "Archive account trie node written: location={} suffix={}",
            location,
            ctx.getBlockNumber().orElse(-1L));
      }
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

    if (trieNodeCheckpointInterval != null) {
      if (shouldWriteSuffixedCf()) {
        ensureIntervalSeeded(storage);
        final BonsaiContext ctx = getStateTrieArchiveContextForWrite(storage);
        byte[] keySuffixed =
            BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix(
                ctx, accountHashLocation.toArrayUnsafe());
        transaction.put(TRIE_BRANCH_STORAGE_ARCHIVE, keySuffixed, node.toArrayUnsafe());
        LOG.trace(
            "Archive storage trie node written: account={} location={} suffix={}",
            accountHash,
            location,
            ctx.getBlockNumber().orElse(-1L));
      }
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
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
  }

  private BonsaiContext getStateTrieArchiveContextForWrite(final SegmentedKeyValueStorage storage) {
    Optional<byte[]> proofBlockNumber =
        storage.get(TRIE_BRANCH_STORAGE, ARCHIVE_PROOF_BLOCK_NUMBER_KEY);
    if (proofBlockNumber.isPresent()) {
      return new BonsaiContext(Bytes.wrap(proofBlockNumber.get()).toLong());
    }
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(
            bytes -> {
              long blockNumber = Bytes.wrap(bytes).toLong();
              long windowStart =
                  ((blockNumber + 1) / trieNodeCheckpointInterval) * trieNodeCheckpointInterval;
              return new BonsaiContext(windowStart);
            })
        .orElse(new BonsaiContext(0L));
  }

  private void ensureIntervalSeeded(final SegmentedKeyValueStorage storage) {
    if (intervalSeeded) return;
    synchronized (this) {
      if (intervalSeeded) return;
      storage
          .get(TRIE_BRANCH_STORAGE_ARCHIVE, ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY)
          .ifPresentOrElse(
              persistedBytes -> {
                long persisted = Bytes.wrap(persistedBytes).toLong();
                if (persisted != trieNodeCheckpointInterval) {
                  throw new RuntimeException(
                      "Checkpoint interval mismatch (DB="
                          + persisted
                          + ", config="
                          + trieNodeCheckpointInterval
                          + ")");
                }
              },
              () -> {
                SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
                tx.put(
                    TRIE_BRANCH_STORAGE_ARCHIVE,
                    ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY,
                    Bytes.ofUnsignedLong(trieNodeCheckpointInterval).toArrayUnsafe());
                tx.commit();
              });
      intervalSeeded = true;
    }
  }

  /**
   * Returns the actual block number currently being written (not the window-start used for suffix
   * keying). If {@code ARCHIVE_PROOF_BLOCK_NUMBER_KEY} is set it is used directly; otherwise it is
   * {@code WORLD_BLOCK_NUMBER_KEY + 1} (the next block whose trie nodes are being committed).
   */
  private long getCurrentBlockNumber(final SegmentedKeyValueStorage storage) {
    final Optional<byte[]> proofBlock =
        storage.get(TRIE_BRANCH_STORAGE, ARCHIVE_PROOF_BLOCK_NUMBER_KEY);
    if (proofBlock.isPresent()) {
      return Bytes.wrap(proofBlock.get()).toLong();
    }
    // TODO: block 1's trie nodes are indexed at block 0 because WORLD_BLOCK_NUMBER_KEY
    // is written in the same tx as the trie nodes and hasn't committed yet. Callers
    // querying history at block 0 may see block-1 state. Fix: read from a pre-committed
    // block-number key or pass the block number explicitly from the persist() caller.
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(b -> Bytes.wrap(b).toLong() + 1L)
        .orElse(0L);
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
   * <p>The bloom accumulator ({@link #pendingBlooms}) is updated but not flushed; callers must
   * invoke {@link #flushPendingBlooms(SegmentedKeyValueStorageTransaction)} before committing the
   * transaction.
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

    // Compute the current mutation count for this key to determine whether this mutation is a
    // checkpoint. Delegated to TrieNodeChangeIndex so the packed-format parsing uses the canonical
    // SUBCOUNT_BYTES and ENTRY_BYTES constants from the archiveindex package.
    final long currentMutationCount = changeIndex.countMutationsUpTo(naturalKey, block - 1);

    final Bytes entry;
    if (priorNode == null) {
      // Creation: no prior node → always FULL | CREATION.
      entry = TrieNodeDiffCodec.encodeDiff(null, newNode);
    } else if (location.size() <= FULL_ABOVE_DEPTH) {
      // Upper-trie node: always FULL to keep proof lookups cheap. Uses the nibble-path location
      // so that both account nodes (naturalKey == location) and storage nodes (naturalKey =
      // accountHash ‖ location) are evaluated against the same depth threshold.
      entry = TrieNodeDiffCodec.encodeFull(newNode);
    } else if (currentMutationCount % CHECKPOINT_INTERVAL == 0) {
      // Checkpoint mutation: store FULL so backward walk terminates within CHECKPOINT_INTERVAL
      // steps.
      entry = TrieNodeDiffCodec.encodeFull(newNode);
    } else {
      // Normal mutation: store the structural delta.
      entry = TrieNodeDiffCodec.encodeDiff(priorNode, newNode);
    }

    historyStore.put(tx, naturalKey, block, entry);
    changeIndex.appendListAndMarkerOnly(tx, naturalKey, block);
    changeIndex.accumulateBloom(pendingBlooms, naturalKey, block);

    LOG.trace(
        "Diff-index entry captured: key={} block={} entryType=0x{}",
        naturalKey,
        block,
        String.format("%02x", entry.get(0)));
  }

  /**
   * Writes accumulated per-range bloom bits (at range boundaries only) and advances coverage
   * progress for the current block.
   *
   * <p>The bloom filter is a 128 KiB byte array per range. Writing it on every block is pure GC
   * pressure; correctness only requires the bloom to be present when a range is complete (the
   * range-marker check provides the authoritative answer for individual blocks). Therefore the
   * bloom flush is deferred: it is only written to {@code tx} when the current block is the last
   * block of its range (i.e. {@code (block + 1) % rangeSize == 0}). On all other blocks the
   * in-memory accumulator is retained for the next block.
   *
   * <p>Progress advancement (when {@code progress} is non-null and the index is enabled) happens on
   * every block:
   *
   * <ol>
   *   <li>Reads the current block number {@code N} from committed storage.
   *   <li>Calls {@link TrieNodeIndexProgress#setLastIndexedBlock(long)} with {@code N}.
   *   <li>Calls {@link TrieNodeIndexProgress#setIndexStartBlock(long)} with the start of {@code
   *       N}'s range.
   *   <li>If {@code N} is the last block in its range, also calls {@link
   *       TrieNodeIndexProgress#markRangeComplete(long)} and flushes the bloom accumulator.
   *   <li>Persists the updated progress via {@link TrieNodeIndexProgress#save}.
   * </ol>
   *
   * <p><strong>Migrator equivalent:</strong> the archive migrator does <em>not</em> call this 2-arg
   * overload (the live-block path). Instead it calls the 1-arg {@link
   * #flushPendingBlooms(SegmentedKeyValueStorageTransaction)} for blooms, then delegates progress
   * advancement to {@code BonsaiFlatDbToArchiveMigrator.advanceMigrationIndexProgress} which uses
   * the known migration block number rather than reading {@code WORLD_BLOCK_NUMBER_KEY} from live
   * storage. Keep the progress mutations in sync between the two paths.
   *
   * @param tx the transaction on which to write bloom entries and the updated progress bytes
   * @param storage committed storage used to read the current block number for progress advancement
   */
  public void flushPendingBlooms(
      final SegmentedKeyValueStorageTransaction tx, final SegmentedKeyValueStorage storage) {
    final long block = getCurrentBlockNumber(storage);
    // Flush the bloom on every block. The earlier deferral-to-range-boundary optimisation caused
    // correctness failures: an empty bloom produces false negatives in modifiedAfter(), making the
    // fast-path return the live trie node for nodes that DID change after T, causing hash
    // mismatches
    // and silent proof failures. Now that the O(n²) RangeRelativeOffsetList.append is fixed, the
    // 128 KiB bloom write per block is no longer a significant GC source.
    if (!pendingBlooms.isEmpty()) {
      changeIndex.flushBloomAccumulator(tx, pendingBlooms);
      pendingBlooms.clear();
    }
    final boolean atRangeBoundary = (block + 1) % ArchiveNodeKey.RANGE_SIZE == 0;
    if (trieNodeIndexEnabled && progress != null) {
      progress.setLastIndexedBlock(block);
      final long rangeId = block / ArchiveNodeKey.RANGE_SIZE;
      progress.setIndexStartBlock(rangeId * ArchiveNodeKey.RANGE_SIZE);
      if (atRangeBoundary) {
        progress.markRangeComplete(rangeId);
      }
      progress.save(tx);
    }
  }

  /**
   * Convenience overload that flushes blooms without advancing coverage progress.
   *
   * <p>This signature is preserved for callers that do not have a committed {@link
   * SegmentedKeyValueStorage} reference at flush time (e.g. tests that construct the strategy
   * without progress wiring). When a storage reference is available, prefer {@link
   * #flushPendingBlooms(SegmentedKeyValueStorageTransaction, SegmentedKeyValueStorage)}.
   *
   * <p><strong>NOTE:</strong> progress advancement is intentionally skipped here. The archive
   * migrator calls this 1-arg overload for bloom flushing and then delegates progress advancement
   * to {@code BonsaiFlatDbToArchiveMigrator.advanceMigrationIndexProgress} separately.
   *
   * @param tx the transaction on which to write bloom entries
   */
  public void flushPendingBlooms(final SegmentedKeyValueStorageTransaction tx) {
    if (!pendingBlooms.isEmpty()) {
      changeIndex.flushBloomAccumulator(tx, pendingBlooms);
      pendingBlooms.clear();
    }
  }
}
