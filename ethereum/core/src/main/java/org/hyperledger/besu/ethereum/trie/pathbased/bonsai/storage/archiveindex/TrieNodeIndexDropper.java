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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops all history-archive and index entries for a specific block number (Design 5, Task 3.5).
 *
 * <p>This class implements reorg cleanup: when Bonsai rolls back a block, the diff-codec entries
 * written to {@code TRIE_NODE_HISTORY_ARCHIVE} and the change-block offsets written to {@code
 * TRIE_NODE_INDEX_ARCHIVE} for that block must be removed to keep the index consistent.
 *
 * <h3>Algorithm (Option A — scan)</h3>
 *
 * <p>Because there is no secondary index mapping block numbers to natural keys, {@link
 * #dropBlock(long, SegmentedKeyValueStorage, SegmentedKeyValueStorageTransaction)} scans the full
 * {@code TRIE_NODE_HISTORY_ARCHIVE} column family and filters entries whose key ends in the 8-byte
 * big-endian encoding of the target block number. For each match:
 *
 * <ol>
 *   <li>Delete the history entry {@code TRIE_NODE_HISTORY_ARCHIVE[naturalKey ‖ block]}.
 *   <li>Remove the offset for {@code block} from the offset list at {@code
 *       TRIE_NODE_INDEX_ARCHIVE[naturalKey ‖ rangeId]}. If the resulting list is empty, also remove
 *       the {@code TRIE_NODE_RANGE_MARKER_ARCHIVE[naturalKey ‖ rangeId]} marker. The range bloom is
 *       <em>not</em> updated (bloom removal is not supported; leaving the false-positive bit in the
 *       bloom is safe but will cause harmless extra index-list lookups — see TODO below).
 * </ol>
 *
 * <p>Reorgs are rare and depth-bounded, so the full CF scan is acceptable. On a live mainnet node
 * with millions of history entries the scan cost is bounded by the I/O for one RocksDB iterator
 * pass over the column family.
 *
 * <p>This class is flag-gated: the caller must check {@code trieNodeIndexEnabled} before calling
 * {@link #dropBlock}.
 *
 * <h3>Rollback hook</h3>
 *
 * <p>TODO: wire {@link #dropBlock} into the Bonsai rollback hook. Investigation findings:
 *
 * <ul>
 *   <li>Rollback is initiated in {@code PathBasedWorldStateProvider#handleReorg} and similar
 *       methods. The concrete Bonsai rollback path is {@code
 *       BonsaiArchiveWorldStateProvider.rollback()} (line ~234), which forwards to {@code
 *       super.handleReorg()} in {@code PathBasedWorldStateProvider}.
 *   <li>The rollback path has access to the block number via the trie-log or the block header being
 *       rolled back.
 *   <li>Neither {@code BonsaiArchiveTrieNodeStrategy} nor this class is currently notified of
 *       rollbacks. The wiring point should be in the trie-log consumer or world-state provider at
 *       the point where the storage transaction for the rolled-back block is prepared.
 * </ul>
 */
public final class TrieNodeIndexDropper {

  private static final Logger LOG = LoggerFactory.getLogger(TrieNodeIndexDropper.class);

  /**
   * Number of bytes in the subCount prefix of each {@code TRIE_NODE_INDEX_ARCHIVE} value.
   *
   * <p>Must match {@code TrieNodeChangeIndex.SUBCOUNT_BYTES} (currently {@code 4}). If that
   * constant ever changes, this value must be updated to match.
   */
  // must match TrieNodeChangeIndex.SUBCOUNT_BYTES
  private static final int SUBCOUNT_BYTES = 4;

  /** Number of bytes per packed offset entry in {@link RangeRelativeOffsetList}. */
  private static final int ENTRY_BYTES = RangeRelativeOffsetList.ENTRY_BYTES;

  private final long rangeSize;

  /**
   * Constructs a new dropper with the canonical range size for Design 5.
   *
   * @see ArchiveNodeKey#RANGE_SIZE
   */
  public TrieNodeIndexDropper() {
    this(ArchiveNodeKey.RANGE_SIZE);
  }

  /**
   * Package-private constructor for testing with a custom range size.
   *
   * @param rangeSize blocks per range; must equal {@link ArchiveNodeKey#RANGE_SIZE} for production
   *     use
   */
  TrieNodeIndexDropper(final long rangeSize) {
    if (rangeSize <= 0) {
      throw new IllegalArgumentException("rangeSize must be > 0, got " + rangeSize);
    }
    this.rangeSize = rangeSize;
  }

  /**
   * Drops all history-archive and index entries for {@code blockNumber} from committed storage via
   * {@code tx}.
   *
   * <p>The caller is responsible for committing {@code tx} after this method returns. If {@code
   * blockNumber} was never captured in the index (e.g. the flag was disabled at that block) this
   * method is a no-op.
   *
   * <p><strong>Flag gate:</strong> the caller must check the {@code trieNodeIndexEnabled} flag
   * before invoking this method.
   *
   * @param blockNumber the block to drop; must be &gt;= 0
   * @param storage committed storage used to read the current index values and to scan the history
   *     CF; reads in this method go to committed storage (pre-{@code tx})
   * @param tx the transaction on which to issue all deletes and updates
   * @throws IllegalArgumentException if {@code blockNumber} is negative
   * @throws NullPointerException if {@code storage} or {@code tx} is {@code null}
   */
  public void dropBlock(
      final long blockNumber,
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction tx) {
    if (blockNumber < 0) {
      throw new IllegalArgumentException("blockNumber must be >= 0, got " + blockNumber);
    }
    Objects.requireNonNull(storage, "storage must not be null");
    Objects.requireNonNull(tx, "tx must not be null");

    final Bytes blockSuffix = Bytes.ofUnsignedLong(blockNumber);
    final long rangeId = blockNumber / rangeSize;
    final int offset = (int) (blockNumber - rangeId * rangeSize);

    // Scan TRIE_NODE_HISTORY_ARCHIVE for all entries keyed by "naturalKey ‖ blockNumber".
    // Keys ending in blockSuffix (8 bytes BE) are exactly the entries recorded at this block.
    final long[] dropped = {0};
    storage.stream(KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE)
        .filter(
            entry -> {
              final Bytes key = Bytes.wrap(entry.getKey());
              // Must be at least 9 bytes (1 byte naturalKey + 8 bytes block suffix).
              if (key.size() < 9) {
                return false;
              }
              // Check that the last 8 bytes equal the target block number.
              return key.slice(key.size() - 8).equals(blockSuffix);
            })
        .forEach(
            entry -> {
              final Bytes historyKey = Bytes.wrap(entry.getKey());
              final Bytes naturalKey = ArchiveNodeKey.naturalKeyFromHistoryKey(historyKey);

              // 1. Delete the history entry.
              tx.remove(
                  KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE, historyKey.toArrayUnsafe());

              // 2. Remove the offset from the index list.
              dropOffsetFromIndex(naturalKey, rangeId, offset, storage, tx);

              dropped[0]++;
            });

    LOG.debug("dropBlock({}): dropped {} history+index entries", blockNumber, dropped[0]);
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  /**
   * Removes {@code offset} from the packed offset list at {@code TRIE_NODE_INDEX_ARCHIVE[naturalKey
   * ‖ rangeId]}.
   *
   * <p>If the resulting list is empty, also removes the range-marker at {@code
   * TRIE_NODE_RANGE_MARKER_ARCHIVE[naturalKey ‖ rangeId]}.
   *
   * <p>The per-range bloom is intentionally NOT updated. Bloom removal is not supported (clearing a
   * bit would require knowing it was not set by another key, which requires iterating all keys in
   * the range). The residual bloom bit is a false positive; the downstream code (range-marker check
   * + offset-list lookup) will correctly return no hit after the marker/list are cleaned up.
   *
   * <p>TODO: if the sub-block chain ({@code TRIE_NODE_SUBBLOCK_ARCHIVE}) contains the offset (i.e.
   * the key was hot and the offset was evicted from the tail into a sub-block before the reorg),
   * sub-block cleanup is not yet implemented. Sub-block reorg cleanup is deferred to a future task
   * because deep reorgs on hot keys are extremely rare and the false-positive sub-block entry is
   * harmless (the history entry has already been deleted above, so the reader will get no result).
   *
   * @param naturalKey the node's natural key
   * @param rangeId the range identifier for {@code blockNumber}
   * @param offset the within-range offset ({@code blockNumber - rangeId * rangeSize})
   * @param storage committed storage for reading the current index value
   * @param tx the transaction on which to issue writes
   */
  private void dropOffsetFromIndex(
      final Bytes naturalKey,
      final long rangeId,
      final int offset,
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction tx) {

    final Bytes indexKey = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    final byte[] indexKeyBytes = indexKey.toArrayUnsafe();

    final byte[] raw =
        storage.get(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes).orElse(null);
    if (raw == null || raw.length < SUBCOUNT_BYTES) {
      // No index entry — nothing to remove.
      return;
    }

    // Parse [4B subCount][packed 3-byte offsets]
    final int subCount =
        ((raw[0] & 0xFF) << 24)
            | ((raw[1] & 0xFF) << 16)
            | ((raw[2] & 0xFF) << 8)
            | (raw[3] & 0xFF);
    final Bytes packed = Bytes.wrap(raw, SUBCOUNT_BYTES, raw.length - SUBCOUNT_BYTES);

    // Remove the target offset from the packed list by rebuilding without it.
    final int n = packed.size() / ENTRY_BYTES;
    int removed = 0;
    // Allocate for up to n entries; slice to (n - removed) * ENTRY_BYTES after the loop.
    final MutableBytes result = MutableBytes.create(n * ENTRY_BYTES);
    int dst = 0;
    for (int i = 0; i < n; i++) {
      final int base = i * ENTRY_BYTES;
      final int entryOffset =
          ((packed.get(base) & 0xFF) << 16)
              | ((packed.get(base + 1) & 0xFF) << 8)
              | (packed.get(base + 2) & 0xFF);
      if (entryOffset == offset && removed == 0) {
        // Remove exactly one occurrence (the first match, since offsets are non-decreasing and
        // a given block appears at most once).
        removed++;
        continue;
      }
      result.set(dst, packed.get(base));
      result.set(dst + 1, packed.get(base + 1));
      result.set(dst + 2, packed.get(base + 2));
      dst += ENTRY_BYTES;
    }

    if (removed == 0) {
      // Offset not found in the tail — it may be in a sub-block or was never there.
      // TODO: scan sub-blocks for the offset if sub-block support is needed.
      return;
    }

    final int remainingEntries = n - removed;
    final Bytes newPacked = result.slice(0, remainingEntries * ENTRY_BYTES);

    if (remainingEntries == 0 && subCount == 0) {
      // The list is now empty and there are no sub-blocks — remove both the index entry and the
      // range marker.
      tx.remove(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes);
      tx.remove(KeyValueSegmentIdentifier.TRIE_NODE_RANGE_MARKER_ARCHIVE, indexKeyBytes);
    } else {
      // Write back the updated index value with the same subCount (sub-blocks are unchanged).
      final byte[] updated = new byte[SUBCOUNT_BYTES + newPacked.size()];
      updated[0] = (byte) ((subCount >>> 24) & 0xFF);
      updated[1] = (byte) ((subCount >>> 16) & 0xFF);
      updated[2] = (byte) ((subCount >>> 8) & 0xFF);
      updated[3] = (byte) (subCount & 0xFF);
      newPacked.copyTo(MutableBytes.wrap(updated, SUBCOUNT_BYTES, newPacked.size()));
      tx.put(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE, indexKeyBytes, updated);
      // Range marker remains (there are still entries in the sub-blocks or the tail).
    }
  }
}
