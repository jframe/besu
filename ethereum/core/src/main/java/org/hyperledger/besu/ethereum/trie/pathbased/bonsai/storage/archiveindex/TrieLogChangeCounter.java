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

import org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.TrieNodePathEnumerator;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

/**
 * Decodes a single {@link TrieLog} into per-depth trie-node write counts for the archive history
 * size estimator. Mirrors the migrator's {@code persist()} walk: every changed leaf touches the
 * nodes on its root→leaf path, deduped per block. See the design doc for the counting model.
 */
public final class TrieLogChangeCounter {

  private static final int ACCOUNT_HASH_BYTES = 32;
  private static final int SLACK = 3;

  private final int fullAboveDepth;
  private final int sampleShift;
  private final TrieShapeModel shapeModel;

  public TrieLogChangeCounter(
      final int fullAboveDepth, final int sampleShift, final TrieShapeModel shapeModel) {
    this.fullAboveDepth = fullAboveDepth;
    this.sampleShift = sampleShift;
    this.shapeModel = shapeModel;
  }

  int terminationCap(final long leafCount) {
    final int cap = (int) Math.ceil(shapeModel.expectedLeafDepth(leafCount)) + SLACK;
    return Math.min(64, Math.max(1, cap));
  }

  boolean isSampled(final Bytes naturalKey) {
    if (sampleShift <= 0) {
      return true;
    }
    final int mask = (1 << sampleShift) - 1;
    // Use the trailing bytes of the natural key (uniform hash) as the sampling hash. Shallow
    // paths (root, depth 1) are shorter than two bytes, so missing bytes contribute zero.
    final int size = naturalKey.size();
    final int last = size >= 1 ? naturalKey.get(size - 1) & 0xFF : 0;
    final int secondLast = size >= 2 ? naturalKey.get(size - 2) & 0xFF : 0;
    final int h = last | (secondLast << 8);
    return (h & mask) == 0;
  }

  public void countBlock(
      final TrieLog trieLog,
      final long blockNumber,
      final long leafCountForEra,
      final ChangeCountResult out) {
    final int cap = terminationCap(leafCountForEra);
    final Set<Bytes> accountPaths = new LinkedHashSet<>();
    final Set<Bytes> storagePaths = new LinkedHashSet<>();

    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              final Bytes accountHash = address.addressHash().getBytes();
              TrieNodePathEnumerator.addLocationPrefixes(
                  TrieNodePathEnumerator.toNibbles(accountHash), cap, null, accountPaths);
              if (change.getPrior() == null && change.getUpdated() != null) {
                out.recordAccountDelta(blockNumber, 1);
              } else if (change.getUpdated() == null && change.getPrior() != null) {
                out.recordAccountDelta(blockNumber, -1);
              }
            });

    trieLog
        .getStorageChanges()
        .forEach(
            (address, slotMap) -> {
              final Bytes accountHash = address.addressHash().getBytes();
              slotMap.forEach(
                  (slotKey, change) ->
                      TrieNodePathEnumerator.addLocationPrefixes(
                          TrieNodePathEnumerator.toNibbles(slotKey.getSlotHash().getBytes()),
                          cap,
                          accountHash,
                          storagePaths));
            });

    for (final Bytes path : accountPaths) {
      recordPath(path, path.size(), out);
    }
    for (final Bytes path : storagePaths) {
      recordPath(path, path.size() - ACCOUNT_HASH_BYTES, out);
    }
  }

  private void recordPath(final Bytes naturalKey, final int depth, final ChangeCountResult out) {
    out.recordMutation(depth, false);
    if (depth <= fullAboveDepth) {
      out.recordUpperFull(depth);
    }
    if (isSampled(naturalKey)) {
      out.recordSampledWrite(naturalKey, depth);
    }
  }
}
