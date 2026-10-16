/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Arrays;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * Benchmark-only: rewinds the trie-node archive to a block by deleting newer history entries and
 * moving coverage back. Removed together with the archive roller benchmark.
 */
public final class ArchiveTruncation {

  private static final int DELETE_CHUNK = 10_000;

  private ArchiveTruncation() {}

  public static void truncateTo(final SegmentedKeyValueStorage storage, final long block) {
    final ArchiveCoverageTracker coverage = new ArchiveCoverageTracker(storage);
    final long last =
        coverage
            .lastIndexedBlock()
            .orElseThrow(() -> new IllegalArgumentException("archive has no coverage"));
    checkArgument(block <= last, "cannot truncate to %s above coverage %s", block, last);
    final long start =
        Bytes.wrap(
                storage
                    .get(TRIE_BRANCH_STORAGE_ARCHIVE, ArchiveCoverageTracker.COVERAGE_KEY)
                    .orElseThrow())
            .getLong(0);
    checkArgument(block >= start, "cannot truncate to %s below coverage start %s", block, start);

    final List<byte[]> doomed =
        storage
            .streamKeys(TRIE_BRANCH_STORAGE_ARCHIVE)
            .filter(key -> !Arrays.equals(key, ArchiveCoverageTracker.COVERAGE_KEY))
            .filter(key -> ArchiveNodeKey.blockFromHistoryKey(Bytes.wrap(key)) > block)
            .toList();
    for (int i = 0; i < doomed.size(); i += DELETE_CHUNK) {
      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      doomed
          .subList(i, Math.min(doomed.size(), i + DELETE_CHUNK))
          .forEach(key -> tx.remove(TRIE_BRANCH_STORAGE_ARCHIVE, key));
      tx.commit();
    }
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        ArchiveCoverageTracker.COVERAGE_KEY,
        Bytes.concatenate(Bytes.ofUnsignedLong(start), Bytes.ofUnsignedLong(block))
            .toArrayUnsafe());
    tx.commit();
  }
}
