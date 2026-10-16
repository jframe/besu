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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ArchiveTruncationTest {

  @Test
  void deletesHistoryAboveTheBlockAndRewindsCoverage() {
    final SegmentedKeyValueStorage storage =
        new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE_ARCHIVE));
    final ArchiveCoverageTracker coverage = new ArchiveCoverageTracker(storage);
    final Bytes natural = ArchiveNodeKey.account(Bytes.of(0x01));
    final var tx = storage.startTransaction();
    for (final long block : new long[] {3L, 5L, 9L}) {
      tx.put(
          TRIE_BRANCH_STORAGE_ARCHIVE,
          ArchiveNodeKey.historyKey(natural, block).toArrayUnsafe(),
          new byte[] {1});
    }
    tx.commit();
    // record() reads committed coverage, so commit one block at a time
    for (long block = 0; block <= 9; block++) {
      final var coverageTx = storage.startTransaction();
      coverage.record(coverageTx, block);
      coverageTx.commit();
    }

    ArchiveTruncation.truncateTo(storage, 5L);

    assertThat(history(storage, natural, 3L)).isPresent();
    assertThat(history(storage, natural, 5L)).isPresent();
    assertThat(history(storage, natural, 9L)).isEmpty();
    assertThat(coverage.lastIndexedBlock()).hasValue(5L);
    assertThat(coverage.hasArchiveBlock(0L)).isTrue();
  }

  private static java.util.Optional<byte[]> history(
      final SegmentedKeyValueStorage storage, final Bytes natural, final long block) {
    return storage.get(
        TRIE_BRANCH_STORAGE_ARCHIVE, ArchiveNodeKey.historyKey(natural, block).toArrayUnsafe());
  }

  @Test
  void refusesToTruncateAboveCoverage() {
    final SegmentedKeyValueStorage storage =
        new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE_ARCHIVE));
    final var tx = storage.startTransaction();
    new ArchiveCoverageTracker(storage).record(tx, 0L);
    tx.commit();

    assertThatThrownBy(() -> ArchiveTruncation.truncateTo(storage, 5L))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
