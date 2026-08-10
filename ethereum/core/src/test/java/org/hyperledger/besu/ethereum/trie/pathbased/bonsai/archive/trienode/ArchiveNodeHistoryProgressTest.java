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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.junit.jupiter.api.Test;

class ArchiveNodeHistoryProgressTest {
  @Test
  void emptyProgressCoversNothing() {
    final ArchiveNodeHistoryProgress p = new ArchiveNodeHistoryProgress();
    assertThat(p.covers(0)).isFalse();
  }

  @Test
  void coversWithinRecordedWindow() {
    final ArchiveNodeHistoryProgress p = new ArchiveNodeHistoryProgress();
    p.setIndexStartBlock(0);
    p.setLastIndexedBlock(10);
    assertThat(p.covers(0)).isTrue();
    assertThat(p.covers(10)).isTrue();
    assertThat(p.covers(11)).isFalse();
  }

  @Test
  void savesAndLoads() {
    final SegmentedKeyValueStorage storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE));
    final ArchiveNodeHistoryProgress p = new ArchiveNodeHistoryProgress();
    p.setIndexStartBlock(3);
    p.setLastIndexedBlock(9);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    p.save(tx);
    tx.commit();
    final ArchiveNodeHistoryProgress loaded = ArchiveNodeHistoryProgress.load(storage);
    assertThat(loaded.indexStartBlock).isEqualTo(3);
    assertThat(loaded.lastIndexedBlock).isEqualTo(9);
  }
}
