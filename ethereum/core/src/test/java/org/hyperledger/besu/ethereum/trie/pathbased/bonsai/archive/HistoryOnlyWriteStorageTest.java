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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HistoryOnlyWriteStorageTest {

  private SegmentedKeyValueStorage delegate;
  private HistoryOnlyWriteStorage filtered;

  @BeforeEach
  void setUp() {
    delegate = new SegmentedInMemoryKeyValueStorage();
    filtered = new HistoryOnlyWriteStorage(delegate);
  }

  @Test
  void writesToHistorySegmentReachTheDelegate() {
    final SegmentedKeyValueStorageTransaction tx = filtered.startTransaction();
    tx.put(TRIE_NODE_HISTORY_ARCHIVE, new byte[] {1}, new byte[] {2});
    tx.commit();
    assertThat(delegate.get(TRIE_NODE_HISTORY_ARCHIVE, new byte[] {1})).contains(new byte[] {2});
  }

  @Test
  void writesToEveryOtherSegmentAreDiscarded() {
    final SegmentedKeyValueStorageTransaction tx = filtered.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, new byte[] {1}, new byte[] {2});
    tx.put(ACCOUNT_INFO_STATE, new byte[] {3}, new byte[] {4});
    tx.put(CODE_STORAGE, new byte[] {5}, new byte[] {6});
    tx.commit();
    assertThat(delegate.get(TRIE_BRANCH_STORAGE, new byte[] {1})).isEmpty();
    assertThat(delegate.get(ACCOUNT_INFO_STATE, new byte[] {3})).isEmpty();
    assertThat(delegate.get(CODE_STORAGE, new byte[] {5})).isEmpty();
  }

  @Test
  void removesAreFilteredTheSameWayAsPuts() {
    final SegmentedKeyValueStorageTransaction seed = delegate.startTransaction();
    seed.put(TRIE_BRANCH_STORAGE, new byte[] {1}, new byte[] {2});
    seed.put(TRIE_NODE_HISTORY_ARCHIVE, new byte[] {3}, new byte[] {4});
    seed.commit();

    final SegmentedKeyValueStorageTransaction tx = filtered.startTransaction();
    tx.remove(TRIE_BRANCH_STORAGE, new byte[] {1});
    tx.remove(TRIE_NODE_HISTORY_ARCHIVE, new byte[] {3});
    tx.commit();

    assertThat(delegate.get(TRIE_BRANCH_STORAGE, new byte[] {1})).contains(new byte[] {2});
    assertThat(delegate.get(TRIE_NODE_HISTORY_ARCHIVE, new byte[] {3})).isEmpty();
  }

  @Test
  void readsPassThroughForEverySegment() {
    final SegmentedKeyValueStorageTransaction seed = delegate.startTransaction();
    seed.put(TRIE_BRANCH_STORAGE, new byte[] {1}, new byte[] {2});
    seed.commit();
    assertThat(filtered.get(TRIE_BRANCH_STORAGE, new byte[] {1})).contains(new byte[] {2});
  }
}
