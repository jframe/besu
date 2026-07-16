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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_FRONTIER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MigrationPrefetcherTest {

  @Test
  @SuppressWarnings("unchecked")
  void prefetchTrieNodes_readsStorageForEnumeratedKeys() {
    final SegmentedKeyValueStorage storage = mock(SegmentedKeyValueStorage.class);
    when(storage.multiGet(Mockito.any(), Mockito.anyList())).thenReturn(List.of(Optional.empty()));
    final Executor direct =
        Runnable::run; // run task synchronously on caller thread for determinism

    final TrieLog log = mock(TrieLog.class);
    // one changed account whose nibble prefixes must be looked up
    final Address address = mock(Address.class);
    when(address.addressHash()).thenReturn(Hash.ZERO);
    when(log.getAccountChanges()).thenReturn(Map.of(address, mock(TrieLog.LogTuple.class)));
    when(log.getStorageChanges()).thenReturn(Map.of());

    final MigrationPrefetcher prefetcher = new MigrationPrefetcher(storage, direct, 4, 3);
    prefetcher.prefetchTrieNodes(log);

    // Frontier CF no longer prefetched (removed in task 12 — frontier write path deleted).
    verify(storage, never()).multiGet(Mockito.eq(TRIE_BRANCH_FRONTIER), Mockito.anyList());
    verify(storage, timeout(1000)).multiGet(Mockito.eq(TRIE_BRANCH_STORAGE), Mockito.anyList());
  }

  @Test
  void prefetchTrieNodes_afterClose_isNoOp() {
    final SegmentedKeyValueStorage storage = mock(SegmentedKeyValueStorage.class);
    final MigrationPrefetcher prefetcher = new MigrationPrefetcher(storage, Runnable::run, 4, 3);
    prefetcher.close();

    final TrieLog log = mock(TrieLog.class);
    when(log.getAccountChanges()).thenReturn(Map.of());
    when(log.getStorageChanges()).thenReturn(Map.of());

    prefetcher.prefetchTrieNodes(log);

    verify(storage, never()).multiGet(Mockito.any(), Mockito.anyList());
  }
}
