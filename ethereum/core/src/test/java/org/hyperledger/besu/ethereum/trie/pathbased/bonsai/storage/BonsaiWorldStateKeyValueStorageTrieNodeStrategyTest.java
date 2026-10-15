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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.junit.jupiter.api.Test;

class BonsaiWorldStateKeyValueStorageTrieNodeStrategyTest {

  private BonsaiWorldStateKeyValueStorage createStorage() {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(),
        new NoOpMetricsSystem(),
        org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  private BonsaiWorldStateKeyValueStorage.Updater updaterWith(final TrieNodeStrategy strategy) {
    final BonsaiWorldStateKeyValueStorage storage = createStorage();
    storage.setTrieNodeStrategy(strategy);
    return storage.updater();
  }

  @Test
  void unmarkedCommitsUseOnBeforeCommit() {
    final TrieNodeStrategy strategy = mock(TrieNodeStrategy.class);

    updaterWith(strategy).commitComposedOnly();
    updaterWith(strategy).commit();

    verify(strategy, times(2)).onBeforeCommit(any(), any());
    verify(strategy, never()).onBeforeBlockCommit(any(), any());
  }

  @Test
  void markedCommitsUseOnBeforeBlockCommit() {
    final TrieNodeStrategy strategy = mock(TrieNodeStrategy.class);

    updaterWith(strategy).markBlockPersist().commitComposedOnly();
    updaterWith(strategy).markBlockPersist().commit();

    verify(strategy, times(2)).onBeforeBlockCommit(any(), any());
    verify(strategy, never()).onBeforeCommit(any(), any());
  }

  @Test
  void defaultStrategyIsBonsaiTrieNodeStrategy() {
    final BonsaiWorldStateKeyValueStorage storage = createStorage();
    assertThat(storage.getTrieNodeStrategy()).isInstanceOf(BonsaiTrieNodeStrategy.class);
  }
}
