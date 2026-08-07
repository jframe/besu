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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BonsaiWorldStateKeyValueStorageTrieNodeStrategyTest {

  private BonsaiWorldStateKeyValueStorage createStorage() {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(),
        new NoOpMetricsSystem(),
        org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  @Test
  void defaultStrategyIsBonsaiTrieNodeStrategy() {
    final BonsaiWorldStateKeyValueStorage storage = createStorage();
    assertThat(storage.getTrieNodeStrategy()).isInstanceOf(BonsaiTrieNodeStrategy.class);
  }

  @Test
  void flushAndDiscardHooksFireOnCommitAndRollback() {
    final AtomicInteger flushes = new AtomicInteger();
    final AtomicInteger discards = new AtomicInteger();
    final TrieNodeStrategy counting =
        new BonsaiTrieNodeStrategy() {
          @Override
          public void onBeforeCommit(
              final SegmentedKeyValueStorage s, final SegmentedKeyValueStorageTransaction t) {
            flushes.incrementAndGet();
          }

          @Override
          public void onDiscard(final SegmentedKeyValueStorageTransaction t) {
            discards.incrementAndGet();
          }
        };
    final BonsaiWorldStateKeyValueStorage storage = createStorage();
    storage.setTrieNodeStrategy(counting);

    final var committing = storage.updater();
    committing.putAccountStateTrieNode(
        Bytes.EMPTY, Bytes32.wrap(Hash.hash(Bytes.of(1)).getBytes()), Bytes.of(1));
    committing.commit();
    assertThat(flushes.get()).isEqualTo(1);

    final var rollingBack = storage.updater();
    rollingBack.putAccountStateTrieNode(
        Bytes.of(0x0e), Bytes32.wrap(Hash.hash(Bytes.of(2)).getBytes()), Bytes.of(2));
    rollingBack.rollback();
    assertThat(discards.get()).isEqualTo(1);
  }
}
