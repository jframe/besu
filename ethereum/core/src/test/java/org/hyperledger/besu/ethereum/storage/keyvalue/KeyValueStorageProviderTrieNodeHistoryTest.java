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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import org.junit.jupiter.api.Test;

class KeyValueStorageProviderTrieNodeHistoryTest {

  private KeyValueStorageProvider newProvider() {
    return new InMemoryKeyValueStorageProvider();
  }

  private DataStorageConfiguration config(
      final DataStorageFormat format, final boolean trieNodeHistoryEnabled) {
    return ImmutableDataStorageConfiguration.builder()
        .dataStorageFormat(format)
        .pathBasedExtraStorageConfiguration(
            ImmutablePathBasedExtraStorageConfiguration.builder()
                .unstable(
                    ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                        .trieNodeHistoryEnabled(trieNodeHistoryEnabled)
                        .build())
                .build())
        .build();
  }

  @Test
  void archiveFormatWithFlagEnabledInstallsArchiveTrieNodeStrategy() {
    final BonsaiWorldStateKeyValueStorage storage =
        (BonsaiWorldStateKeyValueStorage)
            newProvider().createWorldStateStorage(config(DataStorageFormat.X_BONSAI_ARCHIVE, true));
    assertThat(storage.getTrieNodeStrategy()).isInstanceOf(BonsaiArchiveTrieNodeStrategy.class);
  }

  @Test
  void archiveFormatWithFlagDisabledKeepsPlainStrategy() {
    final BonsaiWorldStateKeyValueStorage storage =
        (BonsaiWorldStateKeyValueStorage)
            newProvider()
                .createWorldStateStorage(config(DataStorageFormat.X_BONSAI_ARCHIVE, false));
    assertThat(storage.getTrieNodeStrategy()).isInstanceOf(BonsaiTrieNodeStrategy.class);
    assertThat(storage.getTrieNodeStrategy()).isNotInstanceOf(BonsaiArchiveTrieNodeStrategy.class);
  }

  @Test
  void plainBonsaiFormatNeverInstallsArchiveStrategyEvenWithFlagEnabled() {
    // The feature is archive-only: TRIE_NODE_HISTORY_ARCHIVE is scoped to X_BONSAI_ARCHIVE
    // (Task 6), so enabling the flag on plain BONSAI must be a no-op rather than writing to a
    // column family that does not exist for that format.
    final BonsaiWorldStateKeyValueStorage storage =
        (BonsaiWorldStateKeyValueStorage)
            newProvider().createWorldStateStorage(config(DataStorageFormat.BONSAI, true));
    assertThat(storage.getTrieNodeStrategy()).isInstanceOf(BonsaiTrieNodeStrategy.class);
  }
}
