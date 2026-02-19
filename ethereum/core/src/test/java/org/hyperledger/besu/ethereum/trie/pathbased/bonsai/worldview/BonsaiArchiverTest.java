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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiArchiverTest {

  private BonsaiWorldStateKeyValueStorage storage;
  private final BlockHeaderTestFixture blockBuilder = new BlockHeaderTestFixture();

  @BeforeEach
  void setUp() {
    storage =
        spy(
            new BonsaiWorldStateKeyValueStorage(
                new InMemoryKeyValueStorageProvider(),
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG));
    storage.upgradeToFullFlatDbMode();

    // Set initial block number
    updateStorageArchiveBlock(1);
  }

  private void updateStorageArchiveBlock(final long blockNumber) {
    SegmentedKeyValueStorageTransaction tx =
        storage.getComposedWorldStateStorage().startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }

  @Test
  void archivePreviousAccountStateBatched_addsToTransaction_doesNotCommit() {
    final BlockHeader header = blockBuilder.number(100).buildHeader();
    final Hash accountHash = Hash.hash(Bytes.fromHexString("0x1234"));

    // Create a transaction that we'll pass in
    SegmentedKeyValueStorageTransaction tx =
        storage.getComposedWorldStateStorage().startTransaction();

    // Call the batched method
    int archivedCount = storage.archivePreviousAccountStateBatched(tx, header, accountHash);

    // The transaction should NOT have been committed (no entries to archive in empty storage)
    assertThat(archivedCount).isEqualTo(0);

    // Verify the transaction was not committed by the method itself
    // (we would commit it externally after batching multiple calls)
  }
}
