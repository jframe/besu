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
package org.hyperledger.besu.cli.subcommands.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ChangeCountResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieLogChangeCounter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieShapeModel;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.junit.jupiter.api.Test;

class TrieNodeHistoryEstimateSubCommandTest {

  private static final Address TEST_ADDRESS =
      Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");

  @Test
  void countRangeDecodesStoredTrieLogsAndCountsRootWrites() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final MutableBlockchain blockchain = createInMemoryBlockchain(gen.genesisBlock());
    final SegmentedInMemoryKeyValueStorage trieLogStorage = new SegmentedInMemoryKeyValueStorage();
    appendBlocks(blockchain, gen, 2);
    storeAccountCreationTrieLogs(blockchain, trieLogStorage, 1L, 2L);

    final long[] leafCountByRange = {1L};
    final TrieLogChangeCounter counter = new TrieLogChangeCounter(2, 10, new TrieShapeModel(16));

    final ChangeCountResult result =
        TrieNodeHistoryEstimateSubCommand.countRange(
            blockchain, trieLogStorage, 1L, 3L, counter, leafCountByRange);

    // Each of the two blocks writes the account-trie root (depth 0), deduped per block.
    assertThat(result.mutationsByDepth()[0]).isGreaterThanOrEqualTo(2L);
    // Both blocks create TEST_ADDRESS (prior == null) → +1 leaf delta each, in range 0.
    assertThat(result.accountDeltaByRange()[0]).isEqualTo(2L);
  }

  @Test
  void countRangeFailsFastWhenTrieLogMissing() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final MutableBlockchain blockchain = createInMemoryBlockchain(gen.genesisBlock());
    final SegmentedInMemoryKeyValueStorage trieLogStorage = new SegmentedInMemoryKeyValueStorage();
    appendBlocks(blockchain, gen, 1);
    // Deliberately store no trie log for block 1.

    final long[] leafCountByRange = {1L};
    final TrieLogChangeCounter counter = new TrieLogChangeCounter(2, 10, new TrieShapeModel(16));

    assertThatThrownBy(
            () ->
                TrieNodeHistoryEstimateSubCommand.countRange(
                    blockchain, trieLogStorage, 1L, 2L, counter, leafCountByRange))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("1");
  }

  private static void appendBlocks(
      final MutableBlockchain blockchain, final BlockDataGenerator gen, final int count) {
    final Block head =
        blockchain.getBlockByNumber(blockchain.getChainHeadBlockNumber()).orElseThrow();
    for (final Block block : gen.blockSequence(head, count)) {
      blockchain.appendBlock(block, gen.receipts(block));
    }
  }

  private static void storeAccountCreationTrieLogs(
      final MutableBlockchain blockchain,
      final SegmentedInMemoryKeyValueStorage trieLogStorage,
      final long... blockNumbers) {
    final TrieLogFactoryImpl factory = new TrieLogFactoryImpl();
    final var tx = trieLogStorage.startTransaction();
    for (final long n : blockNumbers) {
      final Hash blockHash = blockchain.getBlockHeader(n).orElseThrow().getHash();
      final TrieLogLayer layer = new TrieLogLayer();
      layer.setBlockHash(blockHash);
      layer.setBlockNumber(n);
      layer.addAccountChange(
          TEST_ADDRESS, null, new PmtStateTrieAccountValue(1, Wei.of(n), Hash.EMPTY, Hash.EMPTY));
      tx.put(TRIE_LOG_STORAGE, blockHash.getBytes().toArrayUnsafe(), factory.serialize(layer));
    }
    tx.commit();
  }
}
