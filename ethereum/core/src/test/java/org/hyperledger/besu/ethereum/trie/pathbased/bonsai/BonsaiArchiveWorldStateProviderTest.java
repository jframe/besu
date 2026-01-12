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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiArchiveInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;

import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiArchiveWorldStateProviderTest {

  private final BlockHeaderTestFixture blockHeaderBuilder = new BlockHeaderTestFixture();
  private MutableBlockchain blockchain;
  private BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;
  private BonsaiArchiveWorldStateProvider archiveWorldStateProvider;

  @BeforeEach
  public void setUp() {
    final Block genesisBlock = createGenesisBlock();
    blockchain = createInMemoryBlockchain(genesisBlock);
    archiveWorldStateProvider = createBonsaiArchiveInMemoryWorldStateArchive(blockchain);
    worldStateKeyValueStorage =
        (BonsaiWorldStateKeyValueStorage) archiveWorldStateProvider.getWorldStateKeyValueStorage();
    archiveWorldStateProvider.getWorldState().persist(genesisBlock.getHeader());
  }

  private Block createGenesisBlock() {
    final BlockHeader genesisHeader =
        blockHeaderBuilder.number(0).difficulty(Difficulty.ONE).buildHeader();
    return new Block(
        genesisHeader, new BlockBody(Collections.emptyList(), Collections.emptyList()));
  }

  private Block createAndAppendBlock(final BlockHeader parentHeader, final long blockNumber) {
    final BlockHeader header =
        blockHeaderBuilder
            .number(blockNumber)
            .parentHash(parentHeader.getHash())
            .difficulty(Difficulty.ONE)
            .buildHeader();
    final Block block =
        new Block(header, new BlockBody(Collections.emptyList(), Collections.emptyList()));
    blockchain.appendBlock(block, Collections.emptyList());
    return block;
  }

  @Test
  void getWorldState_updatesWorldBlockNumber_whenRollingBackToEarlierBlock() {
    // Given: Blockchain with blocks 0, 1, 2, 3, 4, 5 and world state at block 5
    BlockHeader currentHeader = blockchain.getGenesisBlock().getHeader();

    for (long i = 1; i <= 5; i++) {
      Block block = createAndAppendBlock(currentHeader, i);
      currentHeader = block.getHeader();
      archiveWorldStateProvider.getWorldState().persist(currentHeader);
    }

    assertThat(worldStateKeyValueStorage.getWorldStateBlockNumber()).contains(5L);

    // Request world state at block 3 with updateHead=true
    final BlockHeader blockHeader3 = blockchain.getBlockHeader(3).orElseThrow();
    Optional<BonsaiWorldState> worldState =
        archiveWorldStateProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(blockHeader3))
            .map(BonsaiWorldState.class::cast);

    assertThat(worldState).isPresent();
    assertThat(worldState.get().getWorldStateBlockHash()).isEqualTo(blockHeader3.getBlockHash());
    assertThat(worldStateKeyValueStorage.getWorldStateBlockNumber()).contains(3L);
  }

  @Test
  void getWorldState_returnsCorrectState_whenBlockNumbersMatch() {
    Block block1 = createAndAppendBlock(blockchain.getGenesisBlock().getHeader(), 1);
    archiveWorldStateProvider.getWorldState().persist(block1.getHeader());

    // Request world state at block 1 (same as current state)
    Optional<BonsaiWorldState> worldState =
        archiveWorldStateProvider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(block1.getHeader()))
            .map(BonsaiWorldState.class::cast);

    assertThat(worldState).isPresent();
    assertThat(worldState.get().getWorldStateBlockHash())
        .isEqualTo(block1.getHeader().getBlockHash());
    assertThat(worldStateKeyValueStorage.getWorldStateBlockNumber()).contains(1L);
  }

  @Test
  void getWorldState_canRollForward_afterRollback() {
    BlockHeader currentHeader = blockchain.getGenesisBlock().getHeader();

    for (long i = 1; i <= 5; i++) {
      Block block = createAndAppendBlock(currentHeader, i);
      currentHeader = block.getHeader();
      archiveWorldStateProvider.getWorldState().persist(currentHeader);
    }

    // Roll back to block 3
    final BlockHeader blockHeader3 = blockchain.getBlockHeader(3).orElseThrow();
    archiveWorldStateProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(blockHeader3));

    assertThat(worldStateKeyValueStorage.getWorldStateBlockNumber()).contains(3L);

    // Roll forward to block 5
    final BlockHeader blockHeader5 = blockchain.getBlockHeader(5).orElseThrow();
    Optional<BonsaiWorldState> worldState =
        archiveWorldStateProvider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(blockHeader5))
            .map(BonsaiWorldState.class::cast);

    assertThat(worldState).isPresent();
    assertThat(worldState.get().getWorldStateBlockHash()).isEqualTo(blockHeader5.getBlockHash());
    assertThat(worldStateKeyValueStorage.getWorldStateBlockNumber()).contains(5L);
  }
}
