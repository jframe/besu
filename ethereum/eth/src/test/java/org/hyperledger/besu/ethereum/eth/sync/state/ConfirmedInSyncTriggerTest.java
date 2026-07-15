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
package org.hyperledger.besu.ethereum.eth.sync.state;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ConfirmedInSyncTriggerTest {

  private static final Difficulty standardDifficultyPerBlock = Difficulty.ONE;
  private static final long OUR_CHAIN_HEAD_NUMBER = 3;
  private static final long TARGET_CHAIN_DELTA = 20;
  private static final long TARGET_CHAIN_HEIGHT = OUR_CHAIN_HEAD_NUMBER + TARGET_CHAIN_DELTA;

  private final BlockDataGenerator gen = new BlockDataGenerator(1);
  private final Block genesisBlock =
      gen.genesisBlock(new BlockOptions().setDifficulty(Difficulty.ZERO));
  private final MutableBlockchain blockchain =
      InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesisBlock);

  private final AtomicInteger actionCount = new AtomicInteger(0);

  private EthProtocolManager ethProtocolManager;
  private EthPeers ethPeers;
  private SyncState syncState;

  @BeforeEach
  public void setUp() {
    ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setBlockchain(blockchain)
            .setWorldStateArchive(Mockito.mock(WorldStateArchive.class))
            .build();
    ethPeers = ethProtocolManager.ethContext().getEthPeers();

    advanceLocalChain(OUR_CHAIN_HEAD_NUMBER);

    syncState = new SyncState(blockchain, ethPeers);
  }

  @Test
  public void triggersWhenCaughtUpToSyncTarget() {
    final RespondingEthPeer syncTargetPeer = createPeer(TARGET_CHAIN_HEIGHT);
    ConfirmedInSyncTrigger.subscribe(syncState, 0, actionCount::incrementAndGet);

    syncState.setSyncTarget(syncTargetPeer.getEthPeer(), genesisBlock.getHeader());
    assertThat(actionCount).hasValue(0);

    advanceLocalChain(TARGET_CHAIN_HEIGHT);
    assertThat(actionCount).hasValue(1);
  }

  @Test
  public void doesNotTriggerWhenAllPeersLostDuringChainDownload() {
    final RespondingEthPeer syncTargetPeer = createPeer(TARGET_CHAIN_HEIGHT);
    ConfirmedInSyncTrigger.subscribe(syncState, 0, actionCount::incrementAndGet);

    // Chain download starts, then the only peer disconnects and the download fails,
    // clearing the sync target. With no peers left the in-sync check reports "in sync"
    // even though the local chain is still behind.
    syncState.setSyncTarget(syncTargetPeer.getEthPeer(), genesisBlock.getHeader());
    syncTargetPeer.disconnect(DisconnectReason.REQUESTED);
    syncState.clearSyncTarget();

    assertThat(syncState.isInSync(0)).isTrue(); // Sanity check: the spurious signal fired
    assertThat(actionCount).hasValue(0);
  }

  @Test
  public void triggersAfterRecoveryFromSpuriousInSync() {
    final RespondingEthPeer syncTargetPeer = createPeer(TARGET_CHAIN_HEIGHT);
    ConfirmedInSyncTrigger.subscribe(syncState, 0, actionCount::incrementAndGet);

    syncState.setSyncTarget(syncTargetPeer.getEthPeer(), genesisBlock.getHeader());
    syncTargetPeer.disconnect(DisconnectReason.REQUESTED);
    syncState.clearSyncTarget();
    assertThat(actionCount).hasValue(0);

    // Peer reconnects, chain download resumes and genuinely completes
    final RespondingEthPeer reconnectedPeer = createPeer(TARGET_CHAIN_HEIGHT);
    syncState.setSyncTarget(reconnectedPeer.getEthPeer(), genesisBlock.getHeader());
    assertThat(actionCount).hasValue(0);

    advanceLocalChain(TARGET_CHAIN_HEIGHT);
    assertThat(actionCount).hasValue(1);
  }

  @Test
  public void triggersOnIsolatedNodeWithoutPeers() {
    // No peers and no chain download ever started (e.g. single-node network): the
    // in-sync signal is the best information available and must still trigger.
    ConfirmedInSyncTrigger.subscribe(syncState, 0, actionCount::incrementAndGet);

    advanceLocalChain(OUR_CHAIN_HEAD_NUMBER + 1);
    assertThat(actionCount).hasValue(1);
  }

  @Test
  public void triggersOnlyOnce() {
    final RespondingEthPeer syncTargetPeer = createPeer(TARGET_CHAIN_HEIGHT);
    ConfirmedInSyncTrigger.subscribe(syncState, 0, actionCount::incrementAndGet);

    syncState.setSyncTarget(syncTargetPeer.getEthPeer(), genesisBlock.getHeader());
    advanceLocalChain(TARGET_CHAIN_HEIGHT);
    assertThat(actionCount).hasValue(1);

    // Fall out of sync and catch up again: the action must not run a second time
    updateChainState(syncTargetPeer.getEthPeer(), TARGET_CHAIN_HEIGHT + 10);
    syncState.setSyncTarget(syncTargetPeer.getEthPeer(), genesisBlock.getHeader());
    advanceLocalChain(TARGET_CHAIN_HEIGHT + 10);
    assertThat(actionCount).hasValue(1);
  }

  private RespondingEthPeer createPeer(final long blockHeight) {
    return EthProtocolManagerTestUtil.createPeer(ethProtocolManager, blockHeight);
  }

  private void advanceLocalChain(final long newChainHeight) {
    while (blockchain.getChainHeadBlockNumber() < newChainHeight) {
      final BlockHeader parent = blockchain.getChainHeadHeader();
      final Block block =
          gen.block(
              BlockOptions.create()
                  .setDifficulty(standardDifficultyPerBlock)
                  .setParentHash(parent.getHash())
                  .setBlockNumber(parent.getNumber() + 1L)
                  .transactionCount(0));
      final List<TransactionReceipt> receipts = gen.receipts(block);
      blockchain.appendBlock(block, receipts);
    }
  }

  private void updateChainState(final EthPeer peer, final long blockHeight) {
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(blockHeight + 1L)
            .difficulty(Difficulty.ZERO)
            .buildHeader();
    peer.chainState()
        .updateForAnnouncedBlock(header, standardDifficultyPerBlock.multiply(blockHeight));
  }
}
