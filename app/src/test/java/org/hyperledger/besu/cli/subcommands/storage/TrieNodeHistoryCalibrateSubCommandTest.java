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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.CalibrationResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.RecordingTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class TrieNodeHistoryCalibrateSubCommandTest {

  @Test
  void rollbackVerifyThenForwardReplayRecordsRealTrieNodeWrites() {
    final BlockchainSetupUtil setup = BlockchainSetupUtil.forTesting(DataStorageFormat.BONSAI);
    setup.importAllBlocks();

    final BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) setup.getWorldArchive();
    final MutableBlockchain blockchain = setup.getBlockchain();
    final TrieLogManager trieLogManager = archive.getTrieLogManager();

    final long head = blockchain.getChainHeadBlockNumber();
    final long target = head - 2;
    assertThat(target).isGreaterThanOrEqualTo(1L);

    // Roll the real head world state backward to target, then confirm the flat DB / trie logs
    // agree.
    final PathBasedWorldState headWorldState = (PathBasedWorldState) archive.getWorldState();
    TrieNodeHistoryCalibrateSubCommand.rollBackTo(
        headWorldState, blockchain, trieLogManager, head, target, new AtomicLong());
    TrieNodeHistoryCalibrateSubCommand.verifyStateRoot(headWorldState, blockchain, target);
    assertThat(headWorldState.rootHash())
        .isEqualTo(blockchain.getBlockHeader(target).orElseThrow().getStateRoot());

    // Now replay forward through a recording strategy and confirm real writes were measured.
    final RecordingTrieNodeStrategy recorder =
        new RecordingTrieNodeStrategy(new BonsaiTrieNodeStrategy());
    final BonsaiWorldState recordingWorldState =
        TrieNodeHistoryCalibrateSubCommand.buildRecordingWorldState(
            archive, new NoOpMetricsSystem(), recorder);

    final CalibrationResult result =
        TrieNodeHistoryCalibrateSubCommand.replayForward(
            recordingWorldState,
            blockchain,
            trieLogManager,
            target,
            head,
            recorder,
            new AtomicLong());

    final long totalWrites = Arrays.stream(result.writesByDepth()).sum();
    assertThat(totalWrites).isGreaterThan(0L);
    // Forward replay must land back on the real head state root (persist self-verifies each block).
    assertThat(recordingWorldState.rootHash())
        .isEqualTo(blockchain.getBlockHeader(head).orElseThrow().getStateRoot());
  }

  @Test
  void verifyStateRootThrowsOnDivergence() {
    final BlockchainSetupUtil setup = BlockchainSetupUtil.forTesting(DataStorageFormat.BONSAI);
    setup.importAllBlocks();
    final BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) setup.getWorldArchive();
    final MutableBlockchain blockchain = setup.getBlockchain();
    final PathBasedWorldState headWorldState = (PathBasedWorldState) archive.getWorldState();
    final long head = blockchain.getChainHeadBlockNumber();

    // Head world state is at `head`, but we claim it should be at head-1: roots must differ.
    final Hash headRoot = blockchain.getBlockHeader(head).orElseThrow().getStateRoot();
    final Hash parentRoot = blockchain.getBlockHeader(head - 1).orElseThrow().getStateRoot();
    assertThat(headRoot).isNotEqualTo(parentRoot);

    assertThatThrownBy(
            () ->
                TrieNodeHistoryCalibrateSubCommand.verifyStateRoot(
                    headWorldState, blockchain, head - 1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("State root mismatch");
  }
}
