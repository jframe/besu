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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.BackwardHeaderDriver;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.common.SingleBlockHeaderDownloader;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.services.pipeline.Pipeline;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Covers the snap/2 specific behaviour of {@link SnapSyncChainDownloader}: the block-access-list
 * download that runs between Stage 1 and Stage 2, and the continuation cycle run for pivot updates
 * reported by the snap/2 world state downloader.
 */
@ExtendWith(MockitoExtension.class)
class SnapSyncChainDownloaderSnapV2Test {

  @Mock private SnapSyncChainDownloadPipelineFactory pipelineFactory;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private ProtocolContext protocolContext;
  @Mock private EthContext ethContext;
  @Mock private EthPeers ethPeers;
  @Mock private SyncState syncState;
  @Mock private SyncDurationMetrics syncDurationMetrics;
  @Mock private MutableBlockchain blockchain;
  @Mock private EthScheduler scheduler;
  @Mock private SingleBlockHeaderDownloader headerDownloader;

  @TempDir private Path tempDir;

  private ChainSyncStateStorage chainSyncStateStorage;
  private BlockHeader initialPivot;
  private SynchronizerConfiguration syncConfig;
  private Pipeline<List<BlockHeader>> balPipeline;
  private Pipeline<List<BlockHeader>> forwardPipeline;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    initialPivot = header(1000);
    chainSyncStateStorage = new ChainSyncStateStorage(tempDir);
    syncConfig = SynchronizerConfiguration.builder().build();

    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
    lenient().when(blockchain.getGenesisBlockHeader()).thenReturn(header(0));
    lenient().when(blockchain.getChainHeadBlockNumber()).thenReturn(500L);
    lenient().when(blockchain.getChainHeadHeader()).thenReturn(header(500));
    lenient().when(ethContext.getScheduler()).thenReturn(scheduler);
    lenient().when(ethContext.getEthPeers()).thenReturn(ethPeers);
    lenient().when(ethPeers.peerCount()).thenReturn(1);
    lenient().when(syncState.getCheckpoint()).thenReturn(Optional.empty());
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient()
        .when(protocolSpec.getBlockHeaderFunctions())
        .thenReturn(new MainnetBlockHeaderFunctions());

    final Pipeline<Long> backwardPipeline = mock(Pipeline.class);
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    lenient().when(driver.getMatchedAncestor()).thenReturn(Optional.empty());
    lenient()
        .when(pipelineFactory.createBackwardHeaderDownloadPipeline(any()))
        .thenReturn(
            new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                backwardPipeline, driver));
    balPipeline = mock(Pipeline.class);
    forwardPipeline = mock(Pipeline.class);
    lenient()
        .when(pipelineFactory.createBlockAccessListDownloadPipeline(anyLong(), any()))
        .thenReturn(balPipeline);
    lenient()
        .when(
            pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);
    lenient()
        .when(scheduler.startPipeline(any()))
        .thenReturn(CompletableFuture.completedFuture(null));
  }

  @Test
  void blockAccessListsAreDownloadedBeforeBodiesWhenSnap2IsEnabled() throws Exception {
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    // BALs are downloaded from the same anchor as the bodies (the genesis body checkpoint here,
    // since nothing canonical above it has a body) and up to the pivot.
    verify(pipelineFactory).createBlockAccessListDownloadPipeline(0L, initialPivot);
    verify(pipelineFactory).createForwardBodiesAndReceiptsDownloadPipeline(eq(0L), any(), any());

    // Order matters: the block access lists must be present before bodies are imported.
    final InOrder inOrder = inOrder(scheduler);
    inOrder.verify(scheduler).startPipeline(balPipeline);
    inOrder.verify(scheduler).startPipeline(forwardPipeline);
  }

  @Test
  void blockAccessListDownloadIsSkippedWhenTheAnchorAlreadyReachedThePivot() throws Exception {
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    // The chain head is the pivot itself and already has a body: there is nothing left to fetch.
    lenient().when(blockchain.getChainHeadHeader()).thenReturn(initialPivot);
    lenient().when(blockchain.blockIsOnCanonicalChain(initialPivot.getHash())).thenReturn(true);
    lenient()
        .when(blockchain.getBlockBody(initialPivot.getHash()))
        .thenReturn(Optional.of(mock(BlockBody.class)));

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    verify(pipelineFactory, never()).createBlockAccessListDownloadPipeline(anyLong(), any());
    verify(pipelineFactory, never())
        .createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any());
  }

  @Test
  void pivotUpdatesDuringTheInitialCycleRunOneContinuationCycleToTheLatestPivot() throws Exception {
    lenient().when(pipelineFactory.isSnap2Enabled()).thenReturn(true);
    final BlockHeader intermediatePivot = header(1500);
    final BlockHeader latestPivot = header(2000);

    final SnapSyncChainDownloader downloader = downloader();
    downloader.onPivotUpdated(intermediatePivot);
    downloader.onPivotUpdated(latestPivot);
    downloader.onWorldStateHealFinished();
    downloader.start().get(5, TimeUnit.SECONDS);

    verify(pipelineFactory).createBlockAccessListDownloadPipeline(anyLong(), eq(latestPivot));
    verify(pipelineFactory, never())
        .createBlockAccessListDownloadPipeline(anyLong(), eq(intermediatePivot));
  }

  private SnapSyncChainDownloader downloader() {
    return new SnapSyncChainDownloader(
        pipelineFactory,
        syncConfig,
        protocolSchedule,
        protocolContext,
        ethContext,
        syncState,
        syncDurationMetrics,
        initialPivot,
        chainSyncStateStorage,
        headerDownloader);
  }

  private static BlockHeader header(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }
}
