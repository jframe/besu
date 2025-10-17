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
package org.hyperledger.besu.ethereum.eth.sync.fastsync;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockBody;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DownloadSyncReceiptsStepTest {

  private static ProtocolContext protocolContext;
  private static ProtocolSchedule protocolSchedule;
  private static MutableBlockchain blockchain;

  private PeerTaskExecutor peerTaskExecutor;
  private EthProtocolManager ethProtocolManager;

  @BeforeAll
  public static void setUpClass() {
    final BlockchainSetupUtil setupUtil = BlockchainSetupUtil.forTesting(DataStorageFormat.FOREST);
    setupUtil.importFirstBlocks(20);
    protocolContext = setupUtil.getProtocolContext();
    protocolSchedule = setupUtil.getProtocolSchedule();
    blockchain = setupUtil.getBlockchain();
  }

  @BeforeEach
  public void setUp() {
    peerTaskExecutor = mock(PeerTaskExecutor.class);
    TransactionPool transactionPool = mock(TransactionPool.class);
    ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(ProtocolScheduleFixture.TESTING_NETWORK)
            .setBlockchain(blockchain)
            .setEthScheduler(new DeterministicEthScheduler(() -> false))
            .setWorldStateArchive(protocolContext.getWorldStateArchive())
            .setTransactionPool(transactionPool)
            .setEthereumWireProtocolConfiguration(EthProtocolConfiguration.defaultConfig())
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();
  }

  @Test
  public void shouldDownloadReceiptsForBlocksUsingPeerTaskSystem()
      throws ExecutionException, InterruptedException {
    DownloadSyncReceiptsStep downloadSyncReceiptsStep =
        new DownloadSyncReceiptsStep(
            protocolSchedule,
            ethProtocolManager.ethContext(),
            SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build(),
            new NoOpMetricsSystem());

    final List<SyncBlock> blocks =
        asList(mockSyncBlock(), mockSyncBlock(), mockSyncBlock(), mockSyncBlock());

    // Create sync receipts from actual receipts
    Map<BlockHeader, List<SyncTransactionReceipt>> syncReceiptsMap = new HashMap<>();
    blocks.forEach(
        (b) -> {
          TransactionReceipt receipt = Mockito.mock(TransactionReceipt.class);
          // Create a SyncTransactionReceipt with minimal data
          SyncTransactionReceipt syncReceipt = Mockito.mock(SyncTransactionReceipt.class);
          Mockito.when(syncReceipt.getReceiptSupplier()).thenReturn(() -> receipt);
          syncReceiptsMap.put(b.getHeader(), List.of(syncReceipt));
        });

    PeerTaskExecutorResult<Map<BlockHeader, List<SyncTransactionReceipt>>> peerTaskResult =
        new PeerTaskExecutorResult<>(
            Optional.of(syncReceiptsMap),
            PeerTaskExecutorResponseCode.SUCCESS,
            Collections.emptyList());
    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetSyncReceiptsFromPeerTask.class)))
        .thenReturn(peerTaskResult);

    final CompletableFuture<List<SyncBlockWithReceipts>> result =
        downloadSyncReceiptsStep.apply(blocks);

    assertThat(result.get().get(0).getBlock()).isEqualTo(blocks.get(0));
    assertThat(result.get().get(0).getReceipts().size()).isEqualTo(1);
    assertThat(result.get().get(1).getBlock()).isEqualTo(blocks.get(1));
    assertThat(result.get().get(1).getReceipts().size()).isEqualTo(1);
    assertThat(result.get().get(2).getBlock()).isEqualTo(blocks.get(2));
    assertThat(result.get().get(2).getReceipts().size()).isEqualTo(1);
    assertThat(result.get().get(3).getBlock()).isEqualTo(blocks.get(3));
    assertThat(result.get().get(3).getReceipts().size()).isEqualTo(1);
  }

  @Test
  public void shouldLazilyDecodeReceipts() throws ExecutionException, InterruptedException {
    DownloadSyncReceiptsStep downloadSyncReceiptsStep =
        new DownloadSyncReceiptsStep(
            protocolSchedule,
            ethProtocolManager.ethContext(),
            SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build(),
            new NoOpMetricsSystem());

    final List<SyncBlock> blocks = asList(mockSyncBlock());

    // Create a real receipt and convert to sync receipt
    TransactionReceipt originalReceipt =
        new TransactionReceipt(1, 12345L, Collections.emptyList(), Optional.empty());

    // Wrap receipt in SyncTransactionReceipt
    SyncTransactionReceipt syncReceipt = Mockito.mock(SyncTransactionReceipt.class);
    Mockito.when(syncReceipt.getReceiptSupplier()).thenReturn(() -> originalReceipt);

    Map<BlockHeader, List<SyncTransactionReceipt>> syncReceiptsMap = new HashMap<>();
    syncReceiptsMap.put(blocks.get(0).getHeader(), List.of(syncReceipt));

    PeerTaskExecutorResult<Map<BlockHeader, List<SyncTransactionReceipt>>> peerTaskResult =
        new PeerTaskExecutorResult<>(
            Optional.of(syncReceiptsMap),
            PeerTaskExecutorResponseCode.SUCCESS,
            Collections.emptyList());
    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetSyncReceiptsFromPeerTask.class)))
        .thenReturn(peerTaskResult);

    final CompletableFuture<List<SyncBlockWithReceipts>> result =
        downloadSyncReceiptsStep.apply(blocks);

    // Verify that receipts are lazily decoded
    List<SyncBlockWithReceipts> blocksWithReceipts = result.get();
    assertThat(blocksWithReceipts).hasSize(1);
    assertThat(blocksWithReceipts.get(0).getReceipts()).hasSize(1);

    // The receipt should be the same as the original after lazy decoding
    assertThat(blocksWithReceipts.get(0).getReceipts().get(0)).isEqualTo(originalReceipt);
  }

  @Test
  public void shouldReturnSyncReceiptsWithoutDecodingInNewPath()
      throws ExecutionException, InterruptedException {
    DownloadSyncReceiptsStep downloadSyncReceiptsStep =
        new DownloadSyncReceiptsStep(
            protocolSchedule,
            ethProtocolManager.ethContext(),
            SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build(),
            new NoOpMetricsSystem());

    final List<SyncBlock> blocks = asList(mockSyncBlock());

    // Create a sync receipt with encoded bytes (not decoded)
    final TransactionReceipt originalReceipt =
        new TransactionReceipt(1, 12345L, Collections.emptyList(), Optional.empty());
    final SyncTransactionReceipt syncReceipt = Mockito.mock(SyncTransactionReceipt.class);
    Mockito.when(syncReceipt.getReceiptSupplier()).thenReturn(() -> originalReceipt);

    Map<BlockHeader, List<SyncTransactionReceipt>> syncReceiptsMap = new HashMap<>();
    syncReceiptsMap.put(blocks.get(0).getHeader(), List.of(syncReceipt));

    PeerTaskExecutorResult<Map<BlockHeader, List<SyncTransactionReceipt>>> peerTaskResult =
        new PeerTaskExecutorResult<>(
            Optional.of(syncReceiptsMap),
            PeerTaskExecutorResponseCode.SUCCESS,
            Collections.emptyList());
    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetSyncReceiptsFromPeerTask.class)))
        .thenReturn(peerTaskResult);

    final CompletableFuture<List<SyncBlockWithReceipts>> result =
        downloadSyncReceiptsStep.apply(blocks);

    // Verify that SyncBlockWithReceipts contains SyncTransactionReceipts (not decoded yet)
    List<SyncBlockWithReceipts> blocksWithReceipts = result.get();
    assertThat(blocksWithReceipts).hasSize(1);
    assertThat(blocksWithReceipts.get(0).getSyncReceipts()).hasSize(1);
    assertThat(blocksWithReceipts.get(0).getSyncReceipts().get(0)).isEqualTo(syncReceipt);

    // Verify that getReceipts() triggers lazy decoding
    assertThat(blocksWithReceipts.get(0).getReceipts()).hasSize(1);
    assertThat(blocksWithReceipts.get(0).getReceipts().get(0)).isEqualTo(originalReceipt);
  }

  private SyncBlock mockSyncBlock() {
    final BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    Mockito.when(blockHeader.getReceiptsRoot()).thenReturn(Hash.fromHexStringLenient("DEADBEEF"));

    final SyncBlockBody blockBody = Mockito.mock(SyncBlockBody.class);
    Mockito.when(blockBody.getTransactionCount()).thenReturn(1);

    return new SyncBlock(blockHeader, blockBody);
  }
}
