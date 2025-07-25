/*
 * Copyright ConsenSys AG.
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

import static org.hyperledger.besu.ethereum.eth.sync.fastsync.SyncTargetNumberRangeSource.Direction.BACKWARDS;
import static org.hyperledger.besu.ethereum.eth.sync.fastsync.SyncTargetNumberRangeSource.Direction.FORWARDS;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.DETACHED_ONLY;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.FULL;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.LIGHT;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.LIGHT_DETACHED_ONLY;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.LIGHT_SKIP_DETACHED;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.SKIP_DETACHED;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.DownloadBodiesStep;
import org.hyperledger.besu.ethereum.eth.sync.DownloadPipelineFactory;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.range.RangeHeadersValidationStep;
import org.hyperledger.besu.ethereum.eth.sync.range.SyncTargetRange;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncTarget;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;

import java.util.concurrent.CompletionStage;

public class FastSyncDownloadPipelineFactory implements DownloadPipelineFactory {
  protected final SynchronizerConfiguration syncConfig;
  protected final ProtocolSchedule protocolSchedule;
  protected final ProtocolContext protocolContext;
  protected final EthContext ethContext;
  protected final FastSyncState fastSyncState;
  protected final MetricsSystem metricsSystem;
  protected final FastSyncValidationPolicy attachedValidationPolicy;
  protected final FastSyncValidationPolicy detachedValidationPolicy;
  protected final FastSyncValidationPolicy ommerValidationPolicy;

  public FastSyncDownloadPipelineFactory(
      final SynchronizerConfiguration syncConfig,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final FastSyncState fastSyncState,
      final MetricsSystem metricsSystem) {
    this.syncConfig = syncConfig;
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.fastSyncState = fastSyncState;
    this.metricsSystem = metricsSystem;
    final LabelledMetric<Counter> fastSyncValidationCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "fast_sync_validation_mode",
            "Number of blocks validated using light vs full validation during fast sync",
            "validationMode");
    attachedValidationPolicy =
        new FastSyncValidationPolicy(
            this.syncConfig.getFastSyncFullValidationRate(),
            LIGHT_SKIP_DETACHED,
            SKIP_DETACHED,
            fastSyncValidationCounter);
    ommerValidationPolicy =
        new FastSyncValidationPolicy(
            this.syncConfig.getFastSyncFullValidationRate(),
            LIGHT,
            FULL,
            fastSyncValidationCounter);
    detachedValidationPolicy =
        new FastSyncValidationPolicy(
            this.syncConfig.getFastSyncFullValidationRate(),
            LIGHT_DETACHED_ONLY,
            DETACHED_ONLY,
            fastSyncValidationCounter);
  }

  @Override
  public CompletionStage<Void> startPipeline(
      final EthScheduler scheduler,
      final SyncState syncState,
      final SyncTarget syncTarget,
      final Pipeline<?> pipeline) {
    return scheduler
        .startPipeline(createDownloadHeadersPipeline(syncTarget))
        .thenCompose(ignore -> scheduler.startPipeline(createDownloadBlocksPipeline(syncState)));
  }

  // Implemented to satisfy the interface but not used in FastSync.
  @Override
  public Pipeline<SyncTargetRange> createDownloadPipelineForSyncTarget(
      final SyncState syncState, final SyncTarget target) {
    return null;
  }

  protected Pipeline<SyncTargetNumberRange> createDownloadHeadersPipeline(final SyncTarget target) {
    final int downloaderParallelism = syncConfig.getDownloaderParallelism();
    final int headerRequestSize = syncConfig.getDownloaderHeaderRequestSize();
    final int singleHeaderBufferSize = headerRequestSize * downloaderParallelism;

    final SyncTargetNumberRangeSource syncTargetNumberRangeSource =
        new SyncTargetNumberRangeSource(
            getCommonAncestor(target).getNumber(),
            fastSyncState.getPivotBlockNumber().getAsLong(),
            headerRequestSize,
            BACKWARDS);
    final DownloadHeadersBackwardsStep downloadHeadersStep =
        new DownloadHeadersBackwardsStep(
            protocolSchedule, protocolContext, detachedValidationPolicy, ethContext, metricsSystem);
    final RangeHeadersValidationStep validateHeadersJoinUpStep =
        new RangeHeadersValidationStep(protocolSchedule, protocolContext, detachedValidationPolicy);
    final SaveHeadersStep saveHeadersStep = new SaveHeadersStep(protocolContext.getBlockchain());
    return PipelineBuilder.createPipelineFrom(
            "backwardsBlockNumbers",
            syncTargetNumberRangeSource,
            downloaderParallelism,
            metricsSystem.createLabelledCounter(
                BesuMetricCategory.SYNCHRONIZER,
                "chain_download_pipeline_processed_total",
                "Number of entries process by each chain download pipeline stage",
                "step",
                "action"),
            true,
            "fastSyncHeaders")
        .thenProcessAsync("downloadHeaders", downloadHeadersStep, downloaderParallelism)
        .thenFlatMap("validateHeadersJoin", validateHeadersJoinUpStep, singleHeaderBufferSize)
        .andFinishWith("saveHeader", saveHeadersStep);
  }

  private Pipeline<SyncTargetNumberRange> createDownloadBlocksPipeline(final SyncState syncState) {
    final int downloaderParallelism = syncConfig.getDownloaderParallelism();
    final int headerRequestSize = syncConfig.getDownloaderHeaderRequestSize();

    final SyncTargetNumberRangeSource syncTargetNumberRangeSource =
        new SyncTargetNumberRangeSource(
            getCheckpointBlockNumber(syncState),
            fastSyncState.getPivotBlockNumber().getAsLong(),
            headerRequestSize,
            FORWARDS);
    final LoadHeadersStep loadHeadersStep = new LoadHeadersStep(protocolContext.getBlockchain());
    final DownloadBodiesStep downloadBodiesStep =
        new DownloadBodiesStep(protocolSchedule, ethContext, syncConfig, metricsSystem);
    final DownloadReceiptsStep downloadReceiptsStep =
        new DownloadReceiptsStep(protocolSchedule, ethContext, syncConfig, metricsSystem);
    final ImportBlocksStep importBlockStep =
        new ImportBlocksStep(
            protocolSchedule,
            protocolContext,
            attachedValidationPolicy,
            ommerValidationPolicy,
            ethContext,
            fastSyncState.getPivotBlockHeader().get(),
            syncConfig.getSnapSyncConfiguration().isSnapSyncTransactionIndexingEnabled());

    return PipelineBuilder.createPipelineFrom(
            "fetchCheckpoints",
            syncTargetNumberRangeSource,
            downloaderParallelism,
            metricsSystem.createLabelledCounter(
                BesuMetricCategory.SYNCHRONIZER,
                "chain_download_pipeline_processed_total",
                "Number of entries process by each chain download pipeline stage",
                "step",
                "action"),
            true,
            "fastSyncBlocks")
        .thenProcessAsync("loadHeaders", loadHeadersStep, downloaderParallelism)
        .thenProcessAsyncOrdered("downloadBodies", downloadBodiesStep, downloaderParallelism)
        .thenProcessAsyncOrdered("downloadReceipts", downloadReceiptsStep, downloaderParallelism)
        .andFinishWith("importBlock", importBlockStep);
  }

  protected BlockHeader getCommonAncestor(final SyncTarget syncTarget) {
    return syncTarget.commonAncestor();
  }

  private long getCheckpointBlockNumber(final SyncState syncState) {
    return syncConfig.isSnapSyncSavePreCheckpointHeadersOnlyEnabled()
        ? syncState.getCheckpoint().map(Checkpoint::blockNumber).orElse(0L)
        : 0L;
  }
}
