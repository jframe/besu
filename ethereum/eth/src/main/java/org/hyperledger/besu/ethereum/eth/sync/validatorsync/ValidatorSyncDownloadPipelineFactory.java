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
package org.hyperledger.besu.ethereum.eth.sync.validatorsync;

import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.DETACHED_ONLY;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.FULL;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.LIGHT;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.LIGHT_DETACHED_ONLY;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.LIGHT_SKIP_DETACHED;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.SKIP_DETACHED;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.DownloadBodiesStep;
import org.hyperledger.besu.ethereum.eth.sync.DownloadPipelineFactory;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.checkpointsync.CheckpointBlockImportStep;
import org.hyperledger.besu.ethereum.eth.sync.checkpointsync.CheckpointDownloadBlockStep;
import org.hyperledger.besu.ethereum.eth.sync.checkpointsync.CheckpointSource;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.DownloadReceiptsStep;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.FastSyncState;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.FastSyncValidationPolicy;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.ImportBlocksStep;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.range.RangeHeadersValidationStep;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncTarget;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ValidatorSyncDownloadPipelineFactory implements DownloadPipelineFactory {
  protected final SynchronizerConfiguration syncConfig;
  protected final ProtocolSchedule protocolSchedule;
  protected final ProtocolContext protocolContext;
  protected final EthContext ethContext;
  protected final FastSyncState fastSyncState;
  protected final MetricsSystem metricsSystem;
  protected final FastSyncValidationPolicy attachedValidationPolicy;
  protected final FastSyncValidationPolicy detachedValidationPolicy;
  protected final FastSyncValidationPolicy ommerValidationPolicy;

  public ValidatorSyncDownloadPipelineFactory(
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
    final CompletableFuture<Void> downloadHeadersFuture =
        scheduler.startPipeline(createDownloadHeadersPipeline(syncTarget));
    final CompletableFuture<Void> importBlocksFuture = scheduler.startPipeline(pipeline);
    if (syncState.getCheckpoint().isPresent()) {
      final CompletableFuture<Void> downloadCheckPointPipeline =
          scheduler.startPipeline(createDownloadCheckPointPipeline(syncState, syncTarget));
      return downloadCheckPointPipeline
          .thenCompose(unused -> downloadHeadersFuture)
          .thenCompose(unused -> importBlocksFuture);
    } else {
      return downloadHeadersFuture.thenCompose(unused -> importBlocksFuture);
    }
  }

  protected Pipeline<Hash> createDownloadCheckPointPipeline(
      final SyncState syncState, final SyncTarget target) {

    final Checkpoint checkpoint = syncState.getCheckpoint().orElseThrow();

    final BlockHeader checkpointBlockHeader = target.peer().getCheckpointHeader().orElseThrow();
    final CheckpointSource checkPointSource =
        new CheckpointSource(
            syncState,
            checkpointBlockHeader,
            protocolSchedule
                .getByBlockHeader(checkpointBlockHeader)
                .getBlockHeaderFunctions()
                .getCheckPointWindowSize(checkpointBlockHeader));

    final CheckpointBlockImportStep checkPointBlockImportStep =
        new CheckpointBlockImportStep(
            checkPointSource, checkpoint, protocolContext.getBlockchain());

    final CheckpointDownloadBlockStep checkPointDownloadBlockStep =
        new CheckpointDownloadBlockStep(protocolSchedule, ethContext, checkpoint, metricsSystem);

    return PipelineBuilder.createPipelineFrom(
            "fetchCheckpoints",
            checkPointSource,
            1,
            metricsSystem.createLabelledCounter(
                BesuMetricCategory.SYNCHRONIZER,
                "chain_download_pipeline_processed_total",
                "Number of header process by each chain download pipeline stage",
                "step",
                "action"),
            true,
            "checkpointSync")
        .thenProcessAsyncOrdered("downloadBlock", checkPointDownloadBlockStep::downloadBlock, 1)
        .andFinishWith("importBlock", checkPointBlockImportStep);
  }

  protected Pipeline<ValidatorSyncRange> createDownloadHeadersPipeline(final SyncTarget target) {
    final int downloaderParallelism = syncConfig.getDownloaderParallelism();
    final int headerRequestSize = syncConfig.getDownloaderHeaderRequestSize();

    final ValidatorSyncSource validatorSyncSource =
        new ValidatorSyncSource(
            getCommonAncestor(target).getNumber(),
            fastSyncState.getPivotBlockNumber().getAsLong(),
            true,
            headerRequestSize);
    final DownloadHeadersBackwardsStep downloadHeadersStep =
        new DownloadHeadersBackwardsStep(
            protocolSchedule, protocolContext, detachedValidationPolicy, ethContext, metricsSystem);
    final RangeHeadersValidationStep validateHeadersJoinUpStep =
        new RangeHeadersValidationStep(protocolSchedule, protocolContext, detachedValidationPolicy);
    final SaveHeadersStep saveHeadersStep = new SaveHeadersStep(protocolContext.getBlockchain());

    return PipelineBuilder.createPipelineFrom(
            "posPivot",
            validatorSyncSource,
            downloaderParallelism,
            metricsSystem.createLabelledCounter(
                BesuMetricCategory.SYNCHRONIZER,
                "chain_download_pipeline_processed_total",
                "Number of entries process by each chain download pipeline stage",
                "step",
                "action"),
            true,
            "validatorSyncHeaderDownload")
        .thenProcessAsyncOrdered("downloadHeaders", downloadHeadersStep, downloaderParallelism)
        .thenFlatMap("validateHeaders", validateHeadersJoinUpStep, downloaderParallelism)
        .andFinishWith("saveHeader", saveHeadersStep);
  }

  @Override
  public Pipeline<ValidatorSyncRange> createDownloadPipelineForSyncTarget(final SyncTarget target) {
    final int downloaderParallelism = syncConfig.getDownloaderParallelism();
    final int headerRequestSize = syncConfig.getDownloaderHeaderRequestSize();

    final ValidatorSyncSource validatorSyncSource =
        new ValidatorSyncSource(
            getCommonAncestor(target).getNumber(),
            fastSyncState.getPivotBlockNumber().getAsLong(),
            false,
            headerRequestSize);
    final LoadHeadersStep loadHeadersStep = new LoadHeadersStep(protocolContext.getBlockchain());
    final DownloadBodiesStep downloadBodiesStep =
        new DownloadBodiesStep(protocolSchedule, ethContext, metricsSystem);
    final DownloadReceiptsStep downloadReceiptsStep =
        new DownloadReceiptsStep(ethContext, metricsSystem);
    final ImportBlocksStep importBlockStep =
        new ImportBlocksStep(
            protocolSchedule,
            protocolContext,
            attachedValidationPolicy,
            ommerValidationPolicy,
            ethContext,
            fastSyncState.getPivotBlockHeader().get());

    return PipelineBuilder.createPipelineFrom(
            "posPivot",
            validatorSyncSource,
            downloaderParallelism,
            metricsSystem.createLabelledCounter(
                BesuMetricCategory.SYNCHRONIZER,
                "chain_download_pipeline_processed_total",
                "Number of entries process by each chain download pipeline stage",
                "step",
                "action"),
            true,
            "validatorSyncBlockImport")
        .thenFlatMap("loadHeaders", loadHeadersStep, downloaderParallelism)
        .inBatches(headerRequestSize)
        .thenProcessAsyncOrdered("downloadBodies", downloadBodiesStep, downloaderParallelism)
        .thenProcessAsyncOrdered("downloadReceipts", downloadReceiptsStep, downloaderParallelism)
        .andFinishWith("importBlock", importBlockStep);
  }

  protected BlockHeader getCommonAncestor(final SyncTarget target) {
    return target
        .peer()
        .getCheckpointHeader()
        .filter(checkpoint -> checkpoint.getNumber() > target.commonAncestor().getNumber())
        .orElse(target.commonAncestor());
  }
}
