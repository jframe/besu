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
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.LIGHT_DETACHED_ONLY;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.LIGHT_SKIP_DETACHED;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.NONE;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.SKIP_DETACHED;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.DownloadPipelineFactory;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.FastSyncState;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.FastSyncValidationPolicy;
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
            this.syncConfig.getFastSyncFullValidationRate(), NONE, NONE, fastSyncValidationCounter);
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
        .thenCompose(unused -> scheduler.startPipeline(pipeline));
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
    final DownloadBodiesAndReceiptsStep downloadBodiesAndReceiptsStep =
        new DownloadBodiesAndReceiptsStep(protocolSchedule, ethContext, metricsSystem);

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
        .thenProcessAsync("loadHeaders", loadHeadersStep, downloaderParallelism)
        .thenProcessAsync(
            "downloadBodiesAndReceipts", downloadBodiesAndReceiptsStep, downloaderParallelism)
        .andFinishWith("importBlock", (ignore) -> {});
  }

  protected BlockHeader getCommonAncestor(final SyncTarget target) {
    return target
        .peer()
        .getCheckpointHeader()
        .filter(checkpoint -> checkpoint.getNumber() > target.commonAncestor().getNumber())
        .orElse(target.commonAncestor());
  }
}
