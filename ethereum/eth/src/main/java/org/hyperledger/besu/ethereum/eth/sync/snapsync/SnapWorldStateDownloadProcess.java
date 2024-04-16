/*
 * Copyright contributors to Hyperledger Besu
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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.DynamicPivotBlockSelector.doNothingOnPivotChange;
import static org.hyperledger.besu.services.pipeline.PipelineBuilder.createPipelineFrom;

import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.TaskQueueIterator;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloadProcess;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.services.pipeline.Pipe;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;
import org.hyperledger.besu.services.pipeline.WritePipe;
import org.hyperledger.besu.services.tasks.Task;
import org.hyperledger.besu.util.ExceptionUtils;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapWorldStateDownloadProcess implements WorldStateDownloadProcess {

  private static final Logger LOG = LoggerFactory.getLogger(SnapWorldStateDownloadProcess.class);
  private final Pipeline<Task<SnapDataRequest>> completionPipeline;
  private final Pipeline<Task<SnapDataRequest>> fetchAccountPipeline;
  private final Pipeline<Task<SnapDataRequest>> fetchStorageDataPipeline;
  private final Pipeline<Task<SnapDataRequest>> fetchLargeStorageDataPipeline;
  private final Pipeline<Task<SnapDataRequest>> fetchCodePipeline;
  private final Pipeline<Task<SnapDataRequest>> trieHealingPipeline;

  private final Pipeline<Task<SnapDataRequest>> flatAccountHealingPipeline;

  private final Pipeline<Task<SnapDataRequest>> flatStorageHealingPipeline;

  private final WritePipe<Task<SnapDataRequest>> requestsToComplete;

  private SnapWorldStateDownloadProcess(
      final Pipeline<Task<SnapDataRequest>> fetchAccountPipeline,
      final Pipeline<Task<SnapDataRequest>> fetchStorageDataPipeline,
      final Pipeline<Task<SnapDataRequest>> fetchLargeStorageDataPipeline,
      final Pipeline<Task<SnapDataRequest>> fetchCodePipeline,
      final Pipeline<Task<SnapDataRequest>> trieHealingPipeline,
      final Pipeline<Task<SnapDataRequest>> flatAccountHealingPipeline,
      final Pipeline<Task<SnapDataRequest>> flatStorageHealingPipeline,
      final Pipeline<Task<SnapDataRequest>> completionPipeline,
      final WritePipe<Task<SnapDataRequest>> requestsToComplete) {
    this.fetchStorageDataPipeline = fetchStorageDataPipeline;
    this.fetchAccountPipeline = fetchAccountPipeline;
    this.fetchLargeStorageDataPipeline = fetchLargeStorageDataPipeline;
    this.fetchCodePipeline = fetchCodePipeline;
    this.trieHealingPipeline = trieHealingPipeline;
    this.flatAccountHealingPipeline = flatAccountHealingPipeline;
    this.flatStorageHealingPipeline = flatStorageHealingPipeline;
    this.completionPipeline = completionPipeline;
    this.requestsToComplete = requestsToComplete;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public CompletableFuture<Void> start(final EthScheduler ethScheduler) {
    final CompletableFuture<Void> fetchAccountFuture =
        ethScheduler.startPipeline(fetchAccountPipeline);
    final CompletableFuture<Void> fetchStorageFuture =
        ethScheduler.startPipeline(fetchStorageDataPipeline);
    final CompletableFuture<Void> fetchLargeStorageFuture =
        ethScheduler.startPipeline(fetchLargeStorageDataPipeline);
    final CompletableFuture<Void> fetchCodeFuture = ethScheduler.startPipeline(fetchCodePipeline);
    final CompletableFuture<Void> trieHealingFuture =
        ethScheduler.startPipeline(trieHealingPipeline);
    final CompletableFuture<Void> flatAccountHealingFuture =
        ethScheduler.startPipeline(flatAccountHealingPipeline);
    final CompletableFuture<Void> flatStorageHealingFuture =
        ethScheduler.startPipeline(flatStorageHealingPipeline);
    final CompletableFuture<Void> completionFuture = ethScheduler.startPipeline(completionPipeline);

    fetchAccountFuture
        .thenCombine(fetchStorageFuture, (unused, unused2) -> null)
        .thenCombine(fetchLargeStorageFuture, (unused, unused2) -> null)
        .thenCombine(fetchCodeFuture, (unused, unused2) -> null)
        .thenCombine(trieHealingFuture, (unused, unused2) -> null)
        .thenCombine(flatAccountHealingFuture, (unused, unused2) -> null)
        .thenCombine(flatStorageHealingFuture, (unused, unused2) -> null)
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                if (!(ExceptionUtils.rootCause(error) instanceof CancellationException)) {
                  LOG.error("Pipeline failed", error);
                }
                completionPipeline.abort();
              } else {
                // No more data to fetch, so propagate the pipe closure onto the completion pipe.
                requestsToComplete.close();
              }
            });

    completionFuture.exceptionally(
        error -> {
          if (!(ExceptionUtils.rootCause(error) instanceof CancellationException)) {
            LOG.error("Pipeline failed", error);
          }
          fetchAccountPipeline.abort();
          fetchStorageDataPipeline.abort();
          fetchLargeStorageDataPipeline.abort();
          fetchCodePipeline.abort();
          trieHealingPipeline.abort();
          flatAccountHealingPipeline.abort();
          flatStorageHealingPipeline.abort();
          return null;
        });
    return completionFuture;
  }

  @Override
  public void abort() {
    fetchAccountPipeline.abort();
    fetchStorageDataPipeline.abort();
    fetchLargeStorageDataPipeline.abort();
    fetchCodePipeline.abort();
    trieHealingPipeline.abort();
    flatAccountHealingPipeline.abort();
    flatStorageHealingPipeline.abort();
    completionPipeline.abort();
  }

  public static class Builder {

    private SnapSyncConfiguration snapSyncConfiguration;
    private int maxOutstandingRequests;
    private SnapWorldDownloadState downloadState;
    private MetricsSystem metricsSystem;
    private LoadLocalDataStep loadLocalDataStep;
    private RequestDataStep requestDataStep;
    private SnapSyncProcessState snapSyncState;
    private PersistDataStep persistDataStep;
    private CompleteTaskStep completeTaskStep;
    private DynamicPivotBlockSelector pivotBlockManager;

    public Builder configuration(final SnapSyncConfiguration snapSyncConfiguration) {
      this.snapSyncConfiguration = snapSyncConfiguration;
      return this;
    }

    public Builder dynamicPivotBlockSelector(
        final DynamicPivotBlockSelector dynamicPivotBlockSelector) {
      this.pivotBlockManager = dynamicPivotBlockSelector;
      return this;
    }

    public Builder maxOutstandingRequests(final int maxOutstandingRequests) {
      this.maxOutstandingRequests = maxOutstandingRequests;
      return this;
    }

    public Builder loadLocalDataStep(final LoadLocalDataStep loadLocalDataStep) {
      this.loadLocalDataStep = loadLocalDataStep;
      return this;
    }

    public Builder requestDataStep(final RequestDataStep requestDataStep) {
      this.requestDataStep = requestDataStep;
      return this;
    }

    public Builder persistDataStep(final PersistDataStep persistDataStep) {
      this.persistDataStep = persistDataStep;
      return this;
    }

    public Builder completeTaskStep(final CompleteTaskStep completeTaskStep) {
      this.completeTaskStep = completeTaskStep;
      return this;
    }

    public Builder downloadState(final SnapWorldDownloadState downloadState) {
      this.downloadState = downloadState;
      return this;
    }

    public Builder fastSyncState(final SnapSyncProcessState fastSyncState) {
      this.snapSyncState = fastSyncState;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      this.metricsSystem = metricsSystem;
      return this;
    }

    public SnapWorldStateDownloadProcess build() {
      checkNotNull(loadLocalDataStep);
      checkNotNull(requestDataStep);
      checkNotNull(persistDataStep);
      checkNotNull(completeTaskStep);
      checkNotNull(downloadState);
      checkNotNull(snapSyncState);
      checkNotNull(metricsSystem);

      // Room for the requests we expect to do in parallel plus some buffer but not unlimited.
      final int bufferCapacity = snapSyncConfiguration.getTrienodeCountPerRequest() * 2;
      final LabelledMetric<Counter> outputCounter =
          metricsSystem.createLabelledCounter(
              BesuMetricCategory.SYNCHRONIZER,
              "snap_world_state_pipeline_processed_total",
              "Number of entries processed by each world state download pipeline stage",
              "step",
              "action");

      /*
      The logic and intercommunication of different pipelines can be summarized as follows:

      1. Account Data Pipeline (fetchAccountDataPipeline): This process starts with downloading the leaves of the account tree in ranges, with multiple ranges being processed simultaneously.
         If the downloaded accounts are smart contracts, tasks are created in the storage pipeline to download the storage tree of the smart contract, and in the code download pipeline for the smart contract.

      2. Storage Data Pipeline (fetchStorageDataPipeline): Running parallel to the account data pipeline, this pipeline downloads the storage of smart contracts.
          If all slots cannot be downloaded at once, tasks are created in the fetchLargeStorageDataPipeline to download the storage by range, allowing parallelization of large account downloads.

      3. Code Data Pipeline (fetchCodePipeline): This pipeline, running concurrently with the account and storage data pipelines, is responsible for downloading the code of the smart contracts.

      4. Large Storage Data Pipeline (fetchLargeStorageDataPipeline): This pipeline is used when the storage data for a smart contract is too large to be downloaded at once.
          It enables the storage data to be downloaded in ranges, similar to the account data.

      5. Healing Phase: Initiated after all other pipelines have completed their tasks, this phase ensures the integrity and completeness of the downloaded data.
      */
      final Pipeline<Task<SnapDataRequest>> completionPipeline =
          PipelineBuilder.<Task<SnapDataRequest>>createPipeline(
                  "requestDataAvailable", bufferCapacity, outputCounter, true, "node_data_request")
              .andFinishWith(
                  "requestCompleteTask",
                  task -> {
                    try {
                      completeTaskStep.markAsCompleteOrFailed(downloadState, task);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  });

      final Pipe<Task<SnapDataRequest>> requestsToComplete = completionPipeline.getInputPipe();

      final Pipeline<Task<SnapDataRequest>> fetchAccountDataPipeline =
          createPipelineFrom(
                  "dequeueAccountRequestBlocking",
                  new TaskQueueIterator<>(
                      downloadState,
                      () -> {
                        try {
                          return downloadState.dequeueAccountRequestBlocking();
                        } catch (RuntimeException e) {
                          LOG.info("Sync exception detected " + e);
                          e.printStackTrace();
                          throw e;
                        }
                      }),
                  bufferCapacity,
                  outputCounter,
                  true,
                  "world_state_download")
              .thenProcess(
                  "checkNewPivotBlock-Account",
                  tasks -> {
                    try {
                      pivotBlockManager.check(doNothingOnPivotChange);
                      return tasks;
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .thenProcessAsync(
                  "batchDownloadAccountData",
                  requestTask -> {
                    try {

                      return requestDataStep.requestAccount(requestTask);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  },
                  maxOutstandingRequests)
              .thenProcess(
                  "batchPersistAccountData",
                  task -> {
                    try {
                      return persistDataStep.persist(task);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .andFinishWith(
                  "batchAccountDataDownloaded",
                  task -> {
                    try {
                      requestsToComplete.put(task);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  });

      final Pipeline<Task<SnapDataRequest>> fetchStorageDataPipeline =
          createPipelineFrom(
                  "dequeueStorageRequestBlocking",
                  new TaskQueueIterator<>(
                      downloadState,
                      () -> {
                        try {
                          return downloadState.dequeueStorageRequestBlocking();
                        } catch (RuntimeException e) {
                          LOG.info("Sync exception detected " + e);
                          e.printStackTrace();
                          throw e;
                        }
                      }),
                  bufferCapacity,
                  outputCounter,
                  true,
                  "world_state_download")
              .inBatches(snapSyncConfiguration.getStorageCountPerRequest())
              .thenProcess(
                  "checkNewPivotBlock-Storage",
                  tasks -> {
                    try {
                      pivotBlockManager.check(doNothingOnPivotChange);
                      return tasks;
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .thenProcessAsyncOrdered(
                  "batchDownloadStorageData",
                  requestTask -> {
                    try {
                      return requestDataStep.requestStorage(requestTask);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  },
                  maxOutstandingRequests)
              .thenProcess(
                  "batchPersistStorageData",
                  task -> {
                    try {
                      return persistDataStep.persist(task);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .andFinishWith(
                  "batchStorageDataDownloaded",
                  tasks -> {
                    try {
                      tasks.forEach(requestsToComplete::put);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  });

      final Pipeline<Task<SnapDataRequest>> fetchLargeStorageDataPipeline =
          createPipelineFrom(
                  "dequeueLargeStorageRequestBlocking",
                  new TaskQueueIterator<>(
                      downloadState,
                      () -> {
                        try {
                          return downloadState.dequeueLargeStorageRequestBlocking();
                        } catch (RuntimeException e) {
                          LOG.info("Sync exception detected " + e);
                          e.printStackTrace();
                          throw e;
                        }
                      }),
                  bufferCapacity,
                  outputCounter,
                  true,
                  "world_state_download")
              .thenProcess(
                  "checkNewPivotBlock-LargeStorage",
                  tasks -> {
                    try {
                      pivotBlockManager.check(doNothingOnPivotChange);
                      return tasks;
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .thenProcessAsyncOrdered(
                  "batchDownloadLargeStorageData",
                  requestTask -> {
                    try {
                      return requestDataStep.requestStorage(List.of(requestTask));
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  },
                  maxOutstandingRequests)
              .thenProcess(
                  "batchPersistLargeStorageData",
                  task -> {
                    try {
                      persistDataStep.persist(task);
                      return task;
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .andFinishWith(
                  "batchLargeStorageDataDownloaded",
                  tasks -> tasks.forEach(requestsToComplete::put));

      final Pipeline<Task<SnapDataRequest>> fetchCodePipeline =
          createPipelineFrom(
                  "dequeueCodeRequestBlocking",
                  new TaskQueueIterator<>(
                      downloadState,
                      () -> {
                        try {
                          return downloadState.dequeueCodeRequestBlocking();
                        } catch (RuntimeException e) {
                          LOG.info("Sync exception detected " + e);
                          e.printStackTrace();
                          throw e;
                        }
                      }),
                  bufferCapacity,
                  outputCounter,
                  true,
                  "code_blocks_download_pipeline")
              .inBatches(
                  snapSyncConfiguration.getBytecodeCountPerRequest() * 2,
                  new Function<List<Task<SnapDataRequest>>, Integer>() {
                    @Override
                    public Integer apply(final List<Task<SnapDataRequest>> tasks) {
                      try {
                        return snapSyncConfiguration.getBytecodeCountPerRequest()
                            - (int)
                                tasks.stream()
                                    .map(Task::getData)
                                    .map(BytecodeRequest.class::cast)
                                    .map(BytecodeRequest::getCodeHash)
                                    .distinct()
                                    .count();
                      } catch (RuntimeException e) {
                        LOG.info("Sync exception detected " + e);
                        e.printStackTrace();
                        throw e;
                      }
                    }
                  })
              .thenProcess(
                  "checkNewPivotBlock-Code",
                  tasks -> {
                    try {
                      pivotBlockManager.check(
                          (blockHeader, newBlockFound) ->
                              reloadHealWhenNeeded(snapSyncState, downloadState, newBlockFound));
                      return tasks;
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .thenProcessAsyncOrdered(
                  "batchDownloadCodeData",
                  tasks -> {
                    try {
                      return requestDataStep.requestCode(tasks);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  },
                  maxOutstandingRequests)
              .thenProcess(
                  "batchPersistCodeData",
                  tasks -> {
                    try {
                      persistDataStep.persist(tasks);
                      return tasks;
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .andFinishWith(
                  "batchCodeDataDownloaded", tasks -> tasks.forEach(requestsToComplete::put));

      final Pipeline<Task<SnapDataRequest>> trieHealingPipeline =
          createPipelineFrom(
                  "requestTrieNodeDequeued",
                  new TaskQueueIterator<>(
                      downloadState,
                      () -> {
                        try {
                          return downloadState.dequeueTrieNodeRequestBlocking();
                        } catch (RuntimeException e) {
                          LOG.info("Sync exception detected " + e);
                          e.printStackTrace();
                          throw e;
                        }
                      }),
                  bufferCapacity,
                  outputCounter,
                  true,
                  "world_state_heal")
              .thenFlatMapInParallel(
                  "requestLoadLocalTrieNodeData",
                  task -> {
                    try {
                      return loadLocalDataStep.loadLocalDataTrieNode(task, requestsToComplete);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  },
                  3,
                  bufferCapacity)
              .inBatches(snapSyncConfiguration.getTrienodeCountPerRequest())
              .thenProcess(
                  "checkNewPivotBlock-TrieNode",
                  tasks -> {
                    try {
                      pivotBlockManager.check(
                          (blockHeader, newBlockFound) ->
                              reloadHealWhenNeeded(snapSyncState, downloadState, newBlockFound));
                      return tasks;
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .thenProcessAsync(
                  "batchDownloadTrieNodeData",
                  tasks -> {
                    try {
                      return requestDataStep.requestTrieNodeByPath(tasks);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  },
                  maxOutstandingRequests)
              .thenProcess(
                  "batchPersistTrieNodeData",
                  tasks -> {
                    try {
                      persistDataStep.persist(tasks);
                      return tasks;
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .andFinishWith(
                  "batchTrieNodeDataDownloaded", tasks -> tasks.forEach(requestsToComplete::put));

      final Pipeline<Task<SnapDataRequest>> accountFlatDatabaseHealingPipeline =
          createPipelineFrom(
                  "dequeueFlatAccountRequestBlocking",
                  new TaskQueueIterator<>(
                      downloadState,
                      () -> {
                        try {
                          return downloadState.dequeueAccountFlatDatabaseHealingRequestBlocking();
                        } catch (RuntimeException e) {
                          LOG.info("Sync exception detected " + e);
                          e.printStackTrace();
                          throw e;
                        }
                      }),
                  bufferCapacity,
                  outputCounter,
                  true,
                  "world_state_heal")
              .thenProcessAsync(
                  "batchDownloadFlatAccountData",
                  requestTask -> {
                    try {
                      return requestDataStep.requestLocalFlatAccounts(requestTask);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  },
                  maxOutstandingRequests)
              .thenProcess(
                  "batchHealAndPersistFlatAccountData",
                  task -> {
                    try {
                      return persistDataStep.healFlatDatabase(task);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .andFinishWith("batchFlatAccountDataDownloaded", requestsToComplete::put);

      final Pipeline<Task<SnapDataRequest>> storageFlatDatabaseHealingPipeline =
          createPipelineFrom(
                  "dequeueFlatStorageRequestBlocking",
                  new TaskQueueIterator<>(
                      downloadState,
                      () -> {
                        try {
                          return downloadState.dequeueStorageFlatDatabaseHealingRequestBlocking();
                        } catch (RuntimeException e) {
                          LOG.info("Sync exception detected " + e);
                          e.printStackTrace();
                          throw e;
                        }
                      }),
                  bufferCapacity,
                  outputCounter,
                  true,
                  "world_state_heal")
              .thenProcessAsyncOrdered(
                  "batchDownloadFlatStorageData",
                  requestTask -> {
                    try {
                      return requestDataStep.requestLocalFlatStorages(requestTask);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  },
                  maxOutstandingRequests)
              .thenProcess(
                  "batchHealAndPersistFlatStorageData",
                  task -> {
                    try {
                      return persistDataStep.healFlatDatabase(task);
                    } catch (RuntimeException e) {
                      LOG.info("Sync exception detected " + e);
                      e.printStackTrace();
                      throw e;
                    }
                  })
              .andFinishWith("batchFlatStorageDataDownloaded", requestsToComplete::put);

      return new SnapWorldStateDownloadProcess(
          fetchAccountDataPipeline,
          fetchStorageDataPipeline,
          fetchLargeStorageDataPipeline,
          fetchCodePipeline,
          trieHealingPipeline,
          accountFlatDatabaseHealingPipeline,
          storageFlatDatabaseHealingPipeline,
          completionPipeline,
          requestsToComplete);
    }
  }

  private static void reloadHealWhenNeeded(
      final SnapSyncProcessState snapSyncState,
      final SnapWorldDownloadState downloadState,
      final boolean newBlockFound) {
    if (snapSyncState.isHealTrieInProgress() && newBlockFound) {
      downloadState.reloadTrieHeal();
    }
  }
}
