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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-pipeline observability for snap sync world state download. Owns the inflight counters and
 * timer metrics that allow each Phase-1 change to be attributed to a specific pipeline.
 */
public class SnapPipelineMetrics {

  public static final List<String> PIPELINE_LABELS =
      List.of(
          "account",
          "storage",
          "large_storage",
          "code",
          "trie_heal",
          "account_flat_heal",
          "storage_flat_heal");

  private final Map<String, AtomicInteger> inflightByPipeline;
  private final LabelledMetric<OperationTimer> dequeueWaitTimer;

  public SnapPipelineMetrics(final MetricsSystem metricsSystem) {
    this.inflightByPipeline = new LinkedHashMap<>();
    final LabelledSuppliedMetric inflightGauge =
        metricsSystem.createLabelledSuppliedGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_pipeline_inflight_requests",
            "Number of in-flight requests for each snap sync pipeline",
            "pipeline");
    for (final String label : PIPELINE_LABELS) {
      final AtomicInteger counter = new AtomicInteger();
      inflightByPipeline.put(label, counter);
      inflightGauge.labels(counter::doubleValue, label);
    }
    this.dequeueWaitTimer =
        metricsSystem.createLabelledTimer(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_pipeline_dequeue_wait_seconds",
            "Time spent blocked waiting to dequeue the next request, per snap sync pipeline",
            "pipeline");
  }

  /** A handle that decrements the inflight counter when closed. Never throws. */
  @FunctionalInterface
  public interface InflightHandle {
    void close();
  }

  public OperationTimer.TimingContext startDequeueTimer(final String pipeline) {
    requireKnownPipeline(pipeline);
    return dequeueWaitTimer.labels(pipeline).startTimer();
  }

  public InflightHandle trackInflight(final String pipeline) {
    final AtomicInteger counter = requireKnownPipeline(pipeline);
    counter.incrementAndGet();
    return counter::decrementAndGet;
  }

  /** Test-only accessor — returns the current inflight count for a pipeline. */
  int getInflight(final String pipeline) {
    return requireKnownPipeline(pipeline).get();
  }

  private AtomicInteger requireKnownPipeline(final String pipeline) {
    final AtomicInteger counter = inflightByPipeline.get(pipeline);
    if (counter == null) {
      throw new IllegalArgumentException("Unknown snap sync pipeline label: " + pipeline);
    }
    return counter;
  }
}
