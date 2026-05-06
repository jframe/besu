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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.metrics.StubMetricsSystem;

import org.junit.jupiter.api.Test;

class SnapPipelineMetricsTest {

  @Test
  void exposesAllSevenPipelineLabels() {
    assertThat(SnapPipelineMetrics.PIPELINE_LABELS)
        .containsExactlyInAnyOrder(
            "account",
            "storage",
            "large_storage",
            "code",
            "trie_heal",
            "account_flat_heal",
            "storage_flat_heal");
  }

  @Test
  void inflightTrackerIncrementsAndDecrementsForGivenPipeline() {
    final SnapPipelineMetrics metrics = new SnapPipelineMetrics(new StubMetricsSystem());

    final var first = metrics.trackInflight("account");
    final var second = metrics.trackInflight("account");
    assertThat(metrics.getInflight("account")).isEqualTo(2);
    assertThat(metrics.getInflight("storage")).isEqualTo(0);

    first.close();
    assertThat(metrics.getInflight("account")).isEqualTo(1);

    second.close();
    assertThat(metrics.getInflight("account")).isEqualTo(0);
  }

  @Test
  void inflightTrackerIsIndependentPerPipeline() {
    final SnapPipelineMetrics metrics = new SnapPipelineMetrics(new StubMetricsSystem());

    metrics.trackInflight("storage");
    metrics.trackInflight("code");
    metrics.trackInflight("code");

    assertThat(metrics.getInflight("account")).isEqualTo(0);
    assertThat(metrics.getInflight("storage")).isEqualTo(1);
    assertThat(metrics.getInflight("code")).isEqualTo(2);
  }

  @Test
  void rejectsUnknownPipelineLabel() {
    final SnapPipelineMetrics metrics = new SnapPipelineMetrics(new StubMetricsSystem());

    assertThatThrownBy(() -> metrics.trackInflight("nonexistent"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nonexistent");
  }

  @Test
  void startDequeueTimerReturnsTimingContextForKnownPipeline() {
    final SnapPipelineMetrics metrics = new SnapPipelineMetrics(new StubMetricsSystem());

    try (var ignored = metrics.startDequeueTimer("account")) {
      // smoke: starting and closing must not throw
    }
  }

  @Test
  void startDequeueTimerRejectsUnknownPipeline() {
    final SnapPipelineMetrics metrics = new SnapPipelineMetrics(new StubMetricsSystem());

    assertThatThrownBy(() -> metrics.startDequeueTimer("nonexistent"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
