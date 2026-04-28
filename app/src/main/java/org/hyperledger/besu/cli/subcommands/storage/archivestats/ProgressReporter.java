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
package org.hyperledger.besu.cli.subcommands.storage.archivestats;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.function.LongSupplier;

/** Periodically logs scan throughput to a {@link PrintWriter}. */
public final class ProgressReporter {

  private final PrintWriter out;
  private final Duration interval;
  private final LongSupplier nowMillis;
  private long startMillis;
  private long lastReportMillis;
  private long lastReportKeys;
  private String currentLabel;

  /**
   * Construct a progress reporter.
   *
   * @param out where to write progress lines
   * @param interval minimum elapsed time between progress lines
   * @param nowMillis clock supplier; inject for testability
   */
  public ProgressReporter(
      final PrintWriter out, final Duration interval, final LongSupplier nowMillis) {
    this.out = out;
    this.interval = interval;
    this.nowMillis = nowMillis;
  }

  /**
   * Convenience factory using {@link System#currentTimeMillis()}.
   *
   * @param out where to write progress lines
   * @param interval minimum elapsed time between progress lines
   * @return a wall-clock progress reporter
   */
  public static ProgressReporter wallClock(final PrintWriter out, final Duration interval) {
    return new ProgressReporter(out, interval, System::currentTimeMillis);
  }

  /**
   * Mark the start of scanning a CF; resets counters.
   *
   * @param cfLabel CF name for log prefixes
   * @param estimatedTotal estimated total keys for ETA calculation
   */
  public void beginCf(final String cfLabel, final long estimatedTotal) {
    this.currentLabel = cfLabel;
    this.startMillis = nowMillis.getAsLong();
    this.lastReportMillis = startMillis;
    this.lastReportKeys = 0L;
    out.printf("[%s] starting (estimated %d keys)%n", cfLabel, estimatedTotal);
    out.flush();
  }

  /**
   * Tick on each observed key. Emits a progress line when {@link #interval} has elapsed.
   *
   * @param keysSoFar running total of observed keys
   * @param estimatedTotal estimated total keys for ETA calculation
   */
  public void tick(final long keysSoFar, final long estimatedTotal) {
    final long now = nowMillis.getAsLong();
    if (now - lastReportMillis < interval.toMillis()) {
      return;
    }
    final long deltaKeys = keysSoFar - lastReportKeys;
    final long deltaMillis = Math.max(now - lastReportMillis, 1L);
    final double rate = (deltaKeys * 1000.0) / deltaMillis;
    final double pct = estimatedTotal > 0 ? 100.0 * keysSoFar / estimatedTotal : 0.0;
    final long remainingKeys = Math.max(estimatedTotal - keysSoFar, 0L);
    final long etaSeconds = rate > 0 ? Math.round(remainingKeys / rate) : -1L;

    out.printf(
        "[%s] %d / %d keys (%.1f%%) | %.0f keys/s | ETA %s%n",
        currentLabel,
        keysSoFar,
        estimatedTotal,
        pct,
        rate,
        etaSeconds < 0 ? "?" : (etaSeconds + "s"));
    out.flush();

    lastReportMillis = now;
    lastReportKeys = keysSoFar;
  }

  /**
   * Mark CF completion.
   *
   * @param totalKeys final total of observed keys for this CF
   */
  public void endCf(final long totalKeys) {
    final long elapsed = Math.max(nowMillis.getAsLong() - startMillis, 1L);
    out.printf(
        "[%s] done: %d keys in %ds%n",
        currentLabel, totalKeys, Duration.ofMillis(elapsed).toSeconds());
    out.flush();
  }
}
