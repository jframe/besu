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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Collects "distinct slots per {@code (accountHash, rangeId)}" observations from storage-CF {@link
 * RowRecord} events.
 *
 * <p>Storage prefixes are {@code accountHash(32) || slotHash(32)}. Within the lex-ordered stream
 * the aggregator emits one {@code RowRecord} per {@code (prefix, rangeId)} pair, so each call to
 * {@link #accept(RowRecord)} represents a distinct {@code (account, slot, range)} triple. The
 * collector groups by current account (first 32 bytes of the prefix), counts slots per rangeId,
 * and on account transitions flushes one observation per range to an internal histogram.
 *
 * <p>Use only with the storage CF. Behaviour is undefined for prefixes shorter than 32 bytes.
 *
 * <p>Not thread-safe. One instance per scan thread.
 */
public final class SlotFanOutCollector {

  /** Accounts touched per row are bounded; pick a generous cap for the histogram. */
  private static final int HISTOGRAM_BUCKETS = 28;

  private final HistogramCollector histogram = HistogramCollector.log(HISTOGRAM_BUCKETS);
  private final Map<Long, Long> perRangeSlots = new HashMap<>();

  private byte[] currentAccount;
  private long totalAccountRangePairs;

  /** Construct an empty collector. */
  public SlotFanOutCollector() {}

  /**
   * Observe one storage {@link RowRecord}.
   *
   * <p>Caller guarantees {@code row.prefix().length >= 32}.
   *
   * @param row the row event from the streaming aggregator
   */
  public void accept(final RowRecord row) {
    final byte[] prefix = row.prefix();
    if (currentAccount == null || !Arrays.equals(currentAccount, 0, 32, prefix, 0, 32)) {
      flushCurrentAccount();
      currentAccount = Arrays.copyOfRange(prefix, 0, 32);
    }
    perRangeSlots.merge(row.rangeId(), 1L, Long::sum);
  }

  /** Flush any pending observation. Call once after the final {@link #accept(RowRecord)}. */
  public void flush() {
    flushCurrentAccount();
    currentAccount = null;
  }

  /**
   * Snapshot of accumulated stats. Safe to call after {@link #flush()}.
   *
   * @return the result record
   */
  public SlotFanOutResult result() {
    return new SlotFanOutResult(histogram, totalAccountRangePairs);
  }

  private void flushCurrentAccount() {
    if (currentAccount == null) {
      return;
    }
    for (final long count : perRangeSlots.values()) {
      histogram.record(count);
      totalAccountRangePairs++;
    }
    perRangeSlots.clear();
  }
}
