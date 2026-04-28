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
import java.util.BitSet;
import java.util.function.Consumer;

/**
 * Detects {@code (prefix, rangeId)} transitions in a lex-ordered stream of archive entries and
 * emits {@link RowRecord} / {@link KeyRecord} via callbacks.
 *
 * <p>Holds O(1) per-key state: the current prefix buffer, the current range ID, the running
 * counters, and a fixed-size bitset reused via {@code clear()} on every prefix transition.
 *
 * <p>Not thread-safe; one instance per scan thread.
 */
public final class StreamingAggregator {

  private final long rangeSize;
  private final Consumer<RowRecord> onRow;
  private final Consumer<KeyRecord> onKey;
  private final BitSet rangesSeenForKey;

  private byte[] prevPrefix;
  private long prevRangeId = -1;
  private int rangeEntries;
  private long totalEntriesForKey;

  /**
   * Construct an aggregator.
   *
   * @param rangeSize block-range partition size; must be positive.
   * @param onRow callback invoked once per {@code (prefix, rangeId)} pair at end of that pair.
   * @param onKey callback invoked once per prefix at end of that prefix.
   */
  public StreamingAggregator(
      final long rangeSize, final Consumer<RowRecord> onRow, final Consumer<KeyRecord> onKey) {
    if (rangeSize <= 0) {
      throw new IllegalArgumentException("rangeSize must be positive");
    }
    this.rangeSize = rangeSize;
    this.onRow = onRow;
    this.onKey = onKey;
    // Sized to cover any plausible chain height with the configured rangeSize.
    final long bits = Long.divideUnsigned(0xFFFF_FFFF_FFFF_FFFFL, rangeSize) + 1;
    final int boundedBits = (int) Math.min(bits, 1 << 24); // 16M bits cap = 2 MB, plenty
    this.rangesSeenForKey = new BitSet(boundedBits);
  }

  /**
   * Observe one archive entry. Must be called in lex-sorted key order.
   *
   * @param prefix natural key bytes. Caller may reuse the buffer after the call returns.
   * @param blockNumber block number from the key suffix.
   */
  public void observe(final byte[] prefix, final long blockNumber) {
    final long rangeId = blockNumber / rangeSize;

    if (prevPrefix == null || !Arrays.equals(prevPrefix, prefix)) {
      flushKey();
      prevPrefix = Arrays.copyOf(prefix, prefix.length);
      prevRangeId = rangeId;
      rangeEntries = 1;
      totalEntriesForKey = 0;
      rangesSeenForKey.clear();
      rangesSeenForKey.set(safeBitIndex(rangeId));
    } else if (rangeId != prevRangeId) {
      flushRow();
      prevRangeId = rangeId;
      rangeEntries = 1;
      rangesSeenForKey.set(safeBitIndex(rangeId));
    } else {
      rangeEntries++;
    }
  }

  /** Flush any pending records. Must be called once after the last {@link #observe} of the scan. */
  public void flush() {
    flushKey();
  }

  private void flushKey() {
    if (prevPrefix == null) {
      return;
    }
    flushRow();
    onKey.accept(new KeyRecord(prevPrefix, rangesSeenForKey.cardinality(), totalEntriesForKey));
    prevPrefix = null;
  }

  private void flushRow() {
    totalEntriesForKey += rangeEntries;
    onRow.accept(new RowRecord(prevPrefix, prevRangeId, rangeEntries));
  }

  private int safeBitIndex(final long rangeId) {
    if (rangeId < 0 || rangeId >= rangesSeenForKey.size()) {
      throw new IllegalStateException("rangeId " + rangeId + " out of bitset bounds");
    }
    return (int) rangeId;
  }
}
