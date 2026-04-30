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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class SlotFanOutCollectorTest {

  /** Storage prefix = accountHash(32) || slotHash(32). */
  private static byte[] storagePrefix(final int accountByte, final int slotByte) {
    final byte[] p = new byte[64];
    Arrays.fill(p, 0, 32, (byte) accountByte);
    Arrays.fill(p, 32, 64, (byte) slotByte);
    return p;
  }

  @Test
  void singleAccountThreeSlotsAllInOneRangeProducesOneObservationOfThree() {
    final SlotFanOutCollector c = new SlotFanOutCollector();
    c.accept(new RowRecord(storagePrefix(0xaa, 0x01), 0L, 5));
    c.accept(new RowRecord(storagePrefix(0xaa, 0x02), 0L, 1));
    c.accept(new RowRecord(storagePrefix(0xaa, 0x03), 0L, 9));
    c.flush();

    final SlotFanOutResult r = c.result();
    assertThat(r.totalAccountRangePairs()).isEqualTo(1L);
    assertThat(r.histogram().total()).isEqualTo(1L);
    assertThat(r.histogram().max()).isEqualTo(3L);
  }

  @Test
  void singleAccountSlotSpansTwoRangesProducesTwoObservations() {
    final SlotFanOutCollector c = new SlotFanOutCollector();
    // Slot 0x01 in range 0 and range 5 (different rangeIds for same slot under same account).
    c.accept(new RowRecord(storagePrefix(0xaa, 0x01), 0L, 3));
    c.accept(new RowRecord(storagePrefix(0xaa, 0x01), 5L, 2));
    // Slot 0x02 in range 0 only.
    c.accept(new RowRecord(storagePrefix(0xaa, 0x02), 0L, 1));
    c.flush();

    final SlotFanOutResult r = c.result();
    // Range 0 saw 2 distinct slots; range 5 saw 1 distinct slot.
    // -> histogram observations are {2, 1}, total pairs = 2.
    assertThat(r.totalAccountRangePairs()).isEqualTo(2L);
    assertThat(r.histogram().total()).isEqualTo(2L);
    assertThat(r.histogram().max()).isEqualTo(2L);
  }

  @Test
  void twoAccountsBackToBackEachInDistinctRanges() {
    final SlotFanOutCollector c = new SlotFanOutCollector();
    // Account 0xaa: 3 slots in range 0.
    c.accept(new RowRecord(storagePrefix(0xaa, 0x01), 0L, 1));
    c.accept(new RowRecord(storagePrefix(0xaa, 0x02), 0L, 1));
    c.accept(new RowRecord(storagePrefix(0xaa, 0x03), 0L, 1));
    // Account 0xbb: 1 slot in range 1, 2 slots in range 2.
    c.accept(new RowRecord(storagePrefix(0xbb, 0x10), 1L, 1));
    c.accept(new RowRecord(storagePrefix(0xbb, 0x20), 2L, 1));
    c.accept(new RowRecord(storagePrefix(0xbb, 0x21), 2L, 1));
    c.flush();

    final SlotFanOutResult r = c.result();
    // Observations: {3 (aa,0), 1 (bb,1), 2 (bb,2)} -> 3 pairs, max 3.
    assertThat(r.totalAccountRangePairs()).isEqualTo(3L);
    assertThat(r.histogram().total()).isEqualTo(3L);
    assertThat(r.histogram().max()).isEqualTo(3L);
  }
}
