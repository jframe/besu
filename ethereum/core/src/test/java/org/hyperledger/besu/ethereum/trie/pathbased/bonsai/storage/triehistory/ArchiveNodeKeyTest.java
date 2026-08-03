/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.triehistory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ArchiveNodeKeyTest {

  @Test
  void accountNaturalKeyIsLocationUnchanged() {
    final Bytes location = Bytes.fromHexString("0x0102");
    assertThat(ArchiveNodeKey.account(location)).isEqualTo(location);
  }

  @Test
  void storageNaturalKeyIsAccountHashConcatLocation() {
    final Bytes32 accountHash = Bytes32.random();
    final Bytes location = Bytes.fromHexString("0x0a0b");
    assertThat(ArchiveNodeKey.storage(accountHash, location))
        .isEqualTo(Bytes.concatenate(accountHash, location));
  }

  @Test
  void storageNaturalKeyRejectsWrongSizedAccountHash() {
    final Bytes tooShort = Bytes.fromHexString("0x0102");
    final Bytes location = Bytes.EMPTY;
    assertThatThrownBy(() -> ArchiveNodeKey.storage(tooShort, location))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("32");
  }

  @Test
  void historyKeyRoundTripsBlockAndNaturalKeyForBlockZero() {
    final Bytes naturalKey = Bytes.fromHexString("0xabcdef");
    final Bytes key = ArchiveNodeKey.historyKey(naturalKey, 0L);
    assertThat(ArchiveNodeKey.blockFromHistoryKey(key)).isEqualTo(0L);
    assertThat(ArchiveNodeKey.naturalKeyFromHistoryKey(key)).isEqualTo(naturalKey);
  }

  @Test
  void historyKeyRoundTripsBlockAndNaturalKeyForMaxBlock() {
    final Bytes naturalKey = Bytes32.random();
    final Bytes key = ArchiveNodeKey.historyKey(naturalKey, Long.MAX_VALUE);
    assertThat(ArchiveNodeKey.blockFromHistoryKey(key)).isEqualTo(Long.MAX_VALUE);
    assertThat(ArchiveNodeKey.naturalKeyFromHistoryKey(key)).isEqualTo(naturalKey);
  }

  @Test
  void historyKeySortsByNaturalKeyThenByBlockAscending() {
    // Same natural key, ascending blocks must sort ascending lexicographically —
    // this is the property getNearestBefore/seekForPrev relies on for every later task.
    final Bytes naturalKey = Bytes.fromHexString("0x01");
    final Bytes key10 = ArchiveNodeKey.historyKey(naturalKey, 10L);
    final Bytes key11 = ArchiveNodeKey.historyKey(naturalKey, 11L);
    assertThat(key10.compareTo(key11)).isLessThan(0);
  }

  @Test
  void blockFromHistoryKeyRejectsTooShortInput() {
    assertThatThrownBy(() -> ArchiveNodeKey.blockFromHistoryKey(Bytes.fromHexString("0x0102")))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
