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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeEntryTest {
  @Test
  void formatTagIsReadFromHighBits_andDoesNotDisturbFlags() {
    // MPT DIFF: metadata 0x00 -> tag 0, not full/creation/deletion.
    final ArchiveTrieNodeEntry mptDiff =
        ArchiveTrieNodeCodec.decode(Bytes.of((byte) 0x00, 0x11, 0x22));
    assertThat(mptDiff.formatTag()).isZero();
    assertThat(mptDiff.isFull()).isFalse();

    // PBT DIFF: metadata 0x40 (bit 6) -> tag 1, still a diff (bit 0 unset).
    final ArchiveTrieNodeEntry pbtDiff = ArchiveTrieNodeCodec.decode(Bytes.of((byte) 0x40, 0x11));
    assertThat(pbtDiff.formatTag()).isEqualTo(1);
    assertThat(pbtDiff.isFull()).isFalse();
    assertThat(pbtDiff.isDeletion()).isFalse();

    // FULL still detected regardless of format bits: 0x41 = PBT | FULL.
    final ArchiveTrieNodeEntry pbtFull = ArchiveTrieNodeCodec.decode(Bytes.of((byte) 0x41, 0x99));
    assertThat(pbtFull.isFull()).isTrue();
    assertThat(pbtFull.formatTag()).isEqualTo(1);
  }
}
