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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class TrieNodeDiffCodecTest {

  // A realistic branch-node RLP (17-item list with one child hash + mostly empty slots)
  private static final Bytes BRANCH_NODE_RLP =
      Bytes.fromHexString(
          "0xf85180a0000000000000000000000000000000000000000000000000000000000000000180808080808080808080808080808080");

  // A minimal non-empty bytes for quick round-trip checks
  private static final Bytes SIMPLE_NODE_RLP = Bytes.fromHexString("0xdeadbeef");

  // -------------------------------------------------------------------------
  // FULL entry round-trip (the primary goal of Task 1.1)
  // -------------------------------------------------------------------------

  @Test
  void fullEntryRoundTrips() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);
    assertThat(d.isFull()).isTrue();
    assertThat(d.fullNode()).isEqualTo(BRANCH_NODE_RLP);
  }

  @Test
  void fullEntryRoundTripsSimpleBytes() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(SIMPLE_NODE_RLP);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);
    assertThat(d.isFull()).isTrue();
    assertThat(d.fullNode()).isEqualTo(SIMPLE_NODE_RLP);
  }

  // -------------------------------------------------------------------------
  // Metadata byte: ENTRY_FULL sets bit0 (0x01)
  // -------------------------------------------------------------------------

  @Test
  void fullEntryHasMetadataBit0Set() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP);
    // First byte must have ENTRY_FULL (0x01) set
    assertThat(entry.get(0) & TrieNodeDiffCodec.ENTRY_FULL).isEqualTo(TrieNodeDiffCodec.ENTRY_FULL);
  }

  @Test
  void fullEntryLengthIsOneMoreThanNodeRlp() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP);
    assertThat(entry.size()).isEqualTo(BRANCH_NODE_RLP.size() + 1);
  }

  // -------------------------------------------------------------------------
  // Decoded predicates
  // -------------------------------------------------------------------------

  @Test
  void fullDecodedIsNotDeletion() {
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP));
    assertThat(d.isDeletion()).isFalse();
  }

  @Test
  void fullDecodedIsNotCreation() {
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP));
    assertThat(d.isCreation()).isFalse();
  }

  // -------------------------------------------------------------------------
  // fullNode() throws on non-full entries (guard for later diff entries)
  // -------------------------------------------------------------------------

  @Test
  void fullNodeThrowsWhenCalledOnDiffEntry() {
    // Manually craft a diff entry: metadata byte with ENTRY_FULL bit CLEAR (0x00) + some body
    Bytes diffEntry = Bytes.concatenate(Bytes.of(0x00), SIMPLE_NODE_RLP);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(diffEntry);
    assertThat(d.isFull()).isFalse();
    assertThatThrownBy(d::fullNode).isInstanceOf(IllegalStateException.class);
  }

  // -------------------------------------------------------------------------
  // Metadata constants are defined with correct bit positions
  // -------------------------------------------------------------------------

  @Test
  void metadataConstantsHaveCorrectValues() {
    assertThat(TrieNodeDiffCodec.ENTRY_FULL).isEqualTo((byte) 0x01);
    assertThat(TrieNodeDiffCodec.NODE_IS_BRANCH).isEqualTo((byte) 0x02);
    assertThat(TrieNodeDiffCodec.KEY_CHANGED).isEqualTo((byte) 0x04);
    assertThat(TrieNodeDiffCodec.VALUE_CHANGED).isEqualTo((byte) 0x08);
    assertThat(TrieNodeDiffCodec.CREATION).isEqualTo((byte) 0x10);
    assertThat(TrieNodeDiffCodec.DELETION).isEqualTo((byte) 0x20);
  }
}
