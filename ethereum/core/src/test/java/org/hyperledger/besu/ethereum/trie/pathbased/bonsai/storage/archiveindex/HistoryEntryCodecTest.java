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

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class HistoryEntryCodecTest {

  // A valid 2-item (short-node) RLP list: path=0x20, value=0x01. Needed because
  // TrieNodeDiffCodec.encodeDiff (when both args are non-null) parses node arity, which requires
  // a well-formed 2-item or 17-item RLP list — an arbitrary short byte sequence is not sufficient.
  private static final Bytes SOME_NODE_RLP = Bytes.fromHexString("0xc22001");

  @Test
  void roundTripsAFullEntry() {
    final Bytes diffPayload = TrieNodeDiffCodec.encodeFull(SOME_NODE_RLP);
    final Bytes encoded =
        HistoryEntryCodec.encode(HistoryEntryCodec.EntryType.FULL, 0, diffPayload);
    final HistoryEntryCodec.Decoded decoded = HistoryEntryCodec.decode(encoded);
    assertThat(decoded.isFull()).isTrue();
    assertThat(decoded.countSinceFull()).isEqualTo(0);
    assertThat(decoded.diffCodecPayload()).isEqualTo(diffPayload);
  }

  @Test
  void roundTripsACreationEntry() {
    final Bytes diffPayload = TrieNodeDiffCodec.encodeDiff(null, SOME_NODE_RLP);
    final Bytes encoded =
        HistoryEntryCodec.encode(HistoryEntryCodec.EntryType.FULL_CREATION, 0, diffPayload);
    final HistoryEntryCodec.Decoded decoded = HistoryEntryCodec.decode(encoded);
    assertThat(decoded.isFull()).isTrue(); // creation entries carry a full node value
    assertThat(decoded.diffCodecPayload()).isEqualTo(diffPayload);
  }

  @Test
  void roundTripsADiffEntryWithNonZeroCount() {
    // Two valid 2-item short nodes with the same path but a different value, so encodeDiff takes
    // the real short-node-diff path (not a degenerate/creation/type-change fallback).
    final Bytes oldRlp = Bytes.fromHexString("0xc22001");
    final Bytes newRlp = Bytes.fromHexString("0xc22002");
    final Bytes diffPayload = TrieNodeDiffCodec.encodeDiff(oldRlp, newRlp);
    final Bytes encoded =
        HistoryEntryCodec.encode(HistoryEntryCodec.EntryType.DIFF, 9, diffPayload);
    final HistoryEntryCodec.Decoded decoded = HistoryEntryCodec.decode(encoded);
    assertThat(decoded.isFull()).isFalse();
    assertThat(decoded.countSinceFull()).isEqualTo(9);
    assertThat(decoded.diffCodecPayload()).isEqualTo(diffPayload);
  }

  @Test
  void countSinceFullSupportsFullCheckpointIntervalRange() {
    // CHECKPOINT_INTERVAL is 16, so values 0..15 must round-trip; the byte can hold far more.
    for (int count = 0; count < 16; count++) {
      final Bytes encoded =
          HistoryEntryCodec.encode(
              HistoryEntryCodec.EntryType.DIFF,
              count,
              TrieNodeDiffCodec.encodeDiff(SOME_NODE_RLP, SOME_NODE_RLP));
      assertThat(HistoryEntryCodec.decode(encoded).countSinceFull()).isEqualTo(count);
    }
  }
}
