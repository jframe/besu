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

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class MptNodeCodecAdapterTest {

  private final MptNodeCodecAdapter adapter = MptNodeCodecAdapter.INSTANCE;

  private static Bytes hashRef(final int seed) {
    final byte[] b = new byte[33];
    b[0] = (byte) 0xa0;
    for (int i = 1; i < 33; i++) b[i] = (byte) (i + seed);
    return Bytes.wrap(b);
  }

  /** Branch RLP: 16 slots (some hash refs, rest empty) + terminal value. */
  private static Bytes branch(final int[] filledSlots, final Bytes value) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    int fi = 0;
    for (int i = 0; i < 16; i++) {
      if (fi < filledSlots.length && filledSlots[fi] == i) {
        out.writeRaw(hashRef(i));
        fi++;
      } else out.writeNull();
    }
    if (value != null) out.writeBytes(value);
    else out.writeNull();
    out.endList();
    return out.encoded();
  }

  /** Leaf RLP: [compact(path), value]. path is a 3-nibble key + leaf terminator. */
  private static Bytes leaf() {
    final Bytes compactPath =
        org.hyperledger.besu.ethereum.trie.CompactEncoding.encode(Bytes.of(0x01, 0x02, 0x03, 0x10));
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(compactPath);
    out.writeBytes(Bytes.fromHexString("0xdeadbeef"));
    out.endList();
    return out.encoded();
  }

  /** Extension RLP: [compact(path), hashRef]. */
  private static Bytes extension() {
    final Bytes compactPath =
        org.hyperledger.besu.ethereum.trie.CompactEncoding.encode(
            Bytes.of(0x0a, 0x0b)); // even, no terminator
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(compactPath);
    out.writeRaw(hashRef(7));
    out.endList();
    return out.encoded();
  }

  @Test
  void roundTripsSparseBranch() {
    final Bytes node = branch(new int[] {0, 7, 15}, null);
    assertThat(adapter.encode(adapter.parse(node))).isEqualTo(node);
  }

  @Test
  void roundTripsBranchWithTerminalValue() {
    final Bytes node = branch(new int[] {3}, Bytes.fromHexString("0xcafe"));
    assertThat(adapter.encode(adapter.parse(node))).isEqualTo(node);
  }

  @Test
  void roundTripsLeaf() {
    final Bytes node = leaf();
    assertThat(adapter.encode(adapter.parse(node))).isEqualTo(node);
  }

  @Test
  void roundTripsExtension() {
    final Bytes node = extension();
    assertThat(adapter.encode(adapter.parse(node))).isEqualTo(node);
  }

  @Test
  void roundTripsBranchWithEmbeddedChild() {
    // An embedded child is a whole leaf RLP < 32 bytes written inline into a slot.
    final Bytes embedded = leaf(); // small enough to be inlined
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    for (int i = 0; i < 16; i++) {
      if (i == 4) out.writeRaw(embedded);
      else out.writeNull();
    }
    out.writeNull();
    out.endList();
    final Bytes node = out.encoded();
    assertThat(adapter.encode(adapter.parse(node))).isEqualTo(node);
  }

  @Test
  void exposesArityAndFormatTag() {
    assertThat(adapter.arity()).isEqualTo(16);
    assertThat(adapter.formatTag()).isZero();
  }
}
