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

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class NodeLogCodecTest {

  private final NodeCodecAdapter mpt = MptNodeCodecAdapter.INSTANCE;

  private static Bytes hashRef(final int seed) {
    final byte[] b = new byte[33];
    b[0] = (byte) 0xa0;
    for (int i = 1; i < 33; i++) b[i] = (byte) (i + seed);
    return Bytes.wrap(b);
  }

  private static Bytes branch(final int[] filled, final int[] seeds) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    int fi = 0;
    for (int i = 0; i < 16; i++) {
      if (fi < filled.length && filled[fi] == i) {
        out.writeRaw(hashRef(seeds[fi]));
        fi++;
      } else out.writeNull();
    }
    out.writeNull();
    out.endList();
    return out.encoded();
  }

  @Test
  void sparseChildChangeReconstructsExactly() {
    final Bytes v0 = branch(new int[] {1, 5}, new int[] {1, 5});
    final Bytes v1 = branch(new int[] {1, 5}, new int[] {1, 99}); // slot 5 changed

    final Bytes diffEntry = NodeLogCodec.encodeDiff(mpt, v0, v1);
    assertThat(ArchiveTrieNodeCodec.decode(diffEntry).isFull())
        .as("a small child change should be a DIFF, not a FULL")
        .isFalse();
    assertThat(diffEntry.size()).isLessThan(ArchiveTrieNodeCodec.encodeFull(v1).size());

    final Bytes base = ArchiveTrieNodeCodec.encodeFull(v0);
    assertThat(NodeLogCodec.reconstruct(base, List.of(diffEntry))).isEqualTo(v1);
  }

  @Test
  void multiStepChainReconstructsEveryVersion() {
    final Bytes v0 = branch(new int[] {0}, new int[] {0});
    final Bytes v1 = branch(new int[] {0, 3}, new int[] {0, 3}); // add slot 3
    final Bytes v2 = branch(new int[] {0, 3}, new int[] {0, 7}); // change slot 3
    final Bytes d1 = NodeLogCodec.encodeDiff(mpt, v0, v1);
    final Bytes d2 = NodeLogCodec.encodeDiff(mpt, v1, v2);
    final Bytes base = ArchiveTrieNodeCodec.encodeFull(v0);

    assertThat(NodeLogCodec.reconstruct(base, List.of())).isEqualTo(v0);
    assertThat(NodeLogCodec.reconstruct(base, List.of(d1))).isEqualTo(v1);
    assertThat(NodeLogCodec.reconstruct(base, List.of(d1, d2))).isEqualTo(v2);
  }

  @Test
  void creationEncodesAsFullCreation() {
    final Bytes v0 = branch(new int[] {2}, new int[] {2});
    final Bytes entry = NodeLogCodec.encodeDiff(mpt, null, v0);
    final ArchiveTrieNodeEntry decoded = ArchiveTrieNodeCodec.decode(entry);
    assertThat(decoded.isFull()).isTrue();
    assertThat(decoded.isCreation()).isTrue();
    assertThat(decoded.fullNode()).isEqualTo(v0);
    assertThat(decoded.formatTag()).isZero();
  }

  @Test
  void deletionEncodesAsTombstone() {
    final Bytes v0 = branch(new int[] {2}, new int[] {2});
    final Bytes entry = NodeLogCodec.encodeDiff(mpt, v0, null);
    assertThat(ArchiveTrieNodeCodec.decode(entry).isDeletion()).isTrue();
  }

  @Test
  void correctnessGuardFallsBackToFull_whenAdapterCannotReproduce() {
    // A deliberately-lossy adapter whose encode never matches -> guard must store a FULL.
    final NodeCodecAdapter broken =
        new NodeCodecAdapter() {
          @Override
          public NodeLog parse(final Bytes b) {
            return mpt.parse(b);
          }

          @Override
          public Bytes encode(final NodeLog m) {
            return Bytes.fromHexString("0xdead"); // wrong
          }

          @Override
          public int arity() {
            return 16;
          }

          @Override
          public int formatTag() {
            return 0;
          }
        };
    final Bytes v0 = branch(new int[] {1}, new int[] {1});
    final Bytes v1 = branch(new int[] {1}, new int[] {2});
    final Bytes entry = NodeLogCodec.encodeDiff(broken, v0, v1);
    assertThat(ArchiveTrieNodeCodec.decode(entry).isFull())
        .as("non-reproducible diff must fall back to FULL")
        .isTrue();
    assertThat(ArchiveTrieNodeCodec.decode(entry).fullNode()).isEqualTo(v1);
  }

  @Test
  void encodeFullTagsEntryCorrectly() {
    final Bytes node = branch(new int[] {0}, new int[] {0});
    final Bytes entry = NodeLogCodec.encodeFull(MptNodeCodecAdapter.INSTANCE, node);
    final ArchiveTrieNodeEntry decoded = ArchiveTrieNodeCodec.decode(entry);
    assertThat(decoded.isFull()).isTrue();
    assertThat(decoded.isCreation()).isFalse();
    assertThat(decoded.formatTag()).isZero();
  }

  @Test
  void sizeGuardFallsBackToFull_whenDiffNotSmaller() {
    // Two disjoint nodes: the mutation body is not smaller than the node -> FULL.
    final Bytes v0 =
        branch(
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
    final Bytes v1 =
        branch(
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
            new int[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65});
    final Bytes entry = NodeLogCodec.encodeDiff(mpt, v0, v1);
    assertThat(ArchiveTrieNodeCodec.decode(entry).isFull()).isTrue();
  }
}
