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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeCodecTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static Bytes node(final int... bytes) {
    final byte[] b = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      b[i] = (byte) bytes[i];
    }
    return Bytes.wrap(b);
  }

  // ---------------------------------------------------------------------------
  // encodeFull / decode
  // ---------------------------------------------------------------------------

  @Test
  void encodeFullRoundTrips() {
    final Bytes n = node(0xAA, 0xBB, 0xCC);
    final ArchiveTrieNodeEntry e = ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeFull(n));
    assertThat(e.isFull()).isTrue();
    assertThat(e.isDeletion()).isFalse();
    assertThat(e.fullNode()).isEqualTo(n);
  }

  // ---------------------------------------------------------------------------
  // ArchiveTrieNodeEntry.patchBody()
  // ---------------------------------------------------------------------------

  @Test
  void patchBodyThrowsOnFullEntry() {
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeFull(node(0x01)));
    assertThatThrownBy(e::patchBody).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void patchBodyThrowsOnDeletionEntry() {
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(Bytes.of(ArchiveTrieNodeEntry.DELETION));
    assertThatThrownBy(e::patchBody).isInstanceOf(IllegalStateException.class);
  }
}
