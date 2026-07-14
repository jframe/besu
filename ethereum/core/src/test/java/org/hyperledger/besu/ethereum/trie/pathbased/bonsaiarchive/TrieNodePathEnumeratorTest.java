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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class TrieNodePathEnumeratorTest {

  @Test
  void toNibbles_splitsEachByteIntoTwoNibbles() {
    // 0xAB -> [0x0A, 0x0B]
    assertThat(TrieNodePathEnumerator.toNibbles(Bytes.fromHexString("0xab")))
        .isEqualTo(Bytes.of(0x0a, 0x0b));
  }

  @Test
  void addLocationPrefixes_accountEmitsPrefixesInclusiveOfRootAndSelf() {
    final Bytes nibbles = Bytes.of(0x01, 0x02, 0x03); // 3 nibbles
    final Set<Bytes> out = new LinkedHashSet<>();
    TrieNodePathEnumerator.addLocationPrefixes(nibbles, 2, null, out);
    // depths 0,1,2 -> "", "01", "0102"
    assertThat(out).containsExactly(Bytes.EMPTY, Bytes.of(0x01), Bytes.of(0x01, 0x02));
  }

  @Test
  void addLocationPrefixes_storagePrependsAccountHash() {
    final Bytes acct = Bytes.fromHexString("0x" + "11".repeat(32));
    final Bytes nibbles = Bytes.of(0x0a);
    final Set<Bytes> out = new LinkedHashSet<>();
    TrieNodePathEnumerator.addLocationPrefixes(nibbles, 1, acct, out);
    assertThat(out).containsExactly(acct, Bytes.concatenate(acct, Bytes.of(0x0a)));
  }
}
