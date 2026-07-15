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
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class HistoryKeyTest {

  @Test
  void encodeProducesFixedWidthKeyOf10PlusKeyLenBytes() {
    final Bytes naturalKey = Bytes.fromHexString("0x0102030405"); // 5-byte location
    final Bytes key = HistoryKey.encode(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 42L);
    assertThat(key.size()).isEqualTo(10 + naturalKey.size());
    assertThat(HistoryKey.domainOf(key)).isEqualTo(HistoryKey.DOMAIN_ACCOUNT);
    assertThat(HistoryKey.keyLenOf(key)).isEqualTo(naturalKey.size());
    assertThat(HistoryKey.naturalKeyOf(key)).isEqualTo(naturalKey);
    assertThat(HistoryKey.blockOf(key)).isEqualTo(42L);
  }

  @Test
  void storageNaturalKeyRejectsWrongSizedAccountHash() {
    // storageNaturalKey's signature takes a Bytes32, so the "wrong size" rejection happens when
    // constructing that Bytes32 from a short hash: Bytes32.wrap requires an exact 32-byte input
    // (unlike Bytes32.leftPad, which would silently zero-pad it), so it throws here.
    final Bytes shortHash = Bytes.fromHexString("0x1234");
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> HistoryKey.storageNaturalKey(Bytes32.wrap(shortHash), Bytes.EMPTY))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void matchesNodeAcceptsGenuineMatch() {
    final Bytes naturalKey = Bytes.fromHexString("0xabcd");
    final Bytes key = HistoryKey.encode(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 7L);
    assertThat(HistoryKey.matchesNode(key, HistoryKey.DOMAIN_ACCOUNT, naturalKey)).isTrue();
  }

  @Test
  void matchesNodeRejectsDifferentDomain() {
    final Bytes naturalKey = Bytes.fromHexString("0xabcd");
    final Bytes key = HistoryKey.encode(HistoryKey.DOMAIN_ACCOUNT, naturalKey, 7L);
    assertThat(HistoryKey.matchesNode(key, HistoryKey.DOMAIN_STORAGE, naturalKey)).isFalse();
  }

  @Test
  void matchesNodeRejectsDifferentNaturalKeyOfSameLength() {
    final Bytes keyA = Bytes.fromHexString("0xaaaa");
    final Bytes keyB = Bytes.fromHexString("0xbbbb");
    final Bytes entry = HistoryKey.encode(HistoryKey.DOMAIN_ACCOUNT, keyA, 7L);
    assertThat(HistoryKey.matchesNode(entry, HistoryKey.DOMAIN_ACCOUNT, keyB)).isFalse();
  }

  @Test
  void matchesNodeRejectsWrongTotalLength() {
    final Bytes shortKey = Bytes.fromHexString("0x01"); // too short to be a real history key
    assertThat(
            HistoryKey.matchesNode(
                shortKey, HistoryKey.DOMAIN_ACCOUNT, Bytes.fromHexString("0xaa")))
        .isFalse();
  }

  @Test
  void accountAndStorageNaturalKeysOfEqualByteLengthDoNotCollideInEncodedForm() {
    // The historical ambiguity this design closes: a 33-byte account location vs a
    // 32-byte-accountHash + 1-byte-location storage key both produce 33-byte naturalKeys, but the
    // domain byte at position 0 of the encoded key keeps their history entries in disjoint ranges.
    final Bytes accountLocation33 = Bytes.wrap(new byte[33]);
    final Bytes storageAccountHash = Bytes32.ZERO;
    final Bytes storageLocation1 = Bytes.of(0x01);
    final Bytes storageNaturalKey =
        HistoryKey.storageNaturalKey(Bytes32.wrap(storageAccountHash), storageLocation1);
    assertThat(accountLocation33.size()).isEqualTo(storageNaturalKey.size()); // both 33 bytes

    final Bytes accountEntry = HistoryKey.encode(HistoryKey.DOMAIN_ACCOUNT, accountLocation33, 5L);
    final Bytes storageEntry = HistoryKey.encode(HistoryKey.DOMAIN_STORAGE, storageNaturalKey, 5L);
    assertThat(accountEntry).isNotEqualTo(storageEntry);
    assertThat(HistoryKey.domainOf(accountEntry)).isNotEqualTo(HistoryKey.domainOf(storageEntry));
  }
}
