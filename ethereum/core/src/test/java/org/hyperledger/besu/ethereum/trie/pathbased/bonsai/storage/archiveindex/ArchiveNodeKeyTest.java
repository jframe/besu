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
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ArchiveNodeKeyTest {

  private static final Bytes LOCATION = Bytes.fromHexString("0x0102030405");
  private static final Bytes32 ACCOUNT_HASH = Bytes32.random();

  // -----------------------------------------------------------------------
  // Plan's canonical test
  // -----------------------------------------------------------------------

  @Test
  void keysAndRange() {
    Bytes acct = ArchiveNodeKey.account(LOCATION);
    Bytes strg = ArchiveNodeKey.storage(ACCOUNT_HASH, LOCATION);
    assertThat(acct).isEqualTo(LOCATION);
    assertThat(ArchiveNodeKey.rangeId(2_500_000)).isEqualTo(2L);
    assertThat(strg.size()).isEqualTo(32 + LOCATION.size());
  }

  // -----------------------------------------------------------------------
  // account()
  // -----------------------------------------------------------------------

  @Test
  void accountKeyEqualsLocation() {
    assertThat(ArchiveNodeKey.account(LOCATION)).isEqualTo(LOCATION);
  }

  @Test
  void accountKeyEmptyLocation() {
    assertThat(ArchiveNodeKey.account(Bytes.EMPTY)).isEqualTo(Bytes.EMPTY);
  }

  // -----------------------------------------------------------------------
  // storage()
  // -----------------------------------------------------------------------

  @Test
  void storageKeyIsConcatOfAccountHashAndLocation() {
    Bytes key = ArchiveNodeKey.storage(ACCOUNT_HASH, LOCATION);
    assertThat(key.size()).isEqualTo(32 + LOCATION.size());
    assertThat(key.slice(0, 32)).isEqualTo(ACCOUNT_HASH);
    assertThat(key.slice(32)).isEqualTo(LOCATION);
  }

  @Test
  void storageKeyRejectsNon32ByteAccountHash() {
    Bytes badHash = Bytes.of(1, 2, 3); // not 32 bytes
    assertThatThrownBy(() -> ArchiveNodeKey.storage(badHash, LOCATION))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void storageKeyWith32ByteAccountHashAndEmptyLocation() {
    Bytes key = ArchiveNodeKey.storage(ACCOUNT_HASH, Bytes.EMPTY);
    assertThat(key.size()).isEqualTo(32);
  }

  // -----------------------------------------------------------------------
  // rangeId()
  // -----------------------------------------------------------------------

  @Test
  void rangeIdBoundaries() {
    assertThat(ArchiveNodeKey.rangeId(0)).isEqualTo(0L);
    assertThat(ArchiveNodeKey.rangeId(999_999)).isEqualTo(0L);
    assertThat(ArchiveNodeKey.rangeId(1_000_000)).isEqualTo(1L);
    assertThat(ArchiveNodeKey.rangeId(2_500_000)).isEqualTo(2L);
  }

  @Test
  void rangeIdNegativeBlockThrows() {
    assertThatThrownBy(() -> ArchiveNodeKey.rangeId(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // -----------------------------------------------------------------------
  // historyKey() round-trips
  // -----------------------------------------------------------------------

  @Test
  void historyKeyRoundTripAccountNaturalKey() {
    long block = 12_345_678L;
    Bytes naturalKey = ArchiveNodeKey.account(LOCATION);
    Bytes hk = ArchiveNodeKey.historyKey(naturalKey, block);

    assertThat(hk.size()).isEqualTo(naturalKey.size() + 8);
    assertThat(ArchiveNodeKey.blockFromHistoryKey(hk)).isEqualTo(block);
    assertThat(ArchiveNodeKey.naturalKeyFromHistoryKey(hk)).isEqualTo(naturalKey);
  }

  @Test
  void historyKeyRoundTripStorageNaturalKey() {
    long block = 20_000_000L;
    Bytes naturalKey = ArchiveNodeKey.storage(ACCOUNT_HASH, LOCATION);
    Bytes hk = ArchiveNodeKey.historyKey(naturalKey, block);

    assertThat(hk.size()).isEqualTo(naturalKey.size() + 8);
    assertThat(ArchiveNodeKey.blockFromHistoryKey(hk)).isEqualTo(block);
    assertThat(ArchiveNodeKey.naturalKeyFromHistoryKey(hk)).isEqualTo(naturalKey);
  }

  @Test
  void historyKeyLargeBlockNumberBeEncoding() {
    // Use a large block number that exercises upper bytes of BE encoding
    // e.g. 0x0001_0000_0000L = 4,294,967,296 — forces byte at index 3 to be non-zero
    long largeBlock = 0x0001_0000_0000L;
    Bytes naturalKey = ArchiveNodeKey.account(LOCATION);
    Bytes hk = ArchiveNodeKey.historyKey(naturalKey, largeBlock);

    assertThat(ArchiveNodeKey.blockFromHistoryKey(hk)).isEqualTo(largeBlock);
    assertThat(ArchiveNodeKey.naturalKeyFromHistoryKey(hk)).isEqualTo(naturalKey);
  }

  @Test
  void historyKeyZeroBlockNumber() {
    Bytes naturalKey = ArchiveNodeKey.account(LOCATION);
    Bytes hk = ArchiveNodeKey.historyKey(naturalKey, 0L);

    assertThat(ArchiveNodeKey.blockFromHistoryKey(hk)).isEqualTo(0L);
    assertThat(ArchiveNodeKey.naturalKeyFromHistoryKey(hk)).isEqualTo(naturalKey);
  }

  @Test
  void historyKeyExactBytes() {
    // naturalKey(0x0102030405) ‖ block 12 as 8-byte BE (0x000000000000000c)
    Bytes nk = Bytes.fromHexString("0x0102030405");
    assertThat(ArchiveNodeKey.historyKey(nk, 12L))
        .isEqualTo(Bytes.fromHexString("0x0102030405000000000000000c"));
  }

  @Test
  void blockFromHistoryKeyRejectsShortKey() {
    Bytes tooShort = Bytes.fromHexString("0x010203"); // 3 bytes
    assertThatThrownBy(() -> ArchiveNodeKey.blockFromHistoryKey(tooShort))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("historyKey too short");
  }

  @Test
  void naturalKeyFromHistoryKeyRejectsShortKey() {
    Bytes tooShort = Bytes.fromHexString("0x010203"); // 3 bytes
    assertThatThrownBy(() -> ArchiveNodeKey.naturalKeyFromHistoryKey(tooShort))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("historyKey too short");
  }

  // -----------------------------------------------------------------------
  // rangeKey()
  // -----------------------------------------------------------------------

  @Test
  void rangeKeySizeIsNaturalKeyPlusEight() {
    Bytes naturalKey = ArchiveNodeKey.storage(ACCOUNT_HASH, LOCATION);
    long rangeId = ArchiveNodeKey.rangeId(5_000_000L);
    Bytes rk = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    assertThat(rk.size()).isEqualTo(naturalKey.size() + 8);
  }

  @Test
  void rangeKeyEncodesRangeIdBigEndian() {
    Bytes naturalKey = ArchiveNodeKey.account(LOCATION);
    long rangeId = 7L;
    Bytes rk = ArchiveNodeKey.rangeKey(naturalKey, rangeId);
    // Last 8 bytes should encode rangeId
    long decoded = rk.getLong(rk.size() - 8);
    assertThat(decoded).isEqualTo(rangeId);
  }

  @Test
  void rangeKeyExactBytes() {
    // naturalKey(0x0102030405) ‖ rangeId 12 as 8-byte BE (0x000000000000000c)
    Bytes nk = Bytes.fromHexString("0x0102030405");
    assertThat(ArchiveNodeKey.rangeKey(nk, 12L))
        .isEqualTo(Bytes.fromHexString("0x0102030405000000000000000c"));
  }

  // -----------------------------------------------------------------------
  // bloomKey()
  // -----------------------------------------------------------------------

  @Test
  void bloomKeySizeIsEight() {
    assertThat(ArchiveNodeKey.bloomKey(0L).size()).isEqualTo(8);
    assertThat(ArchiveNodeKey.bloomKey(42L).size()).isEqualTo(8);
  }

  @Test
  void bloomKeyEncodesRangeIdBigEndian() {
    long rangeId = 99L;
    Bytes bk = ArchiveNodeKey.bloomKey(rangeId);
    assertThat(bk.getLong(0)).isEqualTo(rangeId);
  }

  @Test
  void bloomKeyExactBytes() {
    assertThat(ArchiveNodeKey.bloomKey(1L)).isEqualTo(Bytes.fromHexString("0x0000000000000001"));
  }
}
