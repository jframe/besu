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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class FlatDbStorageLeafCountProviderTest {

  private static final Bytes ACCOUNT_A = Bytes32.leftPad(Bytes.fromHexString("0xaa11"));
  private static final Bytes ACCOUNT_B = Bytes32.leftPad(Bytes.fromHexString("0xbb22"));

  private final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();

  private void putSlots(final Bytes accountHash, final int count) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    for (int i = 0; i < count; i++) {
      final Bytes slotHash = Bytes32.leftPad(Bytes.ofUnsignedInt(i));
      tx.put(
          ACCOUNT_STORAGE_STORAGE,
          Bytes.concatenate(accountHash, slotHash).toArrayUnsafe(),
          Bytes.of(1).toArrayUnsafe());
    }
    tx.commit();
  }

  @Test
  void countsOnlyTheRequestedAccountsSlots() {
    putSlots(ACCOUNT_A, 5);
    putSlots(ACCOUNT_B, 3);

    final FlatDbStorageLeafCountProvider provider =
        new FlatDbStorageLeafCountProvider(storage, 1_000);

    assertThat(provider.leafCount(ACCOUNT_A)).isEqualTo(5L);
    assertThat(provider.leafCount(ACCOUNT_B)).isEqualTo(3L);
  }

  @Test
  void returnsZeroForAccountWithNoStorage() {
    putSlots(ACCOUNT_A, 4);
    final FlatDbStorageLeafCountProvider provider =
        new FlatDbStorageLeafCountProvider(storage, 1_000);
    assertThat(provider.leafCount(ACCOUNT_B)).isZero();
  }

  @Test
  void capsTheCount() {
    putSlots(ACCOUNT_A, 50);
    final FlatDbStorageLeafCountProvider provider = new FlatDbStorageLeafCountProvider(storage, 10);
    assertThat(provider.leafCount(ACCOUNT_A)).isEqualTo(10L);
  }

  @Test
  void cachesTheResultAcrossCalls() {
    putSlots(ACCOUNT_A, 5);
    final FlatDbStorageLeafCountProvider provider =
        new FlatDbStorageLeafCountProvider(storage, 1_000);
    final long first = provider.leafCount(ACCOUNT_A);

    // Mutate the store after the first probe; a cached provider must not observe the change.
    putSlots(ACCOUNT_A, 20);
    assertThat(provider.leafCount(ACCOUNT_A)).isEqualTo(first).isEqualTo(5L);
  }
}
