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
package org.hyperledger.besu.ethereum.trie.forest.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.worldview.ForestMutableWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BonsaiTrieLogToForestConverterTest {

  private static final Address ALICE =
      Address.fromHexString("0x000000000000000000000000000000000000aa01");
  private static final Address CONTRACT =
      Address.fromHexString("0x000000000000000000000000000000000000cc01");

  private ForestWorldStateKeyValueStorage forestStorage() {
    return new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
  }

  private ForestMutableWorldState oracle(final ForestWorldStateKeyValueStorage storage) {
    return new ForestMutableWorldState(
        storage,
        new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage()),
        EvmConfiguration.DEFAULT);
  }

  private static PmtStateTrieAccountValue account(final long nonce, final long balanceWei) {
    return new PmtStateTrieAccountValue(
        nonce, Wei.of(balanceWei), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
  }

  private static Hash expectedStorageRoot(
      final StorageSlotKey slot1,
      final UInt256 value1,
      final StorageSlotKey slot2,
      final UInt256 value2) {
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.empty(),
            Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()),
            b -> b,
            b -> b);
    trie.put(
        Bytes32.wrap(slot1.getSlotHash().getBytes()),
        RLP.encode(o -> o.writeBytes(value1.toMinimalBytes())));
    trie.put(
        Bytes32.wrap(slot2.getSlotHash().getBytes()),
        RLP.encode(o -> o.writeBytes(value2.toMinimalBytes())));
    return Hash.wrap(trie.getRootHash());
  }

  @Test
  void emptyConverterReportsEmptyTrieRoot() {
    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage());
    assertThat(converter.currentRootHash()).isEqualTo(Hash.EMPTY_TRIE_HASH);
  }

  @Test
  void applyCreatesAccountAndMatchesExpectedStateRoot() {
    final ForestMutableWorldState oracle = oracle(forestStorage());
    final WorldUpdater updater = oracle.updater();
    final MutableAccount alice = updater.createAccount(ALICE);
    alice.setNonce(7);
    alice.setBalance(Wei.of(1234));
    updater.commit();
    oracle.persist(null);
    final Hash expectedRoot = oracle.rootHash();

    final TrieLogLayer layer = new TrieLogLayer();
    layer.addAccountChange(ALICE, null, account(7, 1234));

    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage());
    assertThat(converter.applyTrieLog(layer, expectedRoot)).isEqualTo(expectedRoot);
    assertThat(converter.currentRootHash()).isEqualTo(expectedRoot);
  }

  @Test
  void applyThrowsOnStateRootMismatch() {
    final TrieLogLayer layer = new TrieLogLayer();
    layer.addAccountChange(ALICE, null, account(7, 1234));

    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage());
    assertThatThrownBy(() -> converter.applyTrieLog(layer, Hash.ZERO))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("does not match expected");
  }

  @Test
  void applyPersistsContractCode() {
    final Bytes code = Bytes.fromHexString("0x60016002600055");
    final Hash codeHash = Hash.hash(code);

    final ForestWorldStateKeyValueStorage oracleStorage = forestStorage();
    final ForestMutableWorldState oracle = oracle(oracleStorage);
    final WorldUpdater updater = oracle.updater();
    final MutableAccount contract = updater.createAccount(CONTRACT);
    contract.setNonce(1);
    contract.setCode(code);
    updater.commit();
    oracle.persist(null);
    final Hash expectedRoot = oracle.rootHash();

    final TrieLogLayer layer = new TrieLogLayer();
    layer.addAccountChange(
        CONTRACT, null, new PmtStateTrieAccountValue(1, Wei.ZERO, Hash.EMPTY_TRIE_HASH, codeHash));
    layer.addCodeChange(CONTRACT, Bytes.EMPTY, code, Hash.ZERO);

    final ForestWorldStateKeyValueStorage storage = forestStorage();
    final BonsaiTrieLogToForestConverter converter = new BonsaiTrieLogToForestConverter(storage);
    assertThat(converter.applyTrieLog(layer, expectedRoot)).isEqualTo(expectedRoot);
    assertThat(storage.getCode(codeHash)).contains(code);
  }

  @Test
  void applyRebuildsStorageTrieAndMatchesRoot() {
    final ForestMutableWorldState oracle = oracle(forestStorage());
    final WorldUpdater updater = oracle.updater();
    final MutableAccount contract = updater.createAccount(CONTRACT);
    contract.setNonce(1);
    contract.setStorageValue(UInt256.valueOf(1), UInt256.valueOf(111));
    contract.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(222));
    updater.commit();
    oracle.persist(null);
    final Hash expectedRoot = oracle.rootHash();
    final Hash expectedStorageRoot =
        expectedStorageRoot(
            new StorageSlotKey(UInt256.valueOf(1)), UInt256.valueOf(111),
            new StorageSlotKey(UInt256.valueOf(2)), UInt256.valueOf(222));

    final TrieLogLayer layer = new TrieLogLayer();
    layer.addAccountChange(
        CONTRACT, null, new PmtStateTrieAccountValue(1, Wei.ZERO, expectedStorageRoot, Hash.EMPTY));
    layer.addStorageChange(
        CONTRACT, new StorageSlotKey(UInt256.valueOf(1)), null, UInt256.valueOf(111));
    layer.addStorageChange(
        CONTRACT, new StorageSlotKey(UInt256.valueOf(2)), null, UInt256.valueOf(222));

    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage());
    assertThat(converter.applyTrieLog(layer, expectedRoot)).isEqualTo(expectedRoot);
  }
}
