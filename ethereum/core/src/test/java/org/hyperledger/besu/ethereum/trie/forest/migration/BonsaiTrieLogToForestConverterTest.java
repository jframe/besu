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

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.worldview.ForestMutableWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

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

  private static PmtStateTrieAccountValue acct(
      final long nonce, final long balanceWei, final Hash storageRoot, final Hash codeHash) {
    return new PmtStateTrieAccountValue(nonce, Wei.of(balanceWei), storageRoot, codeHash);
  }

  private static Hash expectedStorageRoot(
      final StorageSlotKey slot1,
      final UInt256 value1,
      final StorageSlotKey slot2,
      final UInt256 value2) {
    return storageRootOf(Map.of(slot1, value1, slot2, value2));
  }

  /**
   * Computes the storage trie root for a fresh (empty-start) storage trie populated with the given
   * non-zero slot values, mirroring how the converter rebuilds storage roots.
   */
  private static Hash storageRootOf(final Map<StorageSlotKey, UInt256> slots) {
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.empty(),
            Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()),
            b -> b,
            b -> b);
    slots.forEach(
        (slot, value) ->
            trie.put(
                Bytes32.wrap(slot.getSlotHash().getBytes()),
                RLP.encode(o -> o.writeBytes(value.toMinimalBytes()))));
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

  @Test
  void zeroingSlotInLaterBlockUpdatesRoot() {
    final StorageSlotKey slot1 = new StorageSlotKey(UInt256.valueOf(1));
    final StorageSlotKey slot2 = new StorageSlotKey(UInt256.valueOf(2));

    // Oracle: block 1 creates two slots, block 2 zeroes slot2.
    final ForestMutableWorldState oracle = oracle(forestStorage());
    final WorldUpdater updater1 = oracle.updater();
    final MutableAccount contract1 = updater1.createAccount(CONTRACT);
    contract1.setNonce(1);
    contract1.setStorageValue(UInt256.valueOf(1), UInt256.valueOf(111));
    contract1.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(222));
    updater1.commit();
    oracle.persist(null);
    final Hash root1 = oracle.rootHash();
    final Hash sroot1 =
        storageRootOf(Map.of(slot1, UInt256.valueOf(111), slot2, UInt256.valueOf(222)));

    final WorldUpdater updater2 = oracle.updater();
    final MutableAccount contract2 = updater2.getAccount(CONTRACT);
    contract2.setStorageValue(UInt256.valueOf(2), UInt256.ZERO);
    updater2.commit();
    oracle.persist(null);
    final Hash root2 = oracle.rootHash();
    final Hash sroot2 = storageRootOf(Map.of(slot1, UInt256.valueOf(111)));

    // Converter replays both blocks against a fresh storage, exercising running-root continuity.
    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage());

    final TrieLogLayer block1 = new TrieLogLayer();
    block1.addAccountChange(CONTRACT, null, acct(1, 0, sroot1, Hash.EMPTY));
    block1.addStorageChange(CONTRACT, slot1, null, UInt256.valueOf(111));
    block1.addStorageChange(CONTRACT, slot2, null, UInt256.valueOf(222));
    assertThat(converter.applyTrieLog(block1, root1)).isEqualTo(root1);

    final TrieLogLayer block2 = new TrieLogLayer();
    block2.addAccountChange(
        CONTRACT, acct(1, 0, sroot1, Hash.EMPTY), acct(1, 0, sroot2, Hash.EMPTY));
    block2.addStorageChange(CONTRACT, slot2, UInt256.valueOf(222), null);
    assertThat(converter.applyTrieLog(block2, root2)).isEqualTo(root2);
  }

  @Test
  void selfDestructThenRecreateWithFreshStorage() {
    final StorageSlotKey oldSlot = new StorageSlotKey(UInt256.valueOf(1));
    final StorageSlotKey sideSlot = new StorageSlotKey(UInt256.valueOf(3));
    final StorageSlotKey newSlot = new StorageSlotKey(UInt256.valueOf(9));

    // Oracle: block 1 creates con with oldSlot=5 and sideSlot=99.
    final ForestMutableWorldState oracle = oracle(forestStorage());
    final WorldUpdater updater1 = oracle.updater();
    final MutableAccount contract1 = updater1.createAccount(CONTRACT);
    contract1.setNonce(1);
    contract1.setStorageValue(UInt256.valueOf(1), UInt256.valueOf(5));
    contract1.setStorageValue(UInt256.valueOf(3), UInt256.valueOf(99));
    updater1.commit();
    oracle.persist(null);
    final Hash root1 = oracle.rootHash();
    final Hash sroot1 =
        storageRootOf(Map.of(oldSlot, UInt256.valueOf(5), sideSlot, UInt256.valueOf(99)));

    // Block 2: selfdestruct con (wiping oldSlot AND sideSlot), then recreate with ONLY newSlot=7.
    final WorldUpdater destroyUpdater = oracle.updater();
    destroyUpdater.deleteAccount(CONTRACT);
    destroyUpdater.commit();
    final WorldUpdater recreateUpdater = oracle.updater();
    final MutableAccount recreated = recreateUpdater.createAccount(CONTRACT);
    recreated.setNonce(1);
    recreated.setStorageValue(UInt256.valueOf(9), UInt256.valueOf(7));
    recreateUpdater.commit();
    oracle.persist(null);
    final Hash root2 = oracle.rootHash();
    final Hash sroot2 = storageRootOf(Map.of(newSlot, UInt256.valueOf(7)));

    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage());

    final TrieLogLayer block1 = new TrieLogLayer();
    block1.addAccountChange(CONTRACT, null, acct(1, 0, sroot1, Hash.EMPTY));
    block1.addStorageChange(CONTRACT, oldSlot, null, UInt256.valueOf(5));
    block1.addStorageChange(CONTRACT, sideSlot, null, UInt256.valueOf(99));
    assertThat(converter.applyTrieLog(block1, root1)).isEqualTo(root1);

    final TrieLogLayer block2 = new TrieLogLayer();
    block2.addAccountChange(
        CONTRACT, acct(1, 0, sroot1, Hash.EMPTY), acct(1, 0, sroot2, Hash.EMPTY));
    // The old slot is removed by the destruction.
    block2.addStorageChange(CONTRACT, oldSlot, UInt256.valueOf(5), null);
    // The recreated slot carries the clear flag so the storage trie resets to empty before reapply.
    // sideSlot is intentionally NOT listed here: only the clear flag can drop it from the trie, so
    // this slot makes the converter's clear branch load-bearing. Without it, replay would start
    // from sroot1 (which still contains sideSlot=99) and produce the wrong storage root.
    block2
        .getStorageChanges()
        .computeIfAbsent(CONTRACT, k -> new TreeMap<>())
        .put(newSlot, new PathBasedValue<>(null, UInt256.valueOf(7), true));
    assertThat(converter.applyTrieLog(block2, root2)).isEqualTo(root2);
  }

  @Test
  void seedGenesisMatchesGenesisStateRoot() {
    final String genesisJson =
        "{"
            + "\"config\": {\"chainId\": 15, \"eip158Block\": 0},"
            + "\"alloc\": {"
            + "  \"0x0000000000000000000000000000000000000001\": {\"balance\": \"111111111\"},"
            + "  \"0x0000000000000000000000000000000000000002\": {\"balance\": \"222222222\"}"
            + "},"
            + "\"coinbase\": \"0x0000000000000000000000000000000000000000\","
            + "\"difficulty\": \"0x0000001\","
            + "\"gasLimit\": \"0x2fefd8\""
            + "}";
    final GenesisConfig genesisConfig = GenesisConfig.fromConfig(genesisJson);
    final GenesisState genesisState =
        GenesisState.fromConfig(genesisConfig, ProtocolScheduleFixture.MAINNET, new CodeCache());
    final Hash genesisRoot = genesisState.getBlock().getHeader().getStateRoot();

    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage());
    converter.seedGenesis(
        genesisState,
        new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage()),
        EvmConfiguration.DEFAULT);

    assertThat(converter.currentRootHash()).isEqualTo(genesisRoot);
  }
}
