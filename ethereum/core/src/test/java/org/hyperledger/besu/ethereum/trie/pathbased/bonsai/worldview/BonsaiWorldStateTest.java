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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BonsaiWorldStateTest {
  @Mock BonsaiWorldStateUpdateAccumulator bonsaiWorldStateUpdateAccumulator;
  @Mock BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater;
  @Mock Blockchain blockchain;
  @Mock BonsaiWorldStateKeyValueStorage bonsaiWorldStateKeyValueStorage;

  private static final Bytes CODE = Bytes.of(10);
  private static final Hash CODE_HASH = Hash.hash(CODE);
  private static final Hash ACCOUNT_HASH = Hash.hash(Address.ZERO.getBytes());
  private static final Address ACCOUNT = Address.ZERO;

  private BonsaiWorldState worldState;

  @BeforeEach
  void setup() {
    worldState =
        new BonsaiWorldState(
            InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain),
            bonsaiWorldStateKeyValueStorage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
  }

  @ParameterizedTest
  @MethodSource("priorAndUpdatedEmptyAndNullBytes")
  void codeUpdateDoesNothingWhenMarkedAsDeletedButAlreadyDeleted(
      final Bytes prior, final Bytes updated) {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(Address.ZERO, new PathBasedValue<>(prior, updated));
    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verifyNoInteractions(bonsaiUpdater);
  }

  @Test
  void codeUpdateDoesNothingWhenAddingSameAsExistingValue() {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(Address.ZERO, new PathBasedValue<>(CODE, CODE));
    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verifyNoInteractions(bonsaiUpdater);
  }

  @ParameterizedTest
  @MethodSource("emptyAndNullBytes")
  void removesCodeWhenMarkedAsDeleted(final Bytes updated) {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(Address.ZERO, new PathBasedValue<>(CODE, updated));
    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verify(bonsaiUpdater).removeCode(ACCOUNT_HASH, CODE_HASH);
  }

  @ParameterizedTest
  @MethodSource("codeValueAndEmptyAndNullBytes")
  void addsCodeForNewCodeValue(final Bytes prior) {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(ACCOUNT, new PathBasedValue<>(prior, CODE));

    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verify(bonsaiUpdater).putCode(ACCOUNT_HASH, CODE_HASH, CODE);
  }

  @Test
  void updateCodeForMultipleValues() {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate = new HashMap<>();
    codeToUpdate.put(Address.fromHexString("0x1"), new PathBasedValue<>(null, CODE));
    codeToUpdate.put(Address.fromHexString("0x2"), new PathBasedValue<>(CODE, null));
    codeToUpdate.put(Address.fromHexString("0x3"), new PathBasedValue<>(Bytes.of(9), CODE));

    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verify(bonsaiUpdater).putCode(Address.fromHexString("0x1").addressHash(), CODE_HASH, CODE);
    verify(bonsaiUpdater).removeCode(Address.fromHexString("0x2").addressHash(), CODE_HASH);
    verify(bonsaiUpdater).putCode(Address.fromHexString("0x3").addressHash(), CODE_HASH, CODE);
  }

  private static Stream<Bytes> emptyAndNullBytes() {
    return Stream.of(Bytes.EMPTY, null);
  }

  private static Stream<Bytes> codeValueAndEmptyAndNullBytes() {
    return Stream.of(Bytes.EMPTY, null);
  }

  private static Stream<Arguments> priorAndUpdatedEmptyAndNullBytes() {
    return Stream.of(
        Arguments.of(null, Bytes.EMPTY),
        Arguments.of(Bytes.EMPTY, null),
        Arguments.of(null, null),
        Arguments.of(Bytes.EMPTY, Bytes.EMPTY));
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void clearStorageTerminatesWithParallelTrieAndMoreThan256Slots() {
    // ParallelStoredMerklePatriciaTrie defers remove() calls to pendingUpdates, so the original
    // clearStorage() loop — which always restarted entriesFrom(Bytes32.ZERO, 256) — would see
    // the same 256 entries every iteration and loop forever. This test fails via timeout without
    // the fix (forward pagination past the last key seen) and passes with it.
    final BonsaiWorldState state =
        new BonsaiWorldState(
            InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain),
            new BonsaiWorldStateKeyValueStorage(
                new InMemoryKeyValueStorageProvider(),
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(), // enables ParallelStoredMerklePatriciaTrie by default
            new CodeCache());

    // Block 1: create an account with 300 storage slots (> 256 batch size)
    final Address address = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final WorldUpdater updater = state.updater();
    final MutableAccount account = updater.createAccount(address, 0, Wei.ONE);
    for (int i = 1; i <= 300; i++) {
      account.setStorageValue(UInt256.valueOf(i), UInt256.ONE);
    }
    updater.commit();
    state.persist(null);

    // Block 2: delete the account — triggers clearStorage() over the 300-slot trie
    final WorldUpdater updater2 = state.updater();
    updater2.deleteAccount(address);
    updater2.commit();
    state.persist(null); // hangs forever without the fix; completes instantly with it

    assertThat(state.get(address)).isNull();
  }

  @Test
  void incrementKeyAddsOneToLastByte() {
    final Bytes32 key = Bytes32.fromHexString("0x" + "00".repeat(31) + "01");
    assertThat(worldState.incrementKey(key))
        .isEqualTo(Bytes32.fromHexString("0x" + "00".repeat(31) + "02"));
  }

  @Test
  void incrementKeyPropagatesCarry() {
    final Bytes32 key = Bytes32.fromHexString("0x" + "00".repeat(31) + "ff");
    assertThat(worldState.incrementKey(key))
        .isEqualTo(Bytes32.fromHexString("0x" + "00".repeat(30) + "0100"));
  }

  @Test
  void incrementKeyOverflowsToZero() {
    final Bytes32 key = Bytes32.fromHexString("0x" + "ff".repeat(32));
    assertThat(worldState.incrementKey(key)).isEqualTo(Bytes32.ZERO);
  }
}
