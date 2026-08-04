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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class PathBasedWorldStateUpdateAccumulatorTest {

  private static final Address TEST_ADDRESS =
      Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");

  private final Blockchain blockchain = mock(Blockchain.class);

  @Test
  void trustTrieLogPriorValueSeedsFromTrieLogInsteadOfReadingFlatDb() {
    // Flat DB deliberately holds a DIFFERENT prior balance than the trie log records.
    // With the flag set, rollForward must succeed and adopt the trie log's value.
    final var accumulator = newAccumulatorWithFlatBalance(Wei.of(999L));
    accumulator.setTrustTrieLogPriorValue(true);

    accumulator.rollForward(trieLogChangingBalance(Wei.of(1L), Wei.of(2L)));

    assertThat(accumulator.getAccountsToUpdate().get(TEST_ADDRESS).getUpdated().getBalance())
        .isEqualTo(Wei.of(2L));
  }

  @Test
  void withoutTrustTrieLogPriorValueDivergenceIsStillDetected() {
    // Default (flag unset) must preserve existing behaviour: the flat/trie-log mismatch throws.
    final var accumulator = newAccumulatorWithFlatBalance(Wei.of(999L));

    assertThatThrownBy(
            () -> accumulator.rollForward(trieLogChangingBalance(Wei.of(1L), Wei.of(2L))))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void skipCodeRollLeavesCodeUntouched() {
    final var accumulator = newAccumulatorWithFlatBalance(Wei.of(1L));
    accumulator.setSkipCodeRoll(true);

    accumulator.rollForward(
        trieLogChangingCode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0x02")));

    assertThat(accumulator.getCodeToUpdate()).isEmpty();
  }

  // --- helpers ---

  private BonsaiWorldStateUpdateAccumulator newAccumulatorWithFlatBalance(final Wei balance) {
    final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
    final BonsaiWorldState worldState =
        new BonsaiWorldState(
            archive,
            new BonsaiWorldStateKeyValueStorage(
                provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new PathBasedCodeCache());
    final WorldUpdater updater = worldState.updater();
    updater.createAccount(TEST_ADDRESS, 1, balance);
    updater.commit();
    worldState.persist(null);
    return (BonsaiWorldStateUpdateAccumulator) worldState.updater();
  }

  private static TrieLogLayer trieLogChangingBalance(final Wei priorBalance, final Wei newBalance) {
    final TrieLogLayer trieLog = new TrieLogLayer();
    final PmtStateTrieAccountValue prior =
        new PmtStateTrieAccountValue(1, priorBalance, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final PmtStateTrieAccountValue updated =
        new PmtStateTrieAccountValue(1, newBalance, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    trieLog.addAccountChange(TEST_ADDRESS, prior, updated);
    return trieLog;
  }

  private static TrieLogLayer trieLogChangingCode(final Bytes priorCode, final Bytes newCode) {
    final TrieLogLayer trieLog = new TrieLogLayer();
    trieLog.addCodeChange(TEST_ADDRESS, priorCode, newCode, Hash.ZERO);
    return trieLog;
  }
}
