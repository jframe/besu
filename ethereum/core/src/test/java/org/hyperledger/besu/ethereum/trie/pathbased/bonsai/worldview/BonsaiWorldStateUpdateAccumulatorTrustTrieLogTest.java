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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BonsaiWorldStateUpdateAccumulatorTrustTrieLogTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  @Mock private BonsaiWorldState worldState;

  private BonsaiWorldStateUpdateAccumulator accumulator;

  @BeforeEach
  void setUp() {
    accumulator =
        new BonsaiWorldStateUpdateAccumulator(
            worldState,
            (__, ___) -> {},
            (__, ___) -> {},
            EvmConfiguration.DEFAULT,
            new CodeCache());
  }

  @Test
  void skipsFlatDbReadForAccountUpdateWhenTrustingTrieLog() {
    accumulator.setTrustTrieLogPriorValue(true);

    final PmtStateTrieAccountValue prior =
        new PmtStateTrieAccountValue(1, Wei.ONE, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final PmtStateTrieAccountValue updated =
        new PmtStateTrieAccountValue(2, Wei.ONE, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final TrieLogLayer layer = new TrieLogLayer();
    layer.addAccountChange(ADDRESS, prior, updated);

    accumulator.rollForward(layer);

    verify(worldState, never()).get(any());
  }

  @Test
  void skipsFlatDbReadForStorageUpdateWhenTrustingTrieLog() {
    accumulator.setTrustTrieLogPriorValue(true);

    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final TrieLogLayer layer = new TrieLogLayer();
    layer.addStorageChange(ADDRESS, slotKey, UInt256.ONE, UInt256.valueOf(2));

    accumulator.rollForward(layer);

    verify(worldState, never()).getStorageValueByStorageSlotKey(any(), any());
  }
}
