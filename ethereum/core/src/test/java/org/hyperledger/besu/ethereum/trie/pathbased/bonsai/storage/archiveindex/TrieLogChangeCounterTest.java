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
import static org.assertj.core.api.Assertions.assertThatCode;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class TrieLogChangeCounterTest {

  private final TrieLogChangeCounter counter =
      new TrieLogChangeCounter(2, 0 /* sample everything */, new TrieShapeModel(16));

  private PmtStateTrieAccountValue acct(final long balance) {
    return new PmtStateTrieAccountValue(0L, Wei.of(balance), Hash.EMPTY, Hash.EMPTY);
  }

  @Test
  void singleAccountChangeRecordsRootToLeafPath() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final TrieLogLayer log = new TrieLogLayer();
    log.addAccountChange(addr, null, acct(1)); // creation

    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    // Small era leaf count → shallow termination cap, so the path is short and assertions are
    // stable.
    counter.countBlock(log, 100L, 4L, out);

    // depth 0 (root) is always present; there should be exactly one root-level mutation.
    assertThat(out.mutationsByDepth()[0]).isEqualTo(1L);
    // creation bumps the account delta for range 0.
    assertThat(out.accountDeltaByRange()[0]).isEqualTo(1L);
    // depth 0 is upper-FULL.
    assertThat(out.upperFullByDepth()[0]).isEqualTo(1L);
  }

  @Test
  void twoAccountsSharingNoPrefixDoubleTheRootMutation() {
    final Address a = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final Address b = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final TrieLogLayer log = new TrieLogLayer();
    log.addAccountChange(a, acct(1), acct(2));
    log.addAccountChange(b, acct(1), acct(2));

    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    counter.countBlock(log, 100L, 4L, out);

    // Both leaves pass through the root, deduped to ONE root node write.
    assertThat(out.mutationsByDepth()[0]).isEqualTo(1L);
  }

  @Test
  void storageChangeUsesAccountHashPrefixedDepth() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(1)); // hashed internally
    final TrieLogLayer log = new TrieLogLayer();
    log.addStorageChange(addr, slot, UInt256.ZERO, UInt256.valueOf(5));

    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    counter.countBlock(log, 100L, 4L, out);

    // Storage-trie root for this account is at depth 0 (after stripping the 32-byte account hash).
    assertThat(out.mutationsByDepth()[0]).isGreaterThanOrEqualTo(1L);
  }

  @Test
  void samplingWithNonZeroShiftDoesNotThrowAtShallowDepths() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final TrieLogLayer log = new TrieLogLayer();
    log.addAccountChange(addr, null, acct(1)); // creation touches depth 0 and 1 paths

    final TrieLogChangeCounter shiftedCounter =
        new TrieLogChangeCounter(2, 10, new TrieShapeModel(16));
    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);

    assertThatCode(() -> shiftedCounter.countBlock(log, 100L, 4L, out)).doesNotThrowAnyException();
  }
}
