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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SnapV2BlockAccessListApplierForestTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  static Stream<StateHarness> harnesses() {
    return Stream.of(new BonsaiStateHarness(), new ForestStateHarness());
  }

  private static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("harnesses")
  void nonceOnlyChangeRetainsExistingBalance(final StateHarness h) {
    h.seedAccount(ALICE, 0L, Wei.of(500), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2 =
        b.appendCanonical(block1.getHeader(), b.balWithNonces(Map.of(ALICE, 7L)), 2L);

    final Bytes32 newRoot =
        new SnapV2BlockAccessListApplier(
                h.coordinator(), b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule())
            .applyBlockAccessLists(
                block1.getHeader().getNumber() + 1,
                block2.getHeader().getNumber(),
                h.forestStartRoot(),
                fullAccountRange(),
                new DownloadedStorageRangeTracker())
            .commit();
    h.updateAccountRoot(newRoot);

    assertThat(h.readAccount(ALICE)).isPresent();
    assertThat(h.readAccount(ALICE).get().getNonce()).isEqualTo(7L);
    assertThat(h.readAccount(ALICE).get().getBalance()).isEqualTo(Wei.of(500));
  }
}
