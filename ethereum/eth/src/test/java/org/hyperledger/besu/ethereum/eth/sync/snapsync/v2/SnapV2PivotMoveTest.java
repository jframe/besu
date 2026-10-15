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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class SnapV2PivotMoveTest {

  private static BlockHeader header(final long number, final int fork) {
    return new BlockHeaderTestFixture().number(number).extraData(Bytes.of(fork)).buildHeader();
  }

  @Test
  void aNormalStepMovesFromTheAncestor() {
    final BlockHeader pivot = header(10, 0);
    final SnapV2PivotMove move =
        new SnapV2PivotMove(pivot, List.of(), List.of(header(11, 0), header(12, 0)));

    assertThat(move.isReorg()).isFalse();
    assertThat(move.oldPivot()).isEqualTo(pivot);
    assertThat(move.newPivot().getNumber()).isEqualTo(12);
  }

  @Test
  void aReorgMovesFromTheOrphanedTip() {
    final BlockHeader orphanedTip = header(11, 1);
    final SnapV2PivotMove move =
        new SnapV2PivotMove(header(10, 0), List.of(orphanedTip), List.of(header(11, 2)));

    assertThat(move.isReorg()).isTrue();
    assertThat(move.oldPivot()).isEqualTo(orphanedTip);
  }

  @Test
  void needsAtLeastOneCanonicalHeader() {
    assertThatThrownBy(() -> new SnapV2PivotMove(header(10, 0), List.of(), List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
