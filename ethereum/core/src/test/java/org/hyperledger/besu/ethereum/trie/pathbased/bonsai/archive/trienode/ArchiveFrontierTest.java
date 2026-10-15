/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveFrontierTest {

  private static final long MAX_LAYERS = 512L;
  private Blockchain blockchain;

  @BeforeEach
  void setUp() {
    blockchain = mock(Blockchain.class);
    when(blockchain.getFinalized()).thenReturn(Optional.empty());
  }

  private void finalizedAt(final long number, final boolean headerLocal) {
    final BlockHeader header = new BlockHeaderTestFixture().number(number).buildHeader();
    when(blockchain.getFinalized()).thenReturn(Optional.of(header.getHash()));
    when(blockchain.getBlockHeader(header.getHash()))
        .thenReturn(headerLocal ? Optional.of(header) : Optional.empty());
  }

  @Test
  void usesFinalizedWhenAheadOfDistance() {
    finalizedAt(9_936L, true);
    assertThat(new ArchiveFrontier(blockchain, () -> 10_000L, MAX_LAYERS).getAsLong())
        .isEqualTo(9_936L);
  }

  @Test
  void fallsBackToHalfWindowDistanceWithoutFinality() {
    assertThat(new ArchiveFrontier(blockchain, () -> 10_000L, MAX_LAYERS).getAsLong())
        .isEqualTo(10_000L - 256L);
  }

  @Test
  void ignoresFinalizedWhoseHeaderIsNotLocal() {
    finalizedAt(9_990L, false);
    assertThat(new ArchiveFrontier(blockchain, () -> 10_000L, MAX_LAYERS).getAsLong())
        .isEqualTo(10_000L - 256L);
  }

  @Test
  void staleFinalizedDoesNotHoldBackDistance() {
    // restart mid-sync: a finalized block stored long ago must not stall inline capture
    finalizedAt(100L, true);
    assertThat(new ArchiveFrontier(blockchain, () -> 50_000L, MAX_LAYERS).getAsLong())
        .isEqualTo(50_000L - 256L);
  }

  @Test
  void neverNegative() {
    assertThat(new ArchiveFrontier(blockchain, () -> 10L, MAX_LAYERS).getAsLong()).isZero();
  }

  @Test
  void readsBestChainHeightEachCall() {
    final long[] best = {1_000L};
    final ArchiveFrontier frontier = new ArchiveFrontier(blockchain, () -> best[0], MAX_LAYERS);
    assertThat(frontier.getAsLong()).isEqualTo(744L);
    best[0] = 2_000L;
    assertThat(frontier.getAsLong()).isEqualTo(1_744L);
  }
}
