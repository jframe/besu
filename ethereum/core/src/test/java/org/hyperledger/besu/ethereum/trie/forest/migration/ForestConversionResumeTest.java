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

import org.hyperledger.besu.datatypes.Hash;

import java.util.Set;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

class ForestConversionResumeTest {

  // Each block n maps to a distinct synthetic state root derived from n.
  private static Hash rootByBlock(final long n) {
    return Hash.hash(org.apache.tuweni.bytes.Bytes.ofUnsignedLong(n));
  }

  private static Predicate<Hash> presentForBlocks(final long highestPresent) {
    final Set<Hash> present = new java.util.HashSet<>();
    for (long n = 0; n <= highestPresent; n++) {
      present.add(rootByBlock(n));
    }
    return present::contains;
  }

  @Test
  void resumesAtHighestPresentBlock() {
    assertThat(
            ForestConversionResume.findResumeBlock(
                10, ForestConversionResumeTest::rootByBlock, presentForBlocks(5)))
        .isEqualTo(5L);
  }

  @Test
  void resumesAtGenesisWhenOnlyGenesisPresent() {
    assertThat(
            ForestConversionResume.findResumeBlock(
                10, ForestConversionResumeTest::rootByBlock, presentForBlocks(0)))
        .isEqualTo(0L);
  }

  @Test
  void resumesAtHeadWhenAllPresent() {
    assertThat(
            ForestConversionResume.findResumeBlock(
                10, ForestConversionResumeTest::rootByBlock, presentForBlocks(10)))
        .isEqualTo(10L);
  }

  @Test
  void headZeroReturnsZero() {
    assertThat(
            ForestConversionResume.findResumeBlock(
                0, ForestConversionResumeTest::rootByBlock, presentForBlocks(0)))
        .isEqualTo(0L);
  }
}
