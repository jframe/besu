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
package org.hyperledger.besu.cli.subcommands.storage.archivestats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.List;

import org.junit.jupiter.api.Test;

class ClassBinnerTest {

  private static final List<String> ACCOUNT_LABELS =
      List.of("dormant", "active", "long-lived", "hot", "mega-hot");
  private static final List<Long> ACCOUNT_BOUNDARIES = List.of(3L, 50L, 10_000L, 1_000_000L);

  @Test
  void valueAtBoundaryLandsInLowerClass() {
    final ClassBinner b = new ClassBinner(ACCOUNT_BOUNDARIES, ACCOUNT_LABELS);
    b.record(3L); // dormant (1-3)
    b.record(50L); // active (4-50)
    b.record(10_000L); // long-lived
    b.record(1_000_000L); // hot
    final var bins = b.snapshot();
    assertThat(bins).extracting(ClassBinner.Bin::label).containsExactlyElementsOf(ACCOUNT_LABELS);
    assertThat(bins).extracting(ClassBinner.Bin::count).containsExactly(1L, 1L, 1L, 1L, 0L);
  }

  @Test
  void valueAboveLastBoundaryLandsInLastClass() {
    final ClassBinner b = new ClassBinner(ACCOUNT_BOUNDARIES, ACCOUNT_LABELS);
    b.record(1_000_001L);
    b.record(99_999_999L);
    assertThat(b.snapshot().get(4).count()).isEqualTo(2L);
  }

  @Test
  void emptyInputProducesAllZeroCounts() {
    final ClassBinner b = new ClassBinner(ACCOUNT_BOUNDARIES, ACCOUNT_LABELS);
    final var bins = b.snapshot();
    assertThat(bins).hasSize(5);
    assertThat(bins).allMatch(bin -> bin.count() == 0L);
    assertThat(bins).allMatch(bin -> bin.percentage() == 0.0);
  }

  @Test
  void percentagesSumToOneHundredWithinRoundingTolerance() {
    final ClassBinner b = new ClassBinner(ACCOUNT_BOUNDARIES, ACCOUNT_LABELS);
    for (int i = 0; i < 73; i++) {
      b.record(2L); // dormant
    }
    for (int i = 0; i < 27; i++) {
      b.record(40L); // active
    }
    final double sum = b.snapshot().stream().mapToDouble(ClassBinner.Bin::percentage).sum();
    assertThat(sum).isCloseTo(100.0, org.assertj.core.data.Offset.offset(0.01));
  }

  @Test
  void rangeMods_format_matchesSpecExample() {
    final ClassBinner b = new ClassBinner(ACCOUNT_BOUNDARIES, ACCOUNT_LABELS);
    final var bins = b.snapshot();
    assertThat(bins.get(0).rangeMods()).isEqualTo("1–3"); // dormant: "1–3" with en-dash
    assertThat(bins.get(1).rangeMods()).isEqualTo("4–50");
    assertThat(bins.get(2).rangeMods()).isEqualTo("51–10000");
    assertThat(bins.get(3).rangeMods()).isEqualTo("10001–1000000");
    assertThat(bins.get(4).rangeMods()).isEqualTo("1000001+");
  }

  @Test
  void rejectsMismatchedLabelCount() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ClassBinner(ACCOUNT_BOUNDARIES, List.of("a", "b", "c")));
  }

  @Test
  void rejectsNonAscendingBoundaries() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ClassBinner(List.of(10L, 5L, 100L, 1000L), ACCOUNT_LABELS));
  }
}
