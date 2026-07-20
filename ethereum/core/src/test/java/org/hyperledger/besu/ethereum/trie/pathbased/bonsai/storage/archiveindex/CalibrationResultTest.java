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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.file.Path;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CalibrationResultTest {

  @TempDir Path tempDir;

  private RecordingTrieNodeStrategy recorderWithTwoWritesAtDepth2() {
    final TrieNodeStrategy delegate = mock(TrieNodeStrategy.class);
    final SegmentedKeyValueStorage storage = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorageTransaction tx = mock(SegmentedKeyValueStorageTransaction.class);
    when(storage.get(any(), any())).thenReturn(Optional.empty());

    final RecordingTrieNodeStrategy rec = new RecordingTrieNodeStrategy(delegate);
    final Bytes location = Bytes.of(0x01, 0x02);
    // Two distinct short-node creations at the same depth: fullSize differs, so the mean is
    // meaningfully between the two individual sizes (not equal to either).
    rec.putFlatAccountTrieNode(storage, tx, location, Bytes32.ZERO, shortNode(Bytes.of(0x01)));
    rec.putFlatAccountTrieNode(
        storage, tx, location, Bytes32.ZERO, shortNode(Bytes.of(0x01, 0x02, 0x03, 0x04, 0x05)));
    return rec;
  }

  private static Bytes shortNode(final Bytes value) {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(Bytes.of(0x12));
          out.writeBytes(value);
          out.endList();
        });
  }

  @Test
  void meanAccumulatesAcrossMultipleWritesAtSameDepthAndShape() {
    final CalibrationResult result = recorderWithTwoWritesAtDepth2().result();

    // encodeFull sizes: (1 metadata + 3-byte node) = 4 and (1 metadata + 8-byte node) = 9 ->
    // mean 6.5. Not just a sum (13) or either individual value.
    assertThat(result.fullShortBytesByDepth()[2]).isEqualTo(6.5);
    assertThat(result.writesByDepth()[2]).isEqualTo(2L);
  }

  @Test
  void toEntrySizeTableProducesSaneFullAndDiffBytes() {
    final CalibrationResult result = recorderWithTwoWritesAtDepth2().result();
    final EntrySizeTable table = result.toEntrySizeTable();

    assertThat(table.fullBytes(2, 0.0)).isEqualTo(6.5); // branchFraction=0 -> pure short bytes
    assertThat(table.keyBytes()).isGreaterThan(0.0);
    // Untouched depth falls back to zero rather than throwing.
    assertThat(table.fullBytes(10, 0.0)).isEqualTo(0.0);
  }

  @Test
  void writeToAndReadFromRoundTripsAllFields() {
    final CalibrationResult original = recorderWithTwoWritesAtDepth2().result();
    final Path file = tempDir.resolve("calibration.json");

    original.writeTo(file);
    final CalibrationResult restored = CalibrationResult.readFrom(file);

    assertThat(restored.fullShortBytesByDepth()).isEqualTo(original.fullShortBytesByDepth());
    assertThat(restored.fullBranchBytesByDepth()).isEqualTo(original.fullBranchBytesByDepth());
    assertThat(restored.diffShortBytesByDepth()).isEqualTo(original.diffShortBytesByDepth());
    assertThat(restored.diffBranchBytesByDepth()).isEqualTo(original.diffBranchBytesByDepth());
    assertThat(restored.writesByDepth()).isEqualTo(original.writesByDepth());
    assertThat(restored.toEntrySizeTable().keyBytes())
        .isEqualTo(original.toEntrySizeTable().keyBytes());
  }

  @Test
  void storageCorrectionIsRealOverAnalyticClampedAndDefaultsToOne() {
    final CalibrationResult result = new CalibrationResult();
    // Real (recorded) storage writes: depth 2 heavily over-counted by the analytic model.
    result.record(2, false, 40, 40, 40, false); // storage write at depth 2
    result.record(2, false, 40, 40, 40, false);
    result.record(2, false, 40, 40, 40, false); // 3 real storage writes at depth 2
    result.record(5, false, 40, 40, 40, false); // 1 real storage write at depth 5
    final long[] analytic = new long[ChangeCountResult.MAX_DEPTH];
    analytic[2] = 12; // analytic over-counted depth 2 4x -> correction 0.25
    analytic[5] = 1; // analytic matched depth 5 -> correction 1.0
    result.setAnalyticStorageWritesByDepth(analytic);

    final double[] correction = result.storageCorrectionByDepth();
    assertThat(correction[2]).isEqualTo(0.25); // 3 real / 12 analytic
    assertThat(correction[5]).isEqualTo(1.0); // 1 / 1
    assertThat(correction[3]).isEqualTo(1.0); // no analytic writes -> no correction
  }

  @Test
  void storageCorrectionSurvivesRoundTripAndOldFilesDefaultToOne() {
    final CalibrationResult original = new CalibrationResult();
    original.record(2, false, 40, 40, 40, false);
    original.record(2, false, 40, 40, 40, false);
    final long[] analytic = new long[ChangeCountResult.MAX_DEPTH];
    analytic[2] = 8; // correction 2/8 = 0.25
    original.setAnalyticStorageWritesByDepth(analytic);
    final Path file = tempDir.resolve("calibration-correction.json");
    original.writeTo(file);

    final CalibrationResult restored = CalibrationResult.readFrom(file);
    assertThat(restored.storageCorrectionByDepth()[2]).isEqualTo(0.25);

    // A calibration file without the correction fields (the pre-feature format) yields all-1.0.
    final CalibrationResult legacy = recorderWithTwoWritesAtDepth2().result();
    final Path legacyFile = tempDir.resolve("legacy.json");
    legacy.writeTo(legacyFile);
    stripFields(
        legacyFile,
        "realAccountWritesByDepth",
        "realStorageWritesByDepth",
        "analyticStorageWritesByDepth");
    final double[] legacyCorrection =
        CalibrationResult.readFrom(legacyFile).storageCorrectionByDepth();
    for (final double c : legacyCorrection) {
      assertThat(c).isEqualTo(1.0);
    }
  }

  private static void stripFields(final Path file, final String... fields) {
    try {
      final com.fasterxml.jackson.databind.ObjectMapper mapper =
          new com.fasterxml.jackson.databind.ObjectMapper();
      final com.fasterxml.jackson.databind.node.ObjectNode root =
          (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(file.toFile());
      for (final String f : fields) {
        root.remove(f);
      }
      mapper.writeValue(file.toFile(), root);
    } catch (final java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
