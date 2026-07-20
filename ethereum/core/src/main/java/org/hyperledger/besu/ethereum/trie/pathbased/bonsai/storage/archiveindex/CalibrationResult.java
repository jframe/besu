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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Accumulates real (not modeled) per-depth/per-shape FULL and DIFF entry byte sizes observed while
 * replaying mainnet blocks through {@link RecordingTrieNodeStrategy}, and converts the resulting
 * per-depth means into an {@link EntrySizeTable} for {@link HistorySizeEstimate} to consume in
 * place of {@link EntrySizeTable#hoodiDefaults()}.
 *
 * <p>Each depth/shape bucket tracks a running sum and count so that {@link
 * #fullBranchBytesByDepth()} etc. can report the mean size on demand.
 */
public final class CalibrationResult {

  private final int maxDepth;

  private final double[] fullBranchSum;
  private final long[] fullBranchCount;
  private final double[] fullShortSum;
  private final long[] fullShortCount;
  private final double[] diffBranchSum;
  private final long[] diffBranchCount;
  private final double[] diffShortSum;
  private final long[] diffShortCount;
  private final long[] writesByDepth;
  // Real (recorded) node writes per depth, split by owning trie. The storage split, divided by the
  // analytic counter's storage writes over the same replay slice, yields the per-depth correction
  // that cancels the analytic fill-every-depth over-count for the (sparse, compacted) storage trie.
  private final long[] realAccountWritesByDepth;
  private final long[] realStorageWritesByDepth;
  // Analytic (TrieLogChangeCounter) storage writes per depth over the same slice, set by the
  // calibration subcommand after replay. Zero (the default / absent-in-file) disables correction.
  private final long[] analyticStorageWritesByDepth;

  // Ceiling on the storage correction factor, so a depth with near-zero analytic writes (noisy
  // ratio) can't blow up the estimate. Storage is over-counted (factor < 1) at the byte-dominant
  // shallow depths and only mildly under-counted deep, so a modest ceiling loses nothing real.
  private static final double MAX_STORAGE_CORRECTION = 8.0;

  private double keyBytesSum;
  private long keyBytesCount;

  public CalibrationResult() {
    this(ChangeCountResult.MAX_DEPTH);
  }

  CalibrationResult(final int maxDepth) {
    this.maxDepth = maxDepth;
    this.fullBranchSum = new double[maxDepth];
    this.fullBranchCount = new long[maxDepth];
    this.fullShortSum = new double[maxDepth];
    this.fullShortCount = new long[maxDepth];
    this.diffBranchSum = new double[maxDepth];
    this.diffBranchCount = new long[maxDepth];
    this.diffShortSum = new double[maxDepth];
    this.diffShortCount = new long[maxDepth];
    this.writesByDepth = new long[maxDepth];
    this.realAccountWritesByDepth = new long[maxDepth];
    this.realStorageWritesByDepth = new long[maxDepth];
    this.analyticStorageWritesByDepth = new long[maxDepth];
  }

  /**
   * Records a single trie-node write observed during replay.
   *
   * @param depth the nibble-path depth ({@code location.size()}), clamped into the overflow bucket
   *     if it exceeds {@code maxDepth - 1}
   * @param isBranch whether the new node RLP decodes to a 17-item branch (vs 2-item short node)
   * @param fullSize the size in bytes of {@link TrieNodeDiffCodec#encodeFull} applied to the new
   *     node
   * @param diffSize the size in bytes of the DIFF (or FULL-for-creation) encoding for this write
   * @param keySize the size in bytes of the history-CF key for this write ({@code naturalKey ‖
   *     block(8 bytes)})
   */
  void record(
      final int depth,
      final boolean isBranch,
      final int fullSize,
      final int diffSize,
      final int keySize,
      final boolean isAccountPath) {
    final int d = Math.min(depth, maxDepth - 1);
    writesByDepth[d]++;
    if (isAccountPath) {
      realAccountWritesByDepth[d]++;
    } else {
      realStorageWritesByDepth[d]++;
    }
    if (isBranch) {
      fullBranchSum[d] += fullSize;
      fullBranchCount[d]++;
      diffBranchSum[d] += diffSize;
      diffBranchCount[d]++;
    } else {
      fullShortSum[d] += fullSize;
      fullShortCount[d]++;
      diffShortSum[d] += diffSize;
      diffShortCount[d]++;
    }
    keyBytesSum += keySize;
    keyBytesCount++;
  }

  public double[] fullBranchBytesByDepth() {
    return means(fullBranchSum, fullBranchCount);
  }

  public double[] fullShortBytesByDepth() {
    return means(fullShortSum, fullShortCount);
  }

  public double[] diffBranchBytesByDepth() {
    return means(diffBranchSum, diffBranchCount);
  }

  public double[] diffShortBytesByDepth() {
    return means(diffShortSum, diffShortCount);
  }

  public long[] writesByDepth() {
    return writesByDepth;
  }

  public long[] realAccountWritesByDepth() {
    return realAccountWritesByDepth;
  }

  public long[] realStorageWritesByDepth() {
    return realStorageWritesByDepth;
  }

  /**
   * Records the analytic ({@link TrieLogChangeCounter}) per-depth storage-node write counts
   * measured over the same replay slice, against which the recorded (real) storage writes define
   * the correction. Called by the calibration subcommand after the forward replay.
   */
  public void setAnalyticStorageWritesByDepth(final long[] analytic) {
    System.arraycopy(
        analytic, 0, analyticStorageWritesByDepth, 0, Math.min(analytic.length, maxDepth));
  }

  /**
   * Per-depth multiplier that maps the estimator's analytic storage-node write count onto the
   * measured reality: {@code realStorage[d] / analyticStorage[d]}, clamped to {@link
   * #MAX_STORAGE_CORRECTION}. Depths where no analytic storage writes were observed in the slice
   * return {@code 1.0} (no correction) — including every depth when the calibration file predates
   * this field, so old files leave the estimate unchanged.
   */
  public double[] storageCorrectionByDepth() {
    final double[] correction = new double[maxDepth];
    for (int d = 0; d < maxDepth; d++) {
      if (analyticStorageWritesByDepth[d] <= 0) {
        correction[d] = 1.0;
      } else {
        final double ratio =
            (double) realStorageWritesByDepth[d] / (double) analyticStorageWritesByDepth[d];
        correction[d] = Math.min(MAX_STORAGE_CORRECTION, ratio);
      }
    }
    return correction;
  }

  private static double[] means(final double[] sum, final long[] count) {
    final double[] means = new double[sum.length];
    for (int d = 0; d < sum.length; d++) {
      means[d] = count[d] == 0 ? 0.0 : sum[d] / count[d];
    }
    return means;
  }

  /**
   * Converts the accumulated per-depth/shape means into an {@link EntrySizeTable}, using the
   * observed average history-CF key size across all recorded writes.
   */
  public EntrySizeTable toEntrySizeTable() {
    final double keyBytesPerEntry = keyBytesCount == 0 ? 0.0 : keyBytesSum / keyBytesCount;
    return new EntrySizeTable(
        fullBranchBytesByDepth(),
        fullShortBytesByDepth(),
        diffBranchBytesByDepth(),
        diffShortBytesByDepth(),
        keyBytesPerEntry);
  }

  /** Serializes this result to {@code path} as JSON. */
  public void writeTo(final Path path) {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode root = mapper.createObjectNode();
    root.put("maxDepth", maxDepth);
    root.put("keyBytesSum", keyBytesSum);
    root.put("keyBytesCount", keyBytesCount);
    putDepthShapeArrays(mapper, root, "fullBranch", fullBranchSum, fullBranchCount);
    putDepthShapeArrays(mapper, root, "fullShort", fullShortSum, fullShortCount);
    putDepthShapeArrays(mapper, root, "diffBranch", diffBranchSum, diffBranchCount);
    putDepthShapeArrays(mapper, root, "diffShort", diffShortSum, diffShortCount);
    putLongArray(mapper, root, "writesByDepth", writesByDepth);
    putLongArray(mapper, root, "realAccountWritesByDepth", realAccountWritesByDepth);
    putLongArray(mapper, root, "realStorageWritesByDepth", realStorageWritesByDepth);
    putLongArray(mapper, root, "analyticStorageWritesByDepth", analyticStorageWritesByDepth);
    try {
      mapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), root);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Deserializes a {@link CalibrationResult} previously written by {@link #writeTo(Path)}. */
  public static CalibrationResult readFrom(final Path path) {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonNode root;
    try {
      root = mapper.readTree(path.toFile());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    final int maxDepth = root.get("maxDepth").asInt();
    final CalibrationResult result = new CalibrationResult(maxDepth);
    result.keyBytesSum = root.get("keyBytesSum").asDouble();
    result.keyBytesCount = root.get("keyBytesCount").asLong();
    readDepthShapeArrays(root, "fullBranch", result.fullBranchSum, result.fullBranchCount);
    readDepthShapeArrays(root, "fullShort", result.fullShortSum, result.fullShortCount);
    readDepthShapeArrays(root, "diffBranch", result.diffBranchSum, result.diffBranchCount);
    readDepthShapeArrays(root, "diffShort", result.diffShortSum, result.diffShortCount);
    readLongArray(root, "writesByDepth", result.writesByDepth);
    // Fields added with the storage-correction feature; absent in older calibration files, in which
    // case they stay zero and storageCorrectionByDepth() returns all-1.0 (no correction).
    readLongArrayIfPresent(root, "realAccountWritesByDepth", result.realAccountWritesByDepth);
    readLongArrayIfPresent(root, "realStorageWritesByDepth", result.realStorageWritesByDepth);
    readLongArrayIfPresent(
        root, "analyticStorageWritesByDepth", result.analyticStorageWritesByDepth);
    return result;
  }

  private static void readLongArrayIfPresent(
      final JsonNode root, final String name, final long[] values) {
    if (root.has(name)) {
      readLongArray(root, name, values);
    }
  }

  private static void putDepthShapeArrays(
      final ObjectMapper mapper,
      final ObjectNode root,
      final String prefix,
      final double[] sum,
      final long[] count) {
    final ArrayNode sumNode = mapper.createArrayNode();
    for (final double v : sum) {
      sumNode.add(v);
    }
    root.set(prefix + "Sum", sumNode);
    putLongArray(mapper, root, prefix + "Count", count);
  }

  private static void putLongArray(
      final ObjectMapper mapper, final ObjectNode root, final String name, final long[] values) {
    final ArrayNode node = mapper.createArrayNode();
    for (final long v : values) {
      node.add(v);
    }
    root.set(name, node);
  }

  private static void readDepthShapeArrays(
      final JsonNode root, final String prefix, final double[] sum, final long[] count) {
    final JsonNode sumNode = root.get(prefix + "Sum");
    for (int i = 0; i < sum.length; i++) {
      sum[i] = sumNode.get(i).asDouble();
    }
    readLongArray(root, prefix + "Count", count);
  }

  private static void readLongArray(final JsonNode root, final String name, final long[] values) {
    final JsonNode node = root.get(name);
    for (int i = 0; i < values.length; i++) {
      values[i] = node.get(i).asLong();
    }
  }
}
