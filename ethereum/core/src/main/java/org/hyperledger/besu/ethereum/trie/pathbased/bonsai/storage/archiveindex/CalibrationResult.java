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
      final int keySize) {
    final int d = Math.min(depth, maxDepth - 1);
    writesByDepth[d]++;
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
    return result;
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
