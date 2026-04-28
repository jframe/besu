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

import java.util.List;

/**
 * Aggregated post-scan stats for one CF.
 *
 * @param totalEntries total archive entries observed
 * @param totalUniqueKeys exact count of distinct prefixes observed in this CF (one per KeyRecord
 *     emitted by the StreamingAggregator)
 * @param totalRows total (key, range) rows emitted
 * @param rowsPerKey histogram of distinct ranges per key
 * @param entriesPerRow histogram of entries per (key, range) row
 * @param modificationsPerKey histogram of total modifications per key
 * @param classBins per-class counts and percentages
 * @param classBoundaries the boundaries used to compute classBins
 * @param rangeStats per-range entry counts and HLL cardinalities
 * @param fpResult bloom FP-rate projections and summaries
 */
public record CfResult(
    long totalEntries,
    long totalUniqueKeys,
    long totalRows,
    HistogramCollector rowsPerKey,
    HistogramCollector entriesPerRow,
    HistogramCollector modificationsPerKey,
    List<ClassBinner.Bin> classBins,
    List<Long> classBoundaries,
    RangeStatsCollector rangeStats,
    FpRateProjector.Result fpResult) {}
