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

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Top-level scan result handed to {@link ReportWriter}.
 *
 * @param dataDir filesystem path to the scanned RocksDB
 * @param chainHead highest block number observed during the scan
 * @param rangeSize block-range partition size used at scan time
 * @param totalRanges (chainHead / rangeSize) + 1
 * @param scanStart wall-clock start of scan
 * @param scanEnd wall-clock end of scan
 * @param rocksDbCfSizeBytes per-CF disk-size estimate
 * @param fpSweepGrid grid of (k, m) bloom sizing points evaluated
 * @param cfResults per-CF aggregated stats
 * @param slotFanOutResults per-CF slot-fan-out stats; only the storage CF gets an entry. Empty when
 *     storage CF was not selected.
 */
public record ScanResult(
    String dataDir,
    long chainHead,
    long rangeSize,
    long totalRanges,
    Instant scanStart,
    Instant scanEnd,
    Map<ArchiveCf, Long> rocksDbCfSizeBytes,
    List<FpRateProjector.GridPoint> fpSweepGrid,
    EnumMap<ArchiveCf, CfResult> cfResults,
    EnumMap<ArchiveCf, SlotFanOutResult> slotFanOutResults) {}
