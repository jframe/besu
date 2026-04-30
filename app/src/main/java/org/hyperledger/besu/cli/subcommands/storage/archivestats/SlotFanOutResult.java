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

/**
 * Aggregated slot-fan-out stats for the storage CF.
 *
 * <p>Each observation is the count of distinct slot hashes touched for one {@code
 * (accountHash, rangeId)} pair. The histogram is over those observations.
 *
 * @param histogram log-bucketed histogram of distinct-slots per (account, range)
 * @param totalAccountRangePairs total number of (account, range) pairs observed (== sum of
 *     histogram counts)
 */
public record SlotFanOutResult(HistogramCollector histogram, long totalAccountRangePairs) {}
