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
 * Aggregate stats for a single key (account, or account+slot) across all ranges it appears in.
 *
 * <p>Invariant: {@code totalModifications == sum(RowRecord.count for that prefix)}. The aggregator
 * guarantees this; consumers may rely on it.
 *
 * @param prefix natural key. Defensively copied by the emitter.
 * @param distinctRanges number of distinct ranges this key was modified in.
 * @param totalModifications total entries observed for this key across all ranges.
 */
public record KeyRecord(byte[] prefix, int distinctRanges, long totalModifications) {}
