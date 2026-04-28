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
 * One {@code (prefix, rangeId)} group's modification count.
 *
 * @param prefix natural key (account hash, or account hash + slot hash). Defensively copied by the
 *     emitter; consumers must not mutate.
 * @param rangeId block-range index = blockNumber / rangeSize.
 * @param count number of archive entries observed for this {@code (prefix, rangeId)}. Always &ge;
 *     1.
 */
public record RowRecord(byte[] prefix, long rangeId, int count) {}
