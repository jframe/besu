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
package org.hyperledger.besu.datatypes;

import java.util.List;

/**
 * Per-frame execution summary for EIP-8141 FRAME transactions, embedded in the transaction
 * receipt.
 *
 * @param status 1 if the frame completed successfully, 0 otherwise
 * @param gasUsed gas consumed by this frame
 * @param logs logs emitted by this frame (empty for VERIFY frames whose state was reverted)
 */
public record FrameReceipt(int status, long gasUsed, List<Log> logs) {}
