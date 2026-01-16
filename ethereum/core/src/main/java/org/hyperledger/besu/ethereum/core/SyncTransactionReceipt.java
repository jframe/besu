/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.core;

import org.apache.tuweni.bytes.Bytes;

/**
 * A memory-efficient transaction receipt representation for sync operations.
 *
 * <p>This class stores only the raw RLP-encoded bytes of the receipt, avoiding the memory overhead
 * of parsed objects (Log, LogTopic, Address, etc.). Fields are parsed lazily on demand.
 *
 * <p>Memory comparison (typical receipt with 2-3 logs):
 *
 * <ul>
 *   <li>TransactionReceipt (fully parsed): ~2,200 bytes
 *   <li>SyncTransactionReceipt (lazy): ~500 bytes
 * </ul>
 */
public class SyncTransactionReceipt {

  private final Bytes rlpBytes;

  /**
   * Creates a new SyncTransactionReceipt from raw RLP-encoded bytes.
   *
   * @param rlpBytes the RLP-encoded receipt bytes
   */
  public SyncTransactionReceipt(final Bytes rlpBytes) {
    this.rlpBytes = rlpBytes;
  }

  /**
   * Returns the raw RLP-encoded bytes of this receipt.
   *
   * @return the RLP-encoded bytes
   */
  public Bytes getRlpBytes() {
    return rlpBytes;
  }
}
