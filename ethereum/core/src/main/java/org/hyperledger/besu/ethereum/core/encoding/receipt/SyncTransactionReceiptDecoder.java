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
package org.hyperledger.besu.ethereum.core.encoding.receipt;

import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;

import org.apache.tuweni.bytes.Bytes;

/**
 * Decoder for creating SyncTransactionReceipt instances from raw RLP bytes.
 *
 * <p>This decoder does not parse the receipt contents - it simply wraps the raw bytes. Parsing is
 * deferred to when the data is actually needed (e.g., for receipts root calculation).
 */
public class SyncTransactionReceiptDecoder {

  /**
   * Creates a SyncTransactionReceipt from raw RLP-encoded bytes.
   *
   * <p>No parsing is performed - the bytes are simply wrapped for later use.
   *
   * @param rawRlp the raw RLP-encoded receipt bytes
   * @return a new SyncTransactionReceipt containing the raw bytes
   */
  public SyncTransactionReceipt decode(final Bytes rawRlp) {
    return new SyncTransactionReceipt(rawRlp);
  }
}
