/*
 * Copyright Hyperledger Besu Contributors.
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
package org.hyperledger.besu.ethereum.core.encoding;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.transaction.Frame;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * Encodes a FRAME transaction (EIP-8141).
 *
 * <p>Format: {@code 0x06 || rlp([chain_id, nonce, sender, frames_list,
 * max_priority_fee_per_gas, max_fee_per_gas, max_fee_per_blob_gas, blob_versioned_hashes])}
 *
 * <p>Each frame is encoded as: {@code [mode, target_or_empty_bytes, gas_limit, data]}
 */
public class FrameTransactionEncoder {

  private FrameTransactionEncoder() {
    // private constructor
  }

  /**
   * Encodes a FRAME transaction into the provided RLP output.
   *
   * @param transaction the transaction to encode (must be of type FRAME)
   * @param out the RLP output stream
   */
  public static void encode(final Transaction transaction, final RLPOutput out) {
    out.startList();
    out.writeBigIntegerScalar(transaction.getChainId().orElseThrow());
    out.writeLongScalar(transaction.getNonce());
    out.writeBytes(transaction.getFrameSender().orElseThrow());
    out.writeList(
        transaction.getFrames().orElseThrow(),
        (frame, frameOut) -> writeFrame(frame, frameOut));
    out.writeUInt256Scalar(transaction.getMaxPriorityFeePerGas().orElseThrow());
    out.writeUInt256Scalar(transaction.getMaxFeePerGas().orElseThrow());
    out.writeUInt256Scalar(transaction.getMaxFeePerBlobGas().orElse(Wei.ZERO));
    BlobTransactionEncoder.writeBlobVersionedHashes(
        out, transaction.getVersionedHashes().orElse(List.of()));
    out.endList();
  }

  private static void writeFrame(final Frame frame, final RLPOutput out) {
    out.startList();
    out.writeIntScalar(Byte.toUnsignedInt(frame.getMode()));
    out.writeBytes(frame.getTarget().map(Bytes::copy).orElse(Bytes.EMPTY));
    out.writeLongScalar(frame.getGasLimit());
    out.writeBytes(frame.getData());
    out.endList();
  }
}
