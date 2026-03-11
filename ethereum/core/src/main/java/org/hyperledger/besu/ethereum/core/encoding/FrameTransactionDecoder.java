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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.transaction.Frame;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Decodes a FRAME transaction (EIP-8141).
 *
 * <p>Format: {@code 0x06 || rlp([chain_id, nonce, sender, frames_list,
 * max_priority_fee_per_gas, max_fee_per_gas, max_fee_per_blob_gas, blob_versioned_hashes])}
 *
 * <p>Each frame is encoded as: {@code [mode, target_or_empty_bytes, gas_limit, data]}
 */
public class FrameTransactionDecoder {

  private FrameTransactionDecoder() {
    // private constructor
  }

  /**
   * Decodes a FRAME transaction from raw typed bytes (including the type prefix byte).
   *
   * @param input the raw bytes (0x06 || rlp(...))
   * @return the decoded transaction
   */
  public static Transaction decode(final Bytes input) {
    return decode(RLP.input(input.slice(1)));
  }

  /**
   * Decodes a FRAME transaction from the provided RLP input.
   *
   * @param input the RLP input (positioned after the type byte)
   * @return the decoded transaction
   */
  public static Transaction decode(final RLPInput input) {
    input.enterList();

    final Transaction.Builder builder =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(input.readBigIntegerScalar())
            .nonce(input.readLongScalar())
            .frameSender(Address.wrap(input.readBytes()))
            .frames(readFrames(input))
            .maxPriorityFeePerGas(Wei.of(input.readUInt256Scalar()))
            .maxFeePerGas(Wei.of(input.readUInt256Scalar()))
            .maxFeePerBlobGas(Wei.of(input.readUInt256Scalar()))
            .versionedHashes(
                input.readList(
                    versionedHashes -> new VersionedHash(versionedHashes.readBytes32())));

    // No signature fields for FRAME transactions — sender is explicit.
    input.leaveList();
    return builder.build();
  }

  private static List<Frame> readFrames(final RLPInput input) {
    return input.readList(
        frameInput -> {
          frameInput.enterList();
          final byte mode = (byte) frameInput.readUnsignedByteScalar();
          final org.apache.tuweni.bytes.Bytes targetBytes = frameInput.readBytes();
          final Optional<Address> target =
              targetBytes.isEmpty() ? Optional.empty() : Optional.of(Address.wrap(targetBytes));
          final long gasLimit = frameInput.readLongScalar();
          final org.apache.tuweni.bytes.Bytes data = frameInput.readBytes();
          frameInput.leaveList();
          return new Frame(mode, target, gasLimit, data);
        });
  }
}
