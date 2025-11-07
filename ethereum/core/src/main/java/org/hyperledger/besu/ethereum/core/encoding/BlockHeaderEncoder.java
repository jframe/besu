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
package org.hyperledger.besu.ethereum.core.encoding;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.PreAllocatedRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPSizeEstimator;

import org.apache.tuweni.bytes.Bytes;

public class BlockHeaderEncoder {
  public Bytes encode(final BlockHeader blockHeader) {
    // Use optimized pre-allocated RLP encoder with size estimation
    final int estimatedSize = estimateBlockHeaderSize(blockHeader);
    final PreAllocatedRLPOutput output = PreAllocatedRLPOutput.get();
    try {
      output.reset(estimatedSize);
      blockHeader.writeTo(output);
      return output.encoded();
    } finally {
      output.returnToPool();
    }
  }

  /**
   * Estimate the RLP-encoded size of a block header.
   *
   * @param blockHeader The block header to estimate
   * @return Estimated size in bytes
   */
  private int estimateBlockHeaderSize(final BlockHeader blockHeader) {
    final int extraDataSize = blockHeader.getExtraData().size();
    final boolean hasWithdrawals = blockHeader.getWithdrawalsRoot().isPresent();
    final boolean hasBlobFields = blockHeader.getExcessBlobGas().isPresent();

    return RLPSizeEstimator.estimateBlockHeaderSize(extraDataSize, hasWithdrawals, hasBlobFields);
  }
}
