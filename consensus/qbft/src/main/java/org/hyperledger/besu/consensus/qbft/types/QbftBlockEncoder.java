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
package org.hyperledger.besu.consensus.qbft.types;

import static org.hyperledger.besu.consensus.qbft.types.BlockHeaderFunctionsUtil.getBlockHeaderFunctions;

import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.datatypes.BlockEncoder;
import org.hyperledger.besu.consensus.qbft.core.datatypes.HashMode;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlock;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

public class QbftBlockEncoder implements BlockEncoder {

  private final QbftExtraDataCodec qbftExtraDataCodec;

  public QbftBlockEncoder(final QbftExtraDataCodec qbftExtraDataCodec) {
    this.qbftExtraDataCodec = qbftExtraDataCodec;
  }

  @Override
  public QbftBlock readFrom(final RLPInput rlpInput, final HashMode hashMode) {
    final BlockHeaderFunctions blockHeaderFunctions =
        getBlockHeaderFunctions(qbftExtraDataCodec, hashMode);
    return new QbftBlockImpl(Block.readFrom(rlpInput, blockHeaderFunctions));
  }

  @Override
  public void writeTo(final QbftBlock block, final RLPOutput rlpOutput) {
    BlockUtil.toBesuBlock(block).writeTo(rlpOutput);
  }
}
