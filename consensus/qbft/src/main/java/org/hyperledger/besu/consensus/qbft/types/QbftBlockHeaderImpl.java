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

import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.evm.log.LogsBloomFilter;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt64;

public class QbftBlockHeaderImpl extends BlockHeader implements QbftBlockHeader {

  public QbftBlockHeaderImpl(
      final BlockHeader blockHeader, final BlockHeaderFunctions blockHeaderFunctions) {
    super(
        blockHeader.getParentHash(),
        blockHeader.getOmmersHash(),
        blockHeader.getCoinbase(),
        blockHeader.getStateRoot(),
        blockHeader.getTransactionsRoot(),
        blockHeader.getReceiptsRoot(),
        blockHeader.getLogsBloom(),
        blockHeader.getDifficulty(),
        blockHeader.getNumber(),
        blockHeader.getGasLimit(),
        blockHeader.getGasUsed(),
        blockHeader.getTimestamp(),
        blockHeader.getExtraData(),
        blockHeader.getBaseFee().orElse(null),
        blockHeader.getMixHashOrPrevRandao(),
        blockHeader.getNonce(),
        blockHeader.getWithdrawalsRoot().orElse(null),
        blockHeader.getGasUsed(),
        blockHeader.getExcessBlobGas().orElse(null),
        blockHeader.getParentBeaconBlockRoot().orElse(null),
        blockHeader.getRequestsHash().orElse(null),
        blockHeader.getTargetBlobsPerBlock().orElse(null),
        blockHeaderFunctions);
  }

  public QbftBlockHeaderImpl(final BlockHeader blockHeader) {
    super(
        blockHeader.getParentHash(),
        blockHeader.getOmmersHash(),
        blockHeader.getCoinbase(),
        blockHeader.getStateRoot(),
        blockHeader.getTransactionsRoot(),
        blockHeader.getReceiptsRoot(),
        blockHeader.getLogsBloom(),
        blockHeader.getDifficulty(),
        blockHeader.getNumber(),
        blockHeader.getGasLimit(),
        blockHeader.getGasUsed(),
        blockHeader.getTimestamp(),
        blockHeader.getExtraData(),
        blockHeader.getBaseFee().orElse(null),
        blockHeader.getMixHashOrPrevRandao(),
        blockHeader.getNonce(),
        blockHeader.getWithdrawalsRoot().orElse(null),
        blockHeader.getGasUsed(),
        blockHeader.getExcessBlobGas().orElse(null),
        blockHeader.getParentBeaconBlockRoot().orElse(null),
        blockHeader.getRequestsHash().orElse(null),
        blockHeader.getTargetBlobsPerBlock().orElse(null),
        blockHeader.getBlockHeaderFunctions());
  }

  public QbftBlockHeaderImpl(
      final Hash parentHash,
      final Hash ommersHash,
      final Address coinbase,
      final Hash stateRoot,
      final Hash transactionsRoot,
      final Hash receiptsRoot,
      final LogsBloomFilter logsBloom,
      final Difficulty difficulty,
      final long number,
      final long gasLimit,
      final long gasUsed,
      final long timestamp,
      final Bytes extraData,
      final Wei baseFee,
      final Bytes32 mixHashOrPrevRandao,
      final long nonce,
      final Hash withdrawalsRoot,
      final Long blobGasUsed,
      final BlobGas excessBlobGas,
      final Bytes32 parentBeaconBlockRoot,
      final Hash requestsHash,
      final UInt64 targetBlobCount,
      final BlockHeaderFunctions blockHeaderFunctions) {
    super(
        parentHash,
        ommersHash,
        coinbase,
        stateRoot,
        transactionsRoot,
        receiptsRoot,
        logsBloom,
        difficulty,
        number,
        gasLimit,
        gasUsed,
        timestamp,
        extraData,
        baseFee,
        mixHashOrPrevRandao,
        nonce,
        withdrawalsRoot,
        blobGasUsed,
        excessBlobGas,
        parentBeaconBlockRoot,
        requestsHash,
        targetBlobCount,
        blockHeaderFunctions);
  }
}
