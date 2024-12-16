package org.hyperledger.besu.consensus.qbft.types;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt64;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.evm.log.LogsBloomFilter;

public class QbftBlockHeaderImpl extends BlockHeader implements QbftBlockHeader {

    public QbftBlockHeaderImpl(final BlockHeader blockHeader, final BlockHeaderFunctions blockHeaderFunctions) {
        super(blockHeader.getParentHash(),
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
                blockHeader.getTargetBlobCount().orElse(null), blockHeaderFunctions);
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
