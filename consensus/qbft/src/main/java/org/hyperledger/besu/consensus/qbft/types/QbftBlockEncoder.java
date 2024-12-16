package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.datatypes.BlockEncoder;
import org.hyperledger.besu.consensus.qbft.core.datatypes.HashMode;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlock;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import static org.hyperledger.besu.consensus.qbft.types.BlockHeaderFunctionsUtil.getBlockHeaderFunctions;

public class QbftBlockEncoder implements BlockEncoder {

    private final QbftExtraDataCodec qbftExtraDataCodec;

    public QbftBlockEncoder(final QbftExtraDataCodec qbftExtraDataCodec) {
        this.qbftExtraDataCodec = qbftExtraDataCodec;
    }

    @Override
    public QbftBlock readFrom(final RLPInput rlpInput, final HashMode hashMode) {
        final BlockHeaderFunctions blockHeaderFunctions = getBlockHeaderFunctions(qbftExtraDataCodec, hashMode);
        return new QbftBlockImpl(Block.readFrom(rlpInput, blockHeaderFunctions));
    }

    @Override
    public void writeTo(final QbftBlock block, final RLPOutput rlpOutput) {
       BlockUtil.toBesuBlock(block).writeTo(rlpOutput);
    }
}
