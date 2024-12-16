package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;

public class QbftBlockImpl extends Block implements QbftBlock {

    public QbftBlockImpl(final BlockHeader header, final BlockBody body) {
        super(header, body);
    }

    @Override
    public boolean isEmpty() {
        return getBody().isEmpty();
    }

    @Override
    public QbftBlockHeader getQbftBlockHeader() {
        return new QbftBlockHeaderImpl(getHeader(), getHeader().getBlockHeaderFunctions());
    }
}
