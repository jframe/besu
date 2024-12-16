package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;

public class QbftBlockWrapper extends Block implements QbftBlock {

    public QbftBlockWrapper(final QbftBlockHeaderImpl header, final BlockBody body) {
        super(header, body);
    }

    @Override
    public boolean isEmpty() {
        return getBody().isEmpty();
    }

    @Override
    public QbftBlockHeader getQbftBlockHeader() {
        return (QbftBlockHeader) getHeader();
    }
}
