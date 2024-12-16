package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.datatypes.BlockCreator;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockCreatorFactory;

public class QbftBlockCreatorFactoryImpl implements QbftBlockCreatorFactory {

    private final org.hyperledger.besu.consensus.qbft.blockcreation.QbftBlockCreatorFactory qbftBlockCreatorFactory;
    private final BftExtraDataCodec bftExtraDataCodec;

    public QbftBlockCreatorFactoryImpl(final org.hyperledger.besu.consensus.qbft.blockcreation.QbftBlockCreatorFactory qbftBlockCreatorFactory, final BftExtraDataCodec bftExtraDataCodec) {
        this.qbftBlockCreatorFactory = qbftBlockCreatorFactory;
        this.bftExtraDataCodec = bftExtraDataCodec;
    }

    @Override
    public BlockCreator create(final int roundNumber) {
        return new BlockCreatorImpl(qbftBlockCreatorFactory.create(roundNumber), bftExtraDataCodec);
    }
}
