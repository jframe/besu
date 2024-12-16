package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.datatypes.ExtraDataProvider;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;

public class QbftExtraDataProviderImpl implements ExtraDataProvider {
    private final BftExtraDataCodec bftExtraDataCodec;

    public QbftExtraDataProviderImpl(final BftExtraDataCodec bftExtraDataCodec) {
        this.bftExtraDataCodec = bftExtraDataCodec;
    }

    @Override
    public BftExtraData getExtraData(final QbftBlockHeader header) {
        return bftExtraDataCodec.decode(BlockWrapper.toBesuBlockHeader(header));
    }
}
