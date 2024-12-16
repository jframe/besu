package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.BftBlockHashing;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.qbft.core.datatypes.BlockHashing;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.datatypes.Hash;

public class BlockHashingImpl implements BlockHashing {
    private final BftBlockHashing bftBlockHashing;

    public BlockHashingImpl(final BftBlockHashing bftBlockHashing) {
        this.bftBlockHashing = bftBlockHashing;
    }

    @Override
    public Hash calculateDataHashForCommittedSeal(final QbftBlockHeader header, final BftExtraData extraData) {
        return bftBlockHashing.calculateDataHashForCommittedSeal(BlockWrapper.toBesuBlockHeader(header), extraData);
    }
}
