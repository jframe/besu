package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.datatypes.BlockCreator;
import org.hyperledger.besu.consensus.qbft.core.datatypes.ExtraDataProvider;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;

import java.util.Collection;

public class BlockCreatorImpl implements BlockCreator {

    private final org.hyperledger.besu.ethereum.blockcreation.BlockCreator besuBlockCreator;
    private final BftExtraDataCodec bftExtraDataCodec;

    public BlockCreatorImpl(final org.hyperledger.besu.ethereum.blockcreation.BlockCreator besuBftBlockCreator, final BftExtraDataCodec bftExtraDataCodec) {
        this.besuBlockCreator = besuBftBlockCreator;
        this.bftExtraDataCodec = bftExtraDataCodec;
    }

    @Override
    public QbftBlock createBlock(final long headerTimeStampSeconds, final QbftBlockHeader parentHeader) {
        var block = besuBlockCreator.createBlock(headerTimeStampSeconds, BlockWrapper.toBesuBlockHeader(parentHeader));
        return new QbftBlockImpl(block.getBlock().getHeader(), block.getBlock().getBody());
    }

    @Override
    public QbftBlock createSealedBlock(final ExtraDataProvider bftExtraDataProvider, final QbftBlock block, final int roundNumber, final Collection<SECPSignature> commitSeals) {
        final BlockHeader initialBesuHeader = BlockWrapper.toBesuBlockHeader(block.getQbftBlockHeader());
        BftExtraData initialExtraData = bftExtraDataProvider.getExtraData(block.getQbftBlockHeader());

        final BftExtraData sealedExtraData =
                new BftExtraData(
                        initialExtraData.getVanityData(),
                        commitSeals,
                        initialExtraData.getVote(),
                        roundNumber,
                        initialExtraData.getValidators());

        final BlockHeader sealedHeader =
                BlockHeaderBuilder.fromHeader(initialBesuHeader)
                        .extraData(bftExtraDataCodec.encode(sealedExtraData))
                        .blockHeaderFunctions(BftBlockHeaderFunctions.forOnchainBlock(bftExtraDataCodec))
                        .buildBlockHeader();

        return new QbftBlockImpl(sealedHeader, BlockWrapper.toBesuBlock(block).getBody());
    }
}
