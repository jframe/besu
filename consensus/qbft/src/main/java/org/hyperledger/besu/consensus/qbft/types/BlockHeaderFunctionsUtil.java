package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.datatypes.HashMode;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;

public class BlockHeaderFunctionsUtil {

    public static  BlockHeaderFunctions getBlockHeaderFunctions(final QbftExtraDataCodec extraDataCodec, final HashMode hashMode) {
        if (hashMode == HashMode.ONCHAIN) {
            return BftBlockHeaderFunctions.forOnchainBlock(extraDataCodec);
        } else if (hashMode == HashMode.COMMITTED_SEAL) {
            return BftBlockHeaderFunctions.forCommittedSeal(extraDataCodec);
        } else {
            throw new IllegalStateException("Invalid HashMode");
        }
    }
}
