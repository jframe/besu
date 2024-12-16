package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;

public class BlockWrapper {

    public static Block toBesuBlock(final QbftBlock block) {
        if (block instanceof Block) {
            return (Block) block;
        }
        throw new IllegalStateException("Invalid Block type");
    }

    public static BlockHeader toBesuBlockHeader(final QbftBlockHeader header) {
        if (header instanceof BlockHeader) {
            return (BlockHeader) header;
        }
        throw new IllegalStateException("Invalid BlockHeader type");
    }

}
