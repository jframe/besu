package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.qbft.core.datatypes.Blockchain;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeader;

public class BlockchainImpl implements Blockchain {
    private final org.hyperledger.besu.ethereum.chain.Blockchain blockchain;

    public BlockchainImpl(final org.hyperledger.besu.ethereum.chain.Blockchain blockchain) {
        this.blockchain = blockchain;
    }

    @Override
    public QbftBlockHeader getChainHeadHeader() {
        BlockHeader chainHeadHeader = blockchain.getChainHeadHeader();
        return new QbftBlockHeaderImpl(chainHeadHeader, chainHeadHeader.getBlockHeaderFunctions());
    }

    @Override
    public long getChainHeadBlockNumber() {
        return blockchain.getChainHeadBlockNumber();
    }
}
