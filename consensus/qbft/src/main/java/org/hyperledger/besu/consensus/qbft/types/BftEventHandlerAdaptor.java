package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.events.BftReceivedMessageEvent;
import org.hyperledger.besu.consensus.common.bft.events.BlockTimerExpiry;
import org.hyperledger.besu.consensus.common.bft.events.NewChainHead;
import org.hyperledger.besu.consensus.common.bft.events.RoundExpiry;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftEventHandler;
import org.hyperledger.besu.ethereum.core.BlockHeader;

public class BftEventHandlerAdaptor implements BftEventHandler {
    private final org.hyperledger.besu.consensus.qbft.core.events.BftEventHandler bftEventHandler;

    public BftEventHandlerAdaptor(final org.hyperledger.besu.consensus.qbft.core.events.BftEventHandler bftEventHandler) {
        this.bftEventHandler = bftEventHandler;
    }

    @Override
    public void start() {
        bftEventHandler.start();
    }

    @Override
    public void handleMessageEvent(final BftReceivedMessageEvent msg) {
        // TODO should have different type
        bftEventHandler.handleMessageEvent(msg);
    }

    @Override
    public void handleNewBlockEvent(final NewChainHead newChainHead) {
        BlockHeader besuNewChainHeadHeader = newChainHead.getNewChainHeadHeader();
        var qbftChainHead = new org.hyperledger.besu.consensus.qbft.core.events.NewChainHead(new QbftBlockHeaderImpl(besuNewChainHeadHeader, besuNewChainHeadHeader.getBlockHeaderFunctions()));
        bftEventHandler.handleNewBlockEvent(qbftChainHead);
    }

    @Override
    public void handleBlockTimerExpiry(final BlockTimerExpiry blockTimerExpiry) {
        // TODO should have different type
        bftEventHandler.handleBlockTimerExpiry(blockTimerExpiry);
    }

    @Override
    public void handleRoundExpiry(final RoundExpiry roundExpiry) {
        // TODO should have different type
        bftEventHandler.handleRoundExpiry(roundExpiry);
    }
}
