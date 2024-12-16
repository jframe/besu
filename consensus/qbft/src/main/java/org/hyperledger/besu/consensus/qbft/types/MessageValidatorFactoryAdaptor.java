package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.blockcreation.ProposerSelector;
import org.hyperledger.besu.consensus.qbft.core.validation.MessageValidatorFactory;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

public class MessageValidatorFactoryAdaptor extends MessageValidatorFactory {

    /**
     * Instantiates a new Message validator factory.
     *
     * @param proposerSelector  the proposer selector
     * @param protocolSchedule  the protocol schedule
     * @param protocolContext   the protocol context
     */
    public MessageValidatorFactoryAdaptor(final ProposerSelector proposerSelector, final ProtocolSchedule protocolSchedule, final ProtocolContext protocolContext) {
        super(proposerSelector, new QbftProtocolScheduleImpl(protocolSchedule, protocolContext), new QbftProtocolContextImpl(protocolContext));
    }
}
