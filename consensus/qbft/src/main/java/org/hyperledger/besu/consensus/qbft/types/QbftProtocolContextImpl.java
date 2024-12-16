package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.qbft.core.QbftBlockInterface;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockImporter;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockValidator;
import org.hyperledger.besu.consensus.qbft.core.datatypes.ProtocolContext;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftValidatorProvider;

public class QbftProtocolContextImpl implements ProtocolContext {

    private final org.hyperledger.besu.ethereum.ProtocolContext besuProtocolContext;

    public QbftProtocolContextImpl(final org.hyperledger.besu.ethereum.ProtocolContext besuProtocolContext) {
        this.besuProtocolContext = besuProtocolContext;
    }

    @Override
    public QbftValidatorProvider getValidatorProvider() {
        return new QbftValidatorProviderImpl(besuProtocolContext.safeConsensusContext(BftContext.class).get().getValidatorProvider());
    }

    @Override
    public QbftBlockInterface getBlockInterface() {
        return new QbftBlockInterfaceImpl(besuProtocolContext.safeConsensusContext(BftContext.class).get().getBlockInterface());
    }

    @Override
    public QbftBlockValidator getBlockValidator() {
        return null;
    }

    @Override
    public QbftBlockImporter getBlockImporter() {
        return null;
    }
}
