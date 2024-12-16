package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.datatypes.ValidatorModeTransitionLogger;

public class ValidatorModeTransitionLoggerImpl implements ValidatorModeTransitionLogger {

    private final org.hyperledger.besu.consensus.qbft.validator.ValidatorModeTransitionLogger validatorModeTransitionLogger;

    public ValidatorModeTransitionLoggerImpl(final org.hyperledger.besu.consensus.qbft.validator.ValidatorModeTransitionLogger validatorModeTransitionLogger) {
        this.validatorModeTransitionLogger = validatorModeTransitionLogger;
    }

    @Override
    public void logTransitionChange(final QbftBlockHeader parentHeader) {
        validatorModeTransitionLogger.logTransitionChange(BlockWrapper.toBesuBlockHeader(parentHeader));
    }
}
