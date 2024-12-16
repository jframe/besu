package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockValidator;
import org.hyperledger.besu.consensus.qbft.core.datatypes.ProtocolContext;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlock;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;

public class QbftBlockValidatorImpl implements QbftBlockValidator {

    private final BlockValidator blockValidator;
    private final org.hyperledger.besu.ethereum.ProtocolContext besuProtocolContext;

    public QbftBlockValidatorImpl(final BlockValidator blockValidator, final org.hyperledger.besu.ethereum.ProtocolContext besuProtocolContext) {
        this.blockValidator = blockValidator;
        this.besuProtocolContext = besuProtocolContext;
    }

    @Override
    public ValidationResult validateAndProcessBlock(final ProtocolContext protocolContext, final QbftBlock block) {
        final BlockProcessingResult blockProcessingResult = blockValidator.validateAndProcessBlock(besuProtocolContext, BlockWrapper.toBesuBlock(block), HeaderValidationMode.FULL, HeaderValidationMode.NONE, false);
        return new ValidationResult(blockProcessingResult.isSuccessful(), blockProcessingResult.errorMessage);
    }
}
