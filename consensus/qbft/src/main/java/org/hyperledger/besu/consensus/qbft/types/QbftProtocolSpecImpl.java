package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockValidator;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockImporter;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftProtocolSpec;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

public class QbftProtocolSpecImpl implements QbftProtocolSpec {
    private final ProtocolSpec besuProtocolSpec;
    private final ProtocolContext context;

    public QbftProtocolSpecImpl(final ProtocolSpec besuProtocolSpec, final ProtocolContext context) {
        this.besuProtocolSpec = besuProtocolSpec;
        this.context = context;
    }

    @Override
    public QbftBlockImporter getBlockImporter() {
        return block -> {
            final BlockImportResult blockImportResult = besuProtocolSpec.getBlockImporter().importBlock(context, BlockWrapper.toBesuBlock(block), HeaderValidationMode.FULL);
            return blockImportResult.isImported();
        };
    }

    @Override
    public QbftBlockValidator getBlockValidator() {
        return new QbftBlockValidatorImpl(besuProtocolSpec.getBlockValidator(), context);
    }
}
