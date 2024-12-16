package org.hyperledger.besu.consensus.qbft.core.datatypes;

public interface QbftProtocolSpec {
    QbftBlockImporter getBlockImporter();

    QbftBlockValidator getBlockValidator();
}
