package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftProtocolSchedule;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftProtocolSpec;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

public class QbftProtocolScheduleImpl implements QbftProtocolSchedule {

    private final ProtocolSchedule besuProtocolSchedule;
    private final ProtocolContext context;

    public QbftProtocolScheduleImpl(final ProtocolSchedule besuProtocolSchedule, final ProtocolContext context) {
        this.besuProtocolSchedule = besuProtocolSchedule;
        this.context = context;
    }

    @Override
    public QbftProtocolSpec getByBlockHeader(final QbftBlockHeader header) {
        final ProtocolSpec protocolSpec = besuProtocolSchedule.getByBlockHeader(BlockWrapper.toBesuBlockHeader(header));
        return new QbftProtocolSpecImpl(protocolSpec, context);
    }
}
