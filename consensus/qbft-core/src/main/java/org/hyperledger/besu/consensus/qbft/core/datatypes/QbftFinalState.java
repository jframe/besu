package org.hyperledger.besu.consensus.qbft.core.datatypes;

import org.hyperledger.besu.consensus.common.bft.BlockTimer;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.network.ValidatorMulticaster;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;

import java.time.Clock;
import java.util.Collection;

public interface QbftFinalState {
    ValidatorMulticaster getValidatorMulticaster();

    NodeKey getNodeKey();

    RoundTimer getRoundTimer();

    boolean isLocalNodeValidator();

    Collection<Address> getValidators();

    Address getLocalAddress();

    Clock getClock();

    QbftBlockCreatorFactory getBlockCreatorFactory();

    int getQuorum();

    BlockTimer getBlockTimer();

    boolean isLocalNodeProposerForRound(ConsensusRoundIdentifier roundIdentifier);
}
