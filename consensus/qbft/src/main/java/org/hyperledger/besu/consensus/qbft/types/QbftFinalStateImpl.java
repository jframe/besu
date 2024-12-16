package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.common.bft.BlockTimer;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.network.ValidatorMulticaster;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockCreatorFactory;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftFinalState;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;

import java.time.Clock;
import java.util.Collection;

public class QbftFinalStateImpl implements QbftFinalState {
    private final ValidatorProvider validatorProvider;
    private final NodeKey nodeKey;
    private final Address localAddress;
    private final RoundTimer roundTimer;
    private final BlockTimer blockTimer;
    private final QbftBlockCreatorFactory blockCreatorFactory;
    private final Clock clock;

    /**
     * Instantiates a new Bft final state.
     *
     * @param validatorProvider the validator provider
     * @param nodeKey the node key
     * @param localAddress the local address
     * @param roundTimer the round timer
     * @param blockTimer the block timer
     * @param blockCreatorFactory the block creator factory
     * @param clock the clock
     */
    public QbftFinalStateImpl(
            final ValidatorProvider validatorProvider,
            final NodeKey nodeKey,
            final Address localAddress,
            final RoundTimer roundTimer,
            final BlockTimer blockTimer,
            final QbftBlockCreatorFactory blockCreatorFactory,
            final Clock clock) {
        this.validatorProvider = validatorProvider;
        this.nodeKey = nodeKey;
        this.localAddress = localAddress;
        this.roundTimer = roundTimer;
        this.blockTimer = blockTimer;
        this.blockCreatorFactory = blockCreatorFactory;
        this.clock = clock;
    }

    /**
     * Gets validators.
     *
     * @return the validators
     */
    @Override
    public Collection<Address> getValidators() {
        return validatorProvider.getValidatorsAtHead();
    }

    /**
     * Gets the validator multicaster.
     *
     * @return the validator multicaster
     */
    @Override
    public ValidatorMulticaster getValidatorMulticaster() {
        return null;
    }

    /**
     * Gets node key.
     *
     * @return the node key
     */
    @Override
    public NodeKey getNodeKey() {
        return nodeKey;
    }

    /**
     * Gets local address.
     *
     * @return the local address
     */
    @Override
    public Address getLocalAddress() {
        return localAddress;
    }

    /**
     * Is local node validator.
     *
     * @return the boolean
     */
    @Override
    public boolean isLocalNodeValidator() {
        final boolean isValidator = getValidators().contains(localAddress);
        return isValidator;
    }

    /**
     * Gets round timer.
     *
     * @return the round timer
     */
    @Override
    public RoundTimer getRoundTimer() {
        return roundTimer;
    }

    /**
     * Gets block creator factory.
     *
     * @return the block creator factory
     */
    @Override
    public QbftBlockCreatorFactory getBlockCreatorFactory() {
        return blockCreatorFactory;
    }

    @Override
    public int getQuorum() {
        return BftHelpers.calculateRequiredValidatorQuorum(getValidators().size());
    }

    @Override
    public BlockTimer getBlockTimer() {
        return blockTimer;
    }

    @Override
    public boolean isLocalNodeProposerForRound(final ConsensusRoundIdentifier roundIdentifier) {
        final boolean isValidator = getValidators().contains(localAddress);
        return isValidator;
    }

    /**
     * Gets clock.
     *
     * @return the clock
     */
    @Override
    public Clock getClock() {
        return clock;
    }
}
