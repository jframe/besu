package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.common.validator.VoteProvider;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftValidatorProvider;
import org.hyperledger.besu.datatypes.Address;

import java.util.Collection;
import java.util.Optional;

import static org.hyperledger.besu.consensus.qbft.types.BlockWrapper.toBesuBlockHeader;

public class QbftValidatorProviderImpl implements QbftValidatorProvider {

    private final ValidatorProvider validatorProvider;

    public QbftValidatorProviderImpl(final ValidatorProvider validatorProvider) {
        this.validatorProvider = validatorProvider;
    }

    @Override
    public Collection<Address> getValidatorsAtHead() {
        return validatorProvider.getValidatorsAtHead();
    }

    @Override
    public Collection<Address> getValidatorsAfterBlock(final QbftBlockHeader header) {
        return validatorProvider.getValidatorsAfterBlock(toBesuBlockHeader(header));
    }

    @Override
    public Collection<Address> getValidatorsForBlock(final QbftBlockHeader header) {
        return validatorProvider.getValidatorsForBlock(toBesuBlockHeader(header));
    }

    @Override
    public Optional<VoteProvider> getVoteProviderAtHead() {
        return validatorProvider.getVoteProviderAtHead();
    }

}
