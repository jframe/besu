package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.QbftBlockInterface;
import org.hyperledger.besu.consensus.qbft.core.datatypes.HashMode;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlock;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;

import static org.hyperledger.besu.consensus.qbft.types.BlockWrapper.toBesuBlock;

public class QbftBlockInterfaceImpl implements QbftBlockInterface {
  private final QbftExtraDataCodec bftExtraDataCodec = new QbftExtraDataCodec();
  private final BftBlockInterface bftBlockInterface;

  public QbftBlockInterfaceImpl(
      final BftBlockInterface bftBlockInterface) {
    this.bftBlockInterface = bftBlockInterface;
  }

    @Override
    public QbftBlock replaceRoundInBlock(final QbftBlock proposalBlock, final int roundNumber, final HashMode hashMode) {
      final Block besuBlock = toBesuBlock(proposalBlock);
      final BlockHeaderFunctions blockHeaderFunctions = getBlockHeaderFunctions(hashMode);
      final Block updatedRoundBlock = bftBlockInterface.replaceRoundInBlock(besuBlock, roundNumber, blockHeaderFunctions);
      return new QbftBlockWrapper(new QbftBlockHeaderImpl(updatedRoundBlock.getHeader(), blockHeaderFunctions), updatedRoundBlock.getBody());
    }

  private BlockHeaderFunctions getBlockHeaderFunctions(final HashMode hashMode) {
    if (hashMode == HashMode.ONCHAIN) {
      return BftBlockHeaderFunctions.forOnchainBlock(bftExtraDataCodec);
    } else if (hashMode == HashMode.COMMITTED_SEAL) {
      return BftBlockHeaderFunctions.forCommittedSeal(bftExtraDataCodec);
    } else {
      throw new IllegalStateException("Invalid HashMode");
    }
  }
}
