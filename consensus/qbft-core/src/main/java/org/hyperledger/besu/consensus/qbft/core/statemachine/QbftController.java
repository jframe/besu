/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.consensus.qbft.core.statemachine;

import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.Gossiper;
import org.hyperledger.besu.consensus.common.bft.MessageTracker;
import org.hyperledger.besu.consensus.common.bft.SynchronizerUpdater;
import org.hyperledger.besu.consensus.common.bft.events.BftReceivedMessageEvent;
import org.hyperledger.besu.consensus.common.bft.events.BlockTimerExpiry;
import org.hyperledger.besu.consensus.common.bft.events.RoundExpiry;
import org.hyperledger.besu.consensus.common.bft.messagewrappers.BftMessage;
import org.hyperledger.besu.consensus.common.bft.payload.Authored;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftFinalState;
import org.hyperledger.besu.consensus.common.bft.statemachine.FutureMessageBuffer;
import org.hyperledger.besu.consensus.qbft.core.datatypes.BlockHeader;
import org.hyperledger.besu.consensus.qbft.core.datatypes.Blockchain;
import org.hyperledger.besu.consensus.qbft.core.events.BftEventHandler;
import org.hyperledger.besu.consensus.qbft.core.events.NewChainHead;
import org.hyperledger.besu.consensus.qbft.core.messagedata.CommitMessageData;
import org.hyperledger.besu.consensus.qbft.core.messagedata.PrepareMessageData;
import org.hyperledger.besu.consensus.qbft.core.messagedata.ProposalMessageData;
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1;
import org.hyperledger.besu.consensus.qbft.core.messagedata.RoundChangeMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Qbft controller. */
public class QbftController implements BftEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(QbftController.class);
  private final Blockchain blockchain;
  private final BftFinalState bftFinalState;
  private final FutureMessageBuffer futureMessageBuffer;
  private final Gossiper gossiper;
  private final MessageTracker duplicateMessageTracker;
  private final SynchronizerUpdater synchronizerUpdater;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private BaseQbftBlockHeightManager currentHeightManager;
  private final QbftBlockHeightManagerFactory qbftBlockHeightManagerFactory;
  private final BftExtraDataCodec bftExtraDataCodec;

  /**
   * Instantiates a new Qbft controller.
   *
   * @param blockchain the blockchain
   * @param bftFinalState the bft final state
   * @param qbftBlockHeightManagerFactory the qbft block height manager factory
   * @param gossiper the gossiper
   * @param duplicateMessageTracker the duplicate errorMessage tracker
   * @param futureMessageBuffer the future errorMessage buffer
   * @param synchronizerUpdater the synchronizer updater
   * @param bftExtraDataCodec the bft extra data codec
   */
  public QbftController(
      final Blockchain blockchain,
      final BftFinalState bftFinalState,
      final QbftBlockHeightManagerFactory qbftBlockHeightManagerFactory,
      final Gossiper gossiper,
      final MessageTracker duplicateMessageTracker,
      final FutureMessageBuffer futureMessageBuffer,
      final SynchronizerUpdater synchronizerUpdater,
      final BftExtraDataCodec bftExtraDataCodec) {

    this.blockchain = blockchain;
    this.bftFinalState = bftFinalState;
    this.futureMessageBuffer = futureMessageBuffer;
    this.gossiper = gossiper;
    this.duplicateMessageTracker = duplicateMessageTracker;
    this.synchronizerUpdater = synchronizerUpdater;
    this.qbftBlockHeightManagerFactory = qbftBlockHeightManagerFactory;
    this.bftExtraDataCodec = bftExtraDataCodec;
  }

  protected void handleMessage(final Message message) {
    final MessageData messageData = message.getData();

    switch (messageData.getCode()) {
      case QbftV1.PROPOSAL:
        consumeMessage(
            message,
            ProposalMessageData.fromMessageData(messageData).decode(bftExtraDataCodec),
            currentHeightManager::handleProposalPayload);
        break;

      case QbftV1.PREPARE:
        consumeMessage(
            message,
            PrepareMessageData.fromMessageData(messageData).decode(),
            currentHeightManager::handlePreparePayload);
        break;

      case QbftV1.COMMIT:
        consumeMessage(
            message,
            CommitMessageData.fromMessageData(messageData).decode(),
            currentHeightManager::handleCommitPayload);
        break;

      case QbftV1.ROUND_CHANGE:
        consumeMessage(
            message,
            RoundChangeMessageData.fromMessageData(messageData).decode(bftExtraDataCodec),
            currentHeightManager::handleRoundChangePayload);
        break;

      default:
        throw new IllegalArgumentException(
            String.format(
                "Received errorMessage with messageCode=%d does not conform to any recognised QBFT errorMessage structure",
                message.getData().getCode()));
    }
  }

  protected void createNewHeightManager(final BlockHeader parentHeader) {
    currentHeightManager = qbftBlockHeightManagerFactory.create(parentHeader);
  }

  protected BaseBlockHeightManager getCurrentHeightManager() {
    return currentHeightManager;
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      startNewHeightManager(blockchain.getChainHeadHeader());
    }
  }

  @Override
  public void handleMessageEvent(final BftReceivedMessageEvent msg) {
    final MessageData data = msg.getMessage().getData();
    if (!duplicateMessageTracker.hasSeenMessage(data)) {
      duplicateMessageTracker.addSeenMessage(data);
      handleMessage(msg.getMessage());
    } else {
      LOG.trace("Discarded duplicate errorMessage");
    }
  }

  /**
   * Consume errorMessage.
   *
   * @param <P> the type parameter of BftMessage
   * @param message the errorMessage
   * @param bftMessage the bft errorMessage
   * @param handleMessage the handle errorMessage
   */
  protected <P extends BftMessage<?>> void consumeMessage(
      final Message message, final P bftMessage, final Consumer<P> handleMessage) {
    LOG.trace("Received BFT {} errorMessage", bftMessage.getClass().getSimpleName());

    // Discard all messages which target the BLOCKCHAIN height (which SHOULD be 1 less than
    // the currentHeightManager, but CAN be the same directly following import).
    if (bftMessage.getRoundIdentifier().getSequenceNumber()
        <= blockchain.getChainHeadBlockNumber()) {
      LOG.debug(
          "Discarding a errorMessage which targets a height {} not above current chain height {}.",
          bftMessage.getRoundIdentifier().getSequenceNumber(),
          blockchain.getChainHeadBlockNumber());
      return;
    }

    if (processMessage(bftMessage, message)) {
      gossiper.send(message);
      handleMessage.accept(bftMessage);
    }
  }

  @Override
  public void handleNewBlockEvent(final NewChainHead newChainHead) {
    final BlockHeader newBlockHeader = newChainHead.getNewChainHeadHeader();
    final BlockHeader currentMiningParent = getCurrentHeightManager().getParentBlockHeader();
    LOG.debug(
        "New chain head detected (block number={})," + " currently mining on top of {}.",
        newBlockHeader.getNumber(),
        currentMiningParent.getNumber());
    if (newBlockHeader.getNumber() < currentMiningParent.getNumber()) {
      LOG.trace(
          "Discarding NewChainHead event, was for previous block height. chainHeight={} eventHeight={}",
          currentMiningParent.getNumber(),
          newBlockHeader.getNumber());
      return;
    }

    if (newBlockHeader.getNumber() == currentMiningParent.getNumber()) {
      if (newBlockHeader.getHash().equals(currentMiningParent.getHash())) {
        LOG.trace(
            "Discarding duplicate NewChainHead event. chainHeight={} newBlockHash={} parentBlockHash={}",
            newBlockHeader.getNumber(),
            newBlockHeader.getHash(),
            currentMiningParent.getHash());
      } else {
        LOG.error(
            "Subsequent NewChainHead event at same block height indicates chain fork. chainHeight={}",
            currentMiningParent.getNumber());
      }
      return;
    }
    startNewHeightManager(newBlockHeader);
  }

  @Override
  public void handleBlockTimerExpiry(final BlockTimerExpiry blockTimerExpiry) {
    final ConsensusRoundIdentifier roundIdentifier = blockTimerExpiry.getRoundIdentifier();
    if (isMsgForCurrentHeight(roundIdentifier)) {
      getCurrentHeightManager().handleBlockTimerExpiry(roundIdentifier);
    } else {
      LOG.trace(
          "Block timer event discarded as it is not for current block height chainHeight={} eventHeight={}",
          getCurrentHeightManager().getChainHeight(),
          roundIdentifier.getSequenceNumber());
    }
  }

  @Override
  public void handleRoundExpiry(final RoundExpiry roundExpiry) {
    // Discard all messages which target the BLOCKCHAIN height (which SHOULD be 1 less than
    // the currentHeightManager, but CAN be the same directly following import).
    if (roundExpiry.getView().getSequenceNumber() <= blockchain.getChainHeadBlockNumber()) {
      LOG.debug("Discarding a round-expiry which targets a height not above current chain height.");
      return;
    }

    if (isMsgForCurrentHeight(roundExpiry.getView())) {
      getCurrentHeightManager().roundExpired(roundExpiry);
    } else {
      LOG.trace(
          "Round expiry event discarded as it is not for current block height chainHeight={} eventHeight={}",
          getCurrentHeightManager().getChainHeight(),
          roundExpiry.getView().getSequenceNumber());
    }
  }

  private void startNewHeightManager(final BlockHeader parentHeader) {
    createNewHeightManager(parentHeader);
    final long newChainHeight = getCurrentHeightManager().getChainHeight();
    futureMessageBuffer.retrieveMessagesForHeight(newChainHeight).forEach(this::handleMessage);
  }

  private boolean processMessage(final BftMessage<?> msg, final Message rawMsg) {
    final ConsensusRoundIdentifier msgRoundIdentifier = msg.getRoundIdentifier();
    if (isMsgForCurrentHeight(msgRoundIdentifier)) {
      return isMsgFromKnownValidator(msg) && bftFinalState.isLocalNodeValidator();
    } else if (isMsgForFutureChainHeight(msgRoundIdentifier)) {
      LOG.trace("Received errorMessage for future block height round={}", msgRoundIdentifier);
      futureMessageBuffer.addMessage(msgRoundIdentifier.getSequenceNumber(), rawMsg);
      // Notify the synchronizer the transmitting peer must have the parent block to the received
      // errorMessage's target height.
      synchronizerUpdater.updatePeerChainState(
          msgRoundIdentifier.getSequenceNumber() - 1L, rawMsg.getConnection());
    } else {
      LOG.trace(
          "BFT errorMessage discarded as it is from a previous block height messageType={} chainHeight={} eventHeight={}",
          msg.getMessageType(),
          getCurrentHeightManager().getChainHeight(),
          msgRoundIdentifier.getSequenceNumber());
    }
    return false;
  }

  private boolean isMsgFromKnownValidator(final Authored msg) {
    return bftFinalState.getValidators().contains(msg.getAuthor());
  }

  private boolean isMsgForCurrentHeight(final ConsensusRoundIdentifier roundIdentifier) {
    return roundIdentifier.getSequenceNumber() == getCurrentHeightManager().getChainHeight();
  }

  private boolean isMsgForFutureChainHeight(final ConsensusRoundIdentifier roundIdentifier) {
    return roundIdentifier.getSequenceNumber() > getCurrentHeightManager().getChainHeight();
  }
}
