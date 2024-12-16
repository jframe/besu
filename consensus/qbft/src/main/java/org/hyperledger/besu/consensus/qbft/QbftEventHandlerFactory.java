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
package org.hyperledger.besu.consensus.qbft;

import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BftBlockHashing;
import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.bft.BftEventQueue;
import org.hyperledger.besu.consensus.common.bft.BftExecutors;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.consensus.common.bft.BlockTimer;
import org.hyperledger.besu.consensus.common.bft.EthSynchronizerUpdater;
import org.hyperledger.besu.consensus.common.bft.MessageTracker;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.UniqueMessageMulticaster;
import org.hyperledger.besu.consensus.common.bft.blockcreation.ProposerSelector;
import org.hyperledger.besu.consensus.common.bft.network.ValidatorPeers;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftEventHandler;
import org.hyperledger.besu.consensus.common.bft.statemachine.FutureMessageBuffer;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.qbft.blockcreation.QbftBlockCreatorFactory;
import org.hyperledger.besu.consensus.qbft.core.datatypes.BlockEncoderRegistry;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftFinalState;
import org.hyperledger.besu.consensus.qbft.core.events.MinedBlockObserver;
import org.hyperledger.besu.consensus.qbft.core.payload.MessageFactory;
import org.hyperledger.besu.consensus.qbft.core.statemachine.QbftBlockHeightManagerFactory;
import org.hyperledger.besu.consensus.qbft.core.statemachine.QbftController;
import org.hyperledger.besu.consensus.qbft.core.statemachine.QbftRoundFactory;
import org.hyperledger.besu.consensus.qbft.core.validation.MessageValidatorFactory;
import org.hyperledger.besu.consensus.qbft.protocol.Istanbul100SubProtocol;
import org.hyperledger.besu.consensus.qbft.types.BftEventHandlerAdaptor;
import org.hyperledger.besu.consensus.qbft.types.BlockHashingImpl;
import org.hyperledger.besu.consensus.qbft.types.BlockUtil;
import org.hyperledger.besu.consensus.qbft.types.BlockchainImpl;
import org.hyperledger.besu.consensus.qbft.types.QbftBlockCreatorFactoryImpl;
import org.hyperledger.besu.consensus.qbft.types.QbftBlockEncoder;
import org.hyperledger.besu.consensus.qbft.types.QbftExtraDataProviderImpl;
import org.hyperledger.besu.consensus.qbft.types.QbftFinalStateImpl;
import org.hyperledger.besu.consensus.qbft.types.QbftProtocolContextImpl;
import org.hyperledger.besu.consensus.qbft.types.QbftProtocolScheduleImpl;
import org.hyperledger.besu.consensus.qbft.types.ValidatorModeTransitionLoggerImpl;
import org.hyperledger.besu.consensus.qbft.validator.ValidatorModeTransitionLogger;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.util.Subscribers;

import java.time.Clock;
import java.time.Duration;

public class QbftEventHandlerFactory {

  public BftEventHandler create(
      final Blockchain blockchain,
      final ProtocolContext protocolContext,
      final BftProtocolSchedule bftProtocolSchedule,
      final ProposerSelector proposerSelector,
      final QbftConfigOptions qbftConfig,
      final NodeKey nodeKey,
      final BftEventQueue bftEventQueue,
      final BftExecutors bftExecutors,
      final Clock clock,
      final ForksSchedule<QbftConfigOptions> qbftForksSchedule,
      final QbftBlockCreatorFactory qbftBlockCreatorFactory,
      final EthProtocolManager ethProtocolManager,
      final org.hyperledger.besu.ethereum.chain.MinedBlockObserver blockLogger) {
    final QbftExtraDataCodec qbftExtraDataCodec = new QbftExtraDataCodec();

    final MessageValidatorFactory messageValidatorFactory =
        new MessageValidatorFactory(
            proposerSelector,
            new QbftProtocolScheduleImpl(bftProtocolSchedule, protocolContext),
            new QbftProtocolContextImpl(protocolContext));

    final FutureMessageBuffer futureMessageBuffer =
        new FutureMessageBuffer(
            qbftConfig.getFutureMessagesMaxDistance(),
            qbftConfig.getFutureMessagesLimit(),
            blockchain.getChainHeadBlockNumber());
    final MessageTracker duplicateMessageTracker =
        new MessageTracker(qbftConfig.getDuplicateMessageLimit());

    final MessageFactory messageFactory = new MessageFactory(nodeKey);

    final ValidatorProvider validatorProvider =
        protocolContext.getConsensusContext(BftContext.class).getValidatorProvider();

    // NOTE: peers should not be used for accessing the network as it does not enforce the
    // "only send once" filter applied by the UniqueMessageMulticaster.
    var peers = new ValidatorPeers(validatorProvider, Istanbul100SubProtocol.NAME);

    final UniqueMessageMulticaster uniqueMessageMulticaster =
        new UniqueMessageMulticaster(peers, qbftConfig.getGossipedHistoryLimit());

    final QbftGossip gossiper = new QbftGossip(uniqueMessageMulticaster, qbftExtraDataCodec);

    final QbftFinalState finalState =
        new QbftFinalStateImpl(
            validatorProvider,
            nodeKey,
            Util.publicKeyToAddress(nodeKey.getPublicKey()),
            proposerSelector,
            uniqueMessageMulticaster,
            new RoundTimer(
                bftEventQueue,
                Duration.ofSeconds(qbftConfig.getRequestTimeoutSeconds()),
                bftExecutors),
            new BlockTimer(bftEventQueue, qbftForksSchedule, bftExecutors, clock),
            new QbftBlockCreatorFactoryImpl(qbftBlockCreatorFactory, qbftExtraDataCodec),
            clock);

    final Subscribers<MinedBlockObserver> minedBlockObservers = Subscribers.create();
    minedBlockObservers.subscribe(
        qbftBlock -> ethProtocolManager.blockMined(BlockUtil.toBesuBlock(qbftBlock)));
    minedBlockObservers.subscribe(
        qbftBlock -> blockLogger.blockMined(BlockUtil.toBesuBlock(qbftBlock)));

    BlockEncoderRegistry.getInstance().setEncoder(new QbftBlockEncoder(qbftExtraDataCodec));

    final var qbftController =
        new QbftController(
            new BlockchainImpl(blockchain),
            finalState,
            new QbftBlockHeightManagerFactory(
                finalState,
                new QbftRoundFactory(
                    finalState,
                    new QbftProtocolContextImpl(protocolContext),
                    new QbftProtocolScheduleImpl(bftProtocolSchedule, protocolContext),
                    minedBlockObservers,
                    messageValidatorFactory,
                    messageFactory,
                    new QbftExtraDataProviderImpl(qbftExtraDataCodec),
                    new BlockHashingImpl(new BftBlockHashing(qbftExtraDataCodec))),
                messageValidatorFactory,
                messageFactory,
                new ValidatorModeTransitionLoggerImpl(
                    new ValidatorModeTransitionLogger(qbftForksSchedule))),
            gossiper,
            duplicateMessageTracker,
            futureMessageBuffer,
            new EthSynchronizerUpdater(ethProtocolManager.ethContext().getEthPeers()),
            qbftExtraDataCodec);

    return new BftEventHandlerAdaptor(qbftController);
  }
}
