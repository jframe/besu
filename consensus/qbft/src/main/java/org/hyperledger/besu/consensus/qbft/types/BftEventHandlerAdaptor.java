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
package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.events.BftReceivedMessageEvent;
import org.hyperledger.besu.consensus.common.bft.events.BlockTimerExpiry;
import org.hyperledger.besu.consensus.common.bft.events.NewChainHead;
import org.hyperledger.besu.consensus.common.bft.events.RoundExpiry;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftEventHandler;
import org.hyperledger.besu.ethereum.core.BlockHeader;

public class BftEventHandlerAdaptor implements BftEventHandler {
  private final org.hyperledger.besu.consensus.qbft.core.events.BftEventHandler bftEventHandler;

  public BftEventHandlerAdaptor(
      final org.hyperledger.besu.consensus.qbft.core.events.BftEventHandler bftEventHandler) {
    this.bftEventHandler = bftEventHandler;
  }

  @Override
  public void start() {
    bftEventHandler.start();
  }

  @Override
  public void handleMessageEvent(final BftReceivedMessageEvent msg) {
    bftEventHandler.handleMessageEvent(msg);
  }

  @Override
  public void handleNewBlockEvent(final NewChainHead newChainHead) {
    BlockHeader besuNewChainHeadHeader = newChainHead.getNewChainHeadHeader();
    var qbftChainHead =
        new org.hyperledger.besu.consensus.qbft.core.events.NewChainHead(
            new QbftBlockHeaderImpl(
                besuNewChainHeadHeader, besuNewChainHeadHeader.getBlockHeaderFunctions()));
    bftEventHandler.handleNewBlockEvent(qbftChainHead);
  }

  @Override
  public void handleBlockTimerExpiry(final BlockTimerExpiry blockTimerExpiry) {
    bftEventHandler.handleBlockTimerExpiry(blockTimerExpiry);
  }

  @Override
  public void handleRoundExpiry(final RoundExpiry roundExpiry) {
    bftEventHandler.handleRoundExpiry(roundExpiry);
  }
}
