/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.sync.fullsync;

import static java.util.concurrent.CompletableFuture.completedFuture;

import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.AbstractSyncTargetManager;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncTarget;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FullSyncTargetManager extends AbstractSyncTargetManager {

  private static final Logger LOG = LoggerFactory.getLogger(FullSyncTargetManager.class);
  private final SynchronizerConfiguration config;
  private final ProtocolContext protocolContext;
  private final EthContext ethContext;
  private final SyncTerminationCondition terminationCondition;
  private final Optional<Supplier<Optional<ForkchoiceEvent>>> forkchoiceStateSupplier;

  FullSyncTargetManager(
      final SynchronizerConfiguration config,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final SyncTerminationCondition terminationCondition) {
    this(
        config,
        protocolSchedule,
        protocolContext,
        ethContext,
        metricsSystem,
        terminationCondition,
        Optional.empty());
  }

  FullSyncTargetManager(
      final SynchronizerConfiguration config,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final SyncTerminationCondition terminationCondition,
      final Optional<Supplier<Optional<ForkchoiceEvent>>> forkchoiceStateSupplier) {
    super(config, protocolSchedule, protocolContext, ethContext, metricsSystem);
    this.config = config;
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.terminationCondition = terminationCondition;
    this.forkchoiceStateSupplier = forkchoiceStateSupplier;
  }

  @Override
  protected Optional<SyncTarget> finalizeSelectedSyncTarget(final SyncTarget syncTarget) {
    final BlockHeader commonAncestor = syncTarget.commonAncestor();
    if (protocolContext
        .getWorldStateArchive()
        .isWorldStateAvailable(commonAncestor.getStateRoot(), commonAncestor.getHash())) {
      return Optional.of(syncTarget);
    } else {
      LOG.warn(
          "Disconnecting {} because world state is not available at common ancestor at block {} ({})",
          syncTarget.peer(),
          commonAncestor.getNumber(),
          commonAncestor.getHash());
      syncTarget.peer().disconnect(DisconnectReason.USELESS_PEER_WORLD_STATE_NOT_AVAILABLE);
      return Optional.empty();
    }
  }

  @Override
  protected CompletableFuture<Optional<EthPeer>> selectBestAvailableSyncTarget() {
    // If we have a forkchoice target and we've reached the safe block, stop syncing
    if (hasReachedForkchoiceSafeBlock()) {
      LOG.info(
          "Reached FCU safe block target at block {}, stopping full sync to transition to backward sync",
          protocolContext.getBlockchain().getChainHeadBlockNumber());
      return completedFuture(Optional.empty());
    }

    final Optional<EthPeer> maybeBestPeer = ethContext.getEthPeers().bestPeerWithHeightEstimate();
    if (!maybeBestPeer.isPresent()) {
      LOG.info(
          "Unable to find sync target. Waiting for {} peers minimum. Currently checking {} peers for usefulness",
          config.getSyncMinimumPeerCount(),
          ethContext.getEthPeers().peerCount());
      return completedFuture(Optional.empty());
    } else {
      final EthPeer bestPeer = maybeBestPeer.get();
      if (isSyncTargetReached(bestPeer)) {
        // We're caught up to our best peer, try again when a new peer connects
        LOG.debug(
            "Caught up to best peer: {}, chain state: {}. Current peers: {}",
            bestPeer,
            bestPeer.chainState(),
            ethContext.getEthPeers().peerCount());
        return completedFuture(Optional.empty());
      }
      LOG.debug(
          "Best peer: {}, chain state: {}. Current peers: {}",
          bestPeer,
          bestPeer.chainState(),
          ethContext.getEthPeers().peerCount());
      return completedFuture(maybeBestPeer);
    }
  }

  private boolean isSyncTargetReached(final EthPeer peer) {
    final MutableBlockchain blockchain = protocolContext.getBlockchain();
    // We're in sync if the peer's chain is no better than our chain
    return !peer.chainState().chainIsBetterThan(blockchain.getChainHead());
  }

  /**
   * Checks if we've reached the FCU safe block target. This is used in post-merge networks to stop
   * full sync once we've synced up to the safe block announced by the consensus client.
   *
   * <p>IMPORTANT: This only applies AFTER terminal difficulty (TTD) has been reached. For networks
   * syncing through the merge, full sync must continue until TTD is reached regardless of FCU safe
   * block position.
   *
   * @return true if we have a forkchoice supplier, TTD has been reached, and we've reached or
   *     passed the safe block
   */
  private boolean hasReachedForkchoiceSafeBlock() {
    if (forkchoiceStateSupplier.isEmpty()) {
      return false;
    }

    // Only use FCU safe block targeting after TTD has been reached
    // This ensures networks syncing through the merge don't stop prematurely
    if (!terminationCondition.shouldStopDownload()) {
      return false;
    }

    final Optional<ForkchoiceEvent> maybeForkchoice = forkchoiceStateSupplier.get().get();
    if (maybeForkchoice.isEmpty() || !maybeForkchoice.get().hasValidSafeBlockHash()) {
      return false;
    }

    final Hash safeBlockHash = maybeForkchoice.get().getSafeBlockHash();
    final MutableBlockchain blockchain = protocolContext.getBlockchain();

    // Check if we have the safe block in our chain
    final boolean hasSafeBlock = blockchain.contains(safeBlockHash);

    if (hasSafeBlock) {
      LOG.debug("Chain contains FCU safe block {}", safeBlockHash);
    }

    return hasSafeBlock;
  }

  @Override
  public boolean shouldContinueDownloading() {
    return terminationCondition.shouldContinueDownload();
  }
}
