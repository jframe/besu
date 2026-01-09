/*
 * Copyright contributors to Hyperledger Besu.
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

import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the target block hash for full sync on post-merge networks. Listens to forkchoice events
 * from the consensus layer and updates the termination condition's target hash to the safe block.
 */
public class FullSyncTargetBlockSelector {

  private static final Logger LOG = LoggerFactory.getLogger(FullSyncTargetBlockSelector.class);

  private final Supplier<Optional<ForkchoiceEvent>> forkchoiceStateSupplier;
  private final FlexibleBlockHashTerminalCondition terminationCondition;
  private final CompletableFuture<Hash> targetBlock = new CompletableFuture<>();
  private volatile Hash lastSafeBlockHash = Hash.ZERO;

  public FullSyncTargetBlockSelector(
      final Supplier<Optional<ForkchoiceEvent>> forkchoiceStateSupplier,
      final FlexibleBlockHashTerminalCondition terminationCondition) {
    this.forkchoiceStateSupplier = forkchoiceStateSupplier;
    this.terminationCondition = terminationCondition;
  }

  /**
   * Called when a new forkchoice event is received from the consensus layer. Updates the
   * termination condition's target hash to the safe block.
   *
   * @param event the forkchoice event containing the safe block hash
   */
  public void onNewForkchoiceEvent(final ForkchoiceEvent event) {
    if (event.hasValidSafeBlockHash()) {
      final Hash safeBlockHash = event.getSafeBlockHash();
      if (!safeBlockHash.equals(lastSafeBlockHash) && !safeBlockHash.equals(Hash.ZERO)) {
        LOG.info(
            "Updating full sync target to safe block hash: {} (previous: {})",
            safeBlockHash,
            lastSafeBlockHash);
        lastSafeBlockHash = safeBlockHash;
        terminationCondition.setBlockHash(safeBlockHash);
        if (!targetBlock.isDone()) {
          targetBlock.complete(safeBlockHash);
        }
      }
    }
  }

  /**
   * Returns a future that completes when a valid target block hash is available. Checks if a
   * forkchoice event is already available and processes it first.
   *
   * @return a CompletableFuture that completes with the target block hash
   */
  public CompletableFuture<Hash> waitForTarget() {
    forkchoiceStateSupplier.get().ifPresent(this::onNewForkchoiceEvent);
    return targetBlock;
  }
}
