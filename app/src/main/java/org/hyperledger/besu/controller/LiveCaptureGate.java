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
package org.hyperledger.besu.controller;

import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Computes the highest block number that is safe to commit to the trie-node history archive.
 *
 * <p>During initial sync (PoS backward sync, snap sync) {@link SyncState#isInitialSyncPhaseDone()}
 * is {@code false} and the network head is not reliably available via peer estimates. In that case
 * every block is safe to capture (we are importing already-finalised history), so {@link
 * Long#MAX_VALUE} is returned.
 *
 * <p>Once initial sync is complete the gate trails the live network head by {@code maxLayers} so
 * that blocks within the reorg window are not archived prematurely.
 */
class LiveCaptureGate implements LongSupplier {

  private final SyncState syncState;
  private final long maxLayers;
  private final AtomicLong highestSafeBlock = new AtomicLong(Long.MIN_VALUE);

  LiveCaptureGate(final SyncState syncState, final long maxLayers) {
    this.syncState = syncState;
    this.maxLayers = maxLayers;
  }

  @Override
  public long getAsLong() {
    if (!syncState.isInitialSyncPhaseDone()) {
      return Long.MAX_VALUE;
    }
    return highestSafeBlock.accumulateAndGet(
        syncState.bestChainHeight() - maxLayers, Math::max);
  }
}
