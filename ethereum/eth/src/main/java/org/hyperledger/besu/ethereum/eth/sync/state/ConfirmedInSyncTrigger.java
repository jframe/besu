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
package org.hyperledger.besu.ethereum.eth.sync.state;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an action exactly once, on the first in-sync notification that can be trusted. A node with
 * no peers is considered "in sync" by definition, so an in-sync notification that arrives while no
 * peer chain-head estimate is available cannot be distinguished from a transient loss of all peers
 * mid chain-download — unless no chain download has ever started, in which case the node is
 * genuinely isolated and the notification is trusted.
 */
public class ConfirmedInSyncTrigger {
  private static final Logger LOG = LoggerFactory.getLogger(ConfirmedInSyncTrigger.class);

  private final SyncState syncState;
  private final Runnable action;
  private final AtomicBoolean triggered = new AtomicBoolean(false);
  private final AtomicBoolean chainDownloadStarted = new AtomicBoolean(false);
  private volatile long inSyncSubscriptionId;
  private volatile long syncStatusSubscriptionId;

  private ConfirmedInSyncTrigger(final SyncState syncState, final Runnable action) {
    this.syncState = syncState;
    this.action = action;
  }

  /**
   * Subscribes to the given sync state and runs {@code action} once, on the first trusted in-sync
   * notification. Both subscriptions are removed when the action runs.
   *
   * @param syncState the sync state to observe
   * @param syncTolerance the number of blocks the local chain may be behind the best peer estimate
   *     while still being considered in sync
   * @param action the action to run once
   * @return the trigger
   */
  public static ConfirmedInSyncTrigger subscribe(
      final SyncState syncState, final long syncTolerance, final Runnable action) {
    final ConfirmedInSyncTrigger trigger = new ConfirmedInSyncTrigger(syncState, action);
    // Subscribe to sync status first so a sync target set immediately before an in-sync
    // notification is always observed by the time the notification is handled
    trigger.syncStatusSubscriptionId =
        syncState.subscribeSyncStatus(
            status -> status.ifPresent(s -> trigger.chainDownloadStarted.set(true)));
    trigger.inSyncSubscriptionId =
        syncState.subscribeInSync(trigger::onInSyncStatusChange, syncTolerance);
    return trigger;
  }

  private void onInSyncStatusChange(final boolean inSync) {
    if (!inSync) {
      return;
    }
    if (chainDownloadStarted.get() && syncState.getBestPeerChainHead().isEmpty()) {
      LOG.info(
          "Ignoring in-sync notification with no peer chain estimate after a chain download has"
              + " started; waiting for the next in-sync notification");
      return;
    }
    if (triggered.compareAndSet(false, true)) {
      syncState.unsubscribeInSync(inSyncSubscriptionId);
      syncState.unsubscribeSyncStatus(syncStatusSubscriptionId);
      action.run();
    }
  }
}
