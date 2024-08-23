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
package org.hyperledger.besu.ethereum.eth.sync.validatorsync;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class ValidatorSyncSource implements Iterator<ValidatorSyncRange> {
  private final long checkpointTarget;
  private final long syncTarget;
  private final boolean backwards;
  private final int headerRequestSize;
  private final AtomicReference<Optional<ValidatorSyncRange>> maybeLastRange =
      new AtomicReference<>(Optional.empty());

  public ValidatorSyncSource(
      final long checkpointTarget,
      final long syncTarget,
      final boolean backwards,
      final int headerRequestSize) {
    this.checkpointTarget = checkpointTarget;
    this.syncTarget = syncTarget;
    this.backwards = backwards;
    this.headerRequestSize = headerRequestSize;
  }

  @Override
  public boolean hasNext() {
    return !hasReachedCheckpointTarget();
  }

  @Override
  public ValidatorSyncRange next() {
    if (maybeLastRange.get().isEmpty()) {
      final ValidatorSyncRange firstRange = createFirstRange();
      maybeLastRange.set(Optional.of(firstRange));
      return firstRange;
    } else if (hasReachedCheckpointTarget()) {
      return null;
    } else {
      final ValidatorSyncRange lastRange = maybeLastRange.get().get();
      final ValidatorSyncRange nextRange = createNextRange(lastRange);
      maybeLastRange.set(Optional.of(nextRange));
      return nextRange;
    }
  }

  private ValidatorSyncRange createFirstRange() {
    if (backwards) {
      return new ValidatorSyncRange(syncTarget - headerRequestSize, syncTarget);
    } else {
      final long startBlockNumber = checkpointTarget;
      return new ValidatorSyncRange(startBlockNumber, startBlockNumber + headerRequestSize);
    }
  }

  private ValidatorSyncRange createNextRange(final ValidatorSyncRange lastRange) {
    if (backwards) {
      final long lowerBlockNumber = Math.max(lastRange.lowerBlockNumber() - headerRequestSize, 0);
      return new ValidatorSyncRange(lowerBlockNumber, lastRange.lowerBlockNumber());
    } else {
      return new ValidatorSyncRange(
          lastRange.upperBlockNumber(), lastRange.upperBlockNumber() + headerRequestSize);
    }
  }

  private boolean hasReachedCheckpointTarget() {
    if (backwards) {
      return maybeLastRange.get().map(r -> r.lowerBlockNumber() <= checkpointTarget).orElse(false);
    } else {
      return maybeLastRange.get().map(r -> r.upperBlockNumber() >= syncTarget).orElse(false);
    }
  }
}
