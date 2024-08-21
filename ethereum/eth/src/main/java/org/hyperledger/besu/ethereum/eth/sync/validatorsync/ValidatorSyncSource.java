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

public class ValidatorSyncSource implements Iterator<ValidatorSyncRange> {
  public static final int HEADER_DOWNLOAD_SIZE = 512;
  private final long checkpointTarget;
  private final long syncTarget;

  private Optional<ValidatorSyncRange> maybeLastRange = Optional.empty();

  public ValidatorSyncSource(final long checkpointTarget, final long syncTarget) {
    this.checkpointTarget = checkpointTarget;
    this.syncTarget = syncTarget;
  }

  @Override
  public boolean hasNext() {
    return !hasReachedCheckpointTarget();
  }

  @Override
  public ValidatorSyncRange next() {
    if (maybeLastRange.isEmpty()) {
      maybeLastRange =
          Optional.of(new ValidatorSyncRange(syncTarget - HEADER_DOWNLOAD_SIZE, syncTarget));
      return maybeLastRange.get();
    } else if (hasReachedCheckpointTarget()) {
      return null;
    } else {
      final ValidatorSyncRange lastRange = maybeLastRange.get();
      final long lowerBlockNumber =
          Math.max(lastRange.lowerBlockNumber() - HEADER_DOWNLOAD_SIZE, 0);
      final ValidatorSyncRange nextRange =
          new ValidatorSyncRange(lowerBlockNumber, lastRange.lowerBlockNumber());
      maybeLastRange = Optional.of(nextRange);
      return nextRange;
    }
  }

  private boolean hasReachedCheckpointTarget() {
    return maybeLastRange.map(r -> r.lowerBlockNumber() <= checkpointTarget).orElse(false);
  }
}
