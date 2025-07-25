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
package org.hyperledger.besu.ethereum.eth.sync.fastsync;

import static org.hyperledger.besu.ethereum.eth.sync.fastsync.SyncTargetNumberRangeSource.Direction.FORWARDS;

import java.util.Iterator;
import java.util.Optional;

/**
 * Provides a source of {@link SyncTargetNumberRange} starting from the end block and moving
 * backwards in increments defined by the header request size.
 */
public class SyncTargetNumberRangeSource implements Iterator<SyncTargetNumberRange> {
  private final long startBlock;
  private final long endBlock;
  private final int headerRequestSize;

  public enum Direction {
    FORWARDS,
    BACKWARDS
  }

  private final Direction direction;

  private Optional<SyncTargetNumberRange> maybeLastRange = Optional.empty();

  /**
   * Constructs a SyncTargetNumberRangeSource.
   *
   * @param startBlock the starting block number
   * @param endBlock the ending block number
   * @param headerRequestSize the size of the header request
   * @param direction the direction of iteration (FORWARDS or BACKWARDS)
   */
  public SyncTargetNumberRangeSource(
      final long startBlock,
      final long endBlock,
      final int headerRequestSize,
      final Direction direction) {
    this.startBlock = startBlock;
    this.endBlock = endBlock;
    this.headerRequestSize = headerRequestSize;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    return direction == FORWARDS ? !hasReachedEndBlock() : !hasReachedStartBlock();
  }

  @Override
  public SyncTargetNumberRange next() {
    if (maybeLastRange.isEmpty()) {
      final SyncTargetNumberRange firstRange = createFirstRange();
      maybeLastRange = Optional.of(firstRange);
      return firstRange;
    } else if (direction == FORWARDS ? hasReachedEndBlock() : hasReachedStartBlock()) {
      return null;
    } else {
      final SyncTargetNumberRange lastRange = maybeLastRange.get();
      final SyncTargetNumberRange nextRange = createNextRange(lastRange);
      maybeLastRange = Optional.of(nextRange);
      return nextRange;
    }
  }

  private SyncTargetNumberRange createFirstRange() {
    if (direction == FORWARDS) {
      long upper = Math.min(startBlock + headerRequestSize, endBlock);
      return new SyncTargetNumberRange(startBlock, upper);
    } else {
      return new SyncTargetNumberRange(endBlock - headerRequestSize, endBlock);
    }
  }

  private SyncTargetNumberRange createNextRange(final SyncTargetNumberRange lastRange) {
    if (direction == FORWARDS) {
      final long nextLower = lastRange.upperBlockNumber();
      final long nextUpper = Math.min(nextLower + headerRequestSize, endBlock);
      return new SyncTargetNumberRange(nextLower, nextUpper);
    } else {
      final long lowerBlockNumber = Math.max(lastRange.lowerBlockNumber() - headerRequestSize, 0);
      return new SyncTargetNumberRange(lowerBlockNumber, lastRange.lowerBlockNumber());
    }
  }

  private boolean hasReachedStartBlock() {
    return maybeLastRange.map(r -> r.lowerBlockNumber() <= startBlock).orElse(false);
  }

  private boolean hasReachedEndBlock() {
    return maybeLastRange.map(r -> r.upperBlockNumber() >= endBlock).orElse(false);
  }
}
