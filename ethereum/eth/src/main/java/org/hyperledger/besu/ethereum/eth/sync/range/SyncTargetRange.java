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
package org.hyperledger.besu.ethereum.eth.sync.range;

import static java.lang.Math.toIntExact;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;

import java.util.Optional;

public record SyncTargetRange(EthPeer syncTarget, TargetRange targetRange) {

  public SyncTargetRange(final EthPeer syncTarget, final BlockHeader start) {
    this(syncTarget, new TargetRange(start, Optional.empty()));
  }

  public SyncTargetRange(final EthPeer syncTarget, final BlockHeader start, final BlockHeader end) {
    this(syncTarget, new TargetRange(start, Optional.of(end)));
  }

  public EthPeer getSyncTarget() {
    return syncTarget;
  }

  public BlockHeader getStart() {
    return targetRange.start();
  }

  public boolean hasEnd() {
    return targetRange.end().isPresent();
  }

  public BlockHeader getEnd() {
    return targetRange.end().get();
  }

  public int getSegmentLengthExclusive() {
    return toIntExact(targetRange.end().get().getNumber() - targetRange.start().getNumber() - 1);
  }
}
