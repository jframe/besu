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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.ethereum.core.BlockHeader;

/** What the pivot mover moves (cf. snap-new2 StateSync.moveTo). */
public interface SnapV2MoveTarget {

  BlockHeader currentPivot();

  /** True once the download completed, failed or was cancelled. */
  boolean isFinished();

  /**
   * Moves the download to {@code move.newPivot()}.
   *
   * @return false if the download finished before the move could start (nothing applied)
   * @throws SnapV2MoveFailedException if the move failed before anything was written
   */
  boolean applyMove(SnapV2PivotMove move);

  boolean checkCompletion(BlockHeader header);

  /** Fails the download: the state cannot be recovered (cf. snap-new2 RestartException). */
  void abort(Throwable cause);
}
