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
package org.hyperledger.besu.ethereum.trie.pathbased.common;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** Context which holds information relevant to a bonsai archive storage query. */
public class BonsaiContext {

  private final AtomicReference<Long> blockNumber;

  /** Creates an empty context with no block number set. */
  public BonsaiContext() {
    this.blockNumber = new AtomicReference<>(null);
  }

  /**
   * Context for Bonsai storage i.e. the block the storage applies to
   *
   * @param blockNumber the block number for this context
   */
  public BonsaiContext(final long blockNumber) {
    this.blockNumber = new AtomicReference<>(blockNumber);
  }

  /**
   * Get the block number currently applied to this context
   *
   * @return the optional block number
   */
  public Optional<Long> getBlockNumber() {
    return Optional.ofNullable(blockNumber.get());
  }

  /**
   * Set the block number for this context
   *
   * @param blockNumber the block number to set, or null to clear
   */
  public void setBlockNumber(final Long blockNumber) {
    this.blockNumber.set(blockNumber);
  }
}
