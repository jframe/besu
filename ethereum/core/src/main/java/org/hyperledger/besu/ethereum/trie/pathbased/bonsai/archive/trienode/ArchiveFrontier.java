/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * The highest block the trie-node archive may capture: blocks at or below it are treated as safe
 * from reorgs.
 *
 * <p>{@code max(finalized, bestChainHeight − maxLayersToLoad / 2, 0)}. Finality drives it on
 * proof-of-stake chains; the distance term drives it on chains without finality (QBFT, IBFT,
 * Clique) and during initial sync, where a stored finalized block can be stale.
 */
public final class ArchiveFrontier implements LongSupplier {

  private final Blockchain blockchain;
  private final LongSupplier bestChainHeight;
  private final long distance;

  public ArchiveFrontier(
      final Blockchain blockchain, final LongSupplier bestChainHeight, final long maxLayersToLoad) {
    this.blockchain = Objects.requireNonNull(blockchain);
    this.bestChainHeight = Objects.requireNonNull(bestChainHeight);
    this.distance = maxLayersToLoad / 2;
  }

  @Override
  public long getAsLong() {
    final long finalized =
        blockchain
            .getFinalized()
            .flatMap(blockchain::getBlockHeader)
            .map(BlockHeader::getNumber)
            .orElse(0L);
    return Math.max(0L, Math.max(finalized, bestChainHeight.getAsLong() - distance));
  }
}
