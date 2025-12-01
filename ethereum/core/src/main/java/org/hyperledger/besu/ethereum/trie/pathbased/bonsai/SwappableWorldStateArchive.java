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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around WorldStateArchive that allows swapping the underlying provider at runtime. This
 * is used during deferred archive migration to switch from BonsaiWorldStateProvider to
 * BonsaiArchiveWorldStateProvider without requiring a restart.
 */
public class SwappableWorldStateArchive implements WorldStateArchive {

  private static final Logger LOG = LoggerFactory.getLogger(SwappableWorldStateArchive.class);

  private final AtomicReference<WorldStateArchive> delegate;

  /**
   * Creates a new SwappableWorldStateArchive with the given initial provider.
   *
   * @param initialProvider the initial world state provider
   */
  public SwappableWorldStateArchive(final WorldStateArchive initialProvider) {
    this.delegate = new AtomicReference<>(initialProvider);
  }

  /**
   * Swaps the underlying provider to a new one. This is typically called after archive migration
   * completes to switch from BonsaiWorldStateProvider to BonsaiArchiveWorldStateProvider.
   *
   * @param newProvider the new world state provider
   */
  public void swapProvider(final WorldStateArchive newProvider) {
    final WorldStateArchive oldProvider = delegate.getAndSet(newProvider);
    LOG.info(
        "Swapped world state provider from {} to {}",
        oldProvider.getClass().getSimpleName(),
        newProvider.getClass().getSimpleName());
  }

  /**
   * Gets the current delegate provider.
   *
   * @return the current world state archive provider
   */
  public WorldStateArchive getDelegate() {
    return delegate.get();
  }

  @Override
  public Optional<WorldState> get(final Hash rootHash, final Hash blockHash) {
    return delegate.get().get(rootHash, blockHash);
  }

  @Override
  public boolean isWorldStateAvailable(final Hash rootHash, final Hash blockHash) {
    return delegate.get().isWorldStateAvailable(rootHash, blockHash);
  }

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    return delegate.get().getWorldState(queryParams);
  }

  @Override
  public MutableWorldState getWorldState() {
    return delegate.get().getWorldState();
  }

  @Override
  public void resetArchiveStateTo(final BlockHeader blockHeader) {
    delegate.get().resetArchiveStateTo(blockHeader);
  }

  @Override
  public Optional<Bytes> getNodeData(final Hash hash) {
    return delegate.get().getNodeData(hash);
  }

  @Override
  public <U> Optional<U> getAccountProof(
      final BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper) {
    return delegate.get().getAccountProof(blockHeader, accountAddress, accountStorageKeys, mapper);
  }

  @Override
  public void heal(final Optional<Address> maybeAccountToRepair, final Bytes location) {
    delegate.get().heal(maybeAccountToRepair, location);
  }

  @Override
  public void close() throws IOException {
    delegate.get().close();
  }
}
