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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveWorldState extends BonsaiWorldState {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveWorldState.class);

  private Optional<BonsaiContext> readContext = Optional.empty();
  private Optional<BonsaiContext> writeContext = Optional.empty();

  public BonsaiArchiveWorldState(
      final BonsaiWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache,
      final BlockHeader blockHeader) {
    super(archive, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache);

    // Initialize read context from the block header for this world state
    // This allows reads to work correctly for world states created at specific blocks
    if (blockHeader != null) {
      setReadContext(new BonsaiContext(blockHeader.getNumber()));
    } else {
      // No block header provided, default to genesis
      setReadContext(new BonsaiContext(0L));
    }
  }

  @Override
  protected Supplier<Optional<BonsaiContext>> getReadContextSupplier() {
    return () -> {
      if (readContext.isEmpty()) {
        LOG.warn("Read context is empty for BonsaiArchiveWorldState");
      }
      return readContext;
    };
  }

  @Override
  protected Supplier<Optional<BonsaiContext>> getWriteContextSupplier() {
    return () -> writeContext;
  }

  public void setReadContext(final BonsaiContext context) {
    this.readContext = Optional.of(context);
  }

  public void setWriteContext(final BonsaiContext context) {
    this.writeContext = Optional.of(context);
  }

  @Override
  protected void prePersist(final BlockHeader blockHeader) {
    long blockNumber = blockHeader == null ? 0L : blockHeader.getNumber();
    LOG.debug("prePersist: setting writeContext to block {}", blockNumber);
    setWriteContext(new BonsaiContext(blockNumber));
  }

  @Override
  protected void postPersistSuccess(final BlockHeader blockHeader) {
    if (blockHeader != null) {
      LOG.debug("postPersistSuccess: setting readContext to block {}", blockHeader.getNumber());
      setReadContext(new BonsaiContext(blockHeader.getNumber()));
    }
  }

  @Override
  public void resetWorldStateTo(final BlockHeader blockHeader) {
    super.resetWorldStateTo(blockHeader);
    // Update read context to match the new block we're reset to
    LOG.debug("resetWorldStateTo: setting readContext to block {}", blockHeader.getNumber());
    setReadContext(new BonsaiContext(blockHeader.getNumber()));
  }
}
