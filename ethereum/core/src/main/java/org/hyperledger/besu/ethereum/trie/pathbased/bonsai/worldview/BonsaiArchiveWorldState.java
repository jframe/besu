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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootCommitter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
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
      final CodeCache codeCache) {
    super(archive, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache);
  }

  @Override
  public Supplier<Optional<BonsaiContext>> getReadContextSupplier() {
    return () -> {
      if (readContext.isEmpty()) {
        LOG.warn("Read context is empty! This should not happen for BonsaiArchiveWorldState");
      }
      return readContext;
    };
  }

  @Override
  public Supplier<Optional<BonsaiContext>> getWriteContextSupplier() {
    return () -> writeContext;
  }

  public void setReadContext(final BonsaiContext context) {
    this.readContext = Optional.of(context);
  }

  public void setWriteContext(final BonsaiContext context) {
    this.writeContext = Optional.of(context);
  }

  public void clearReadContext() {
    this.readContext = Optional.empty();
  }

  public void clearWriteContext() {
    this.writeContext = Optional.empty();
  }

  @Override
  public void persist(final BlockHeader blockHeader, final StateRootCommitter committer) {
    final Optional<BlockHeader> maybeBlockHeader = Optional.ofNullable(blockHeader);
    LOG.atDebug()
        .setMessage("Persist archive world state for block {}")
        .addArgument(maybeBlockHeader)
        .log();

    // Set write context for the block being persisted
    if (blockHeader != null) {
      setWriteContext(new BonsaiContext(blockHeader.getNumber()));
    }

    boolean success = false;

    // Get updater with write context supplier
    final BonsaiWorldStateKeyValueStorage.Updater stateUpdater =
        getWorldStateStorage().updater(getWriteContextSupplier());
    Runnable saveTrieLog = () -> {};
    Runnable cacheWorldState = () -> {};

    try {
      final Hash calculatedRootHash =
          committer.computeRootAndCommit(this, stateUpdater, blockHeader, worldStateConfig);

      if (blockHeader != null) {
        verifyWorldStateRoot(calculatedRootHash, blockHeader);
        saveTrieLog =
            () -> {
              trieLogManager.saveTrieLog(accumulator, calculatedRootHash, blockHeader, this);
            };
        cacheWorldState =
            () -> cachedWorldStorageManager.addCachedLayer(blockHeader, calculatedRootHash, this);

        stateUpdater
            .getWorldStateTransaction()
            .put(
                TRIE_BRANCH_STORAGE,
                WORLD_BLOCK_HASH_KEY,
                blockHeader.getHash().getBytes().toArrayUnsafe());
        worldStateBlockHash = blockHeader.getHash();
      } else {
        stateUpdater.getWorldStateTransaction().remove(TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY);
        worldStateBlockHash = null;
      }

      stateUpdater
          .getWorldStateTransaction()
          .put(
              TRIE_BRANCH_STORAGE,
              WORLD_ROOT_HASH_KEY,
              calculatedRootHash.getBytes().toArrayUnsafe());

      stateUpdater
          .getWorldStateTransaction()
          .put(
              TRIE_BRANCH_STORAGE,
              WORLD_BLOCK_NUMBER_KEY,
              Bytes.ofUnsignedLong(blockHeader == null ? 0L : blockHeader.getNumber())
                  .toArrayUnsafe());
      worldStateRootHash = calculatedRootHash;
      success = true;
    } finally {
      if (success) {
        saveTrieLog.run();
        stateUpdater.commitComposedOnly();
        if (!isStorageFrozen) {
          cacheWorldState.run();
        }
        accumulator.reset();

        // Update read context to match the persisted block
        if (blockHeader != null) {
          setReadContext(new BonsaiContext(blockHeader.getNumber()));
        }
      } else {
        stateUpdater.rollback();
        accumulator.reset();
      }
    }
  }
}
