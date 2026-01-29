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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;

import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiArchiveWorldStateProvider extends BonsaiWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveWorldStateProvider.class);

  public BonsaiArchiveWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final Optional<Long> maxLayersToLoad,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final CodeCache codeCache) {
    super(
        worldStateKeyValueStorage,
        blockchain,
        maxLayersToLoad,
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        worldStateHealerSupplier,
        codeCache);
  }

  @VisibleForTesting
  BonsaiArchiveWorldStateProvider(
      final BonsaiCachedWorldStorageManager bonsaiCachedWorldStorageManager,
      final TrieLogManager trieLogManager,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final EvmConfiguration evmConfiguration,
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final CodeCache codeCache) {
    super(
        bonsaiCachedWorldStorageManager,
        trieLogManager,
        worldStateKeyValueStorage,
        blockchain,
        bonsaiCachedMerkleTrieLoader,
        evmConfiguration,
        worldStateHealerSupplier,
        codeCache);
  }

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    // Log the current WORLD_BLOCK_NUMBER_KEY value BEFORE updating
    var currentWBN = worldStateKeyValueStorage.getWorldStateBlockNumber();
    LOG.info(
        "[DIAG] getWorldState: stateRoot={}, blockHash={}, blockNumber={}, shouldWorldStateUpdateHead={}, currentWORLD_BLOCK_NUMBER_KEY={}",
        queryParams.getStateRoot(),
        queryParams.getBlockHash().toShortHexString(),
        queryParams.getBlockHeader().getNumber(),
        queryParams.shouldWorldStateUpdateHead(),
        currentWBN.orElse(-1L));

    // For archive mode, ensure WORLD_BLOCK_NUMBER_KEY is set to (target - 1) before rolling.
    // This is critical because the archive flat DB strategy uses (WORLD_BLOCK_NUMBER_KEY + 1)
    // as the write context. When writing state for block N, we need write context = N,
    // so WORLD_BLOCK_NUMBER_KEY must be N-1.
    updateWorldBlockNumber(queryParams.getBlockHash());

    if (queryParams.shouldWorldStateUpdateHead()) {
      var result = getFullWorldState(queryParams);
      LOG.info(
          "[DIAG] getWorldState (updateHead=true): result={}",
          result.isPresent() ? "present" : "EMPTY");
      return result;
    } else {
      // If we are creating a world state for a historic/archive block, we have 2 options:
      // 1. Roll back and create a layered world state. We can do this as far back as 512 blocks by
      // default, and we end up with a full state trie & flat DB at the desired block
      // 2. Rely entirely on the flat DB, which is less safe because we can't check the world state
      // root is correct but at least gives us the ability to serve historic state. The rollback
      // step in this case is minimal - take the chain head state and reset the block hash and
      // number for
      // archive flat DB queries
      final BlockHeader chainHeadBlockHeader = blockchain.getChainHeadHeader();
      if (chainHeadBlockHeader.getNumber() - queryParams.getBlockHeader().getNumber()
          >= trieLogManager.getMaxLayersToLoad()) {
        LOG.debug(
            "Returning archive state without verifying state root {}",
            trieLogManager.getMaxLayersToLoad());
        return cachedWorldStorageManager
            .getWorldState(chainHeadBlockHeader.getHash())
            .map(MutableWorldState::disableTrie)
            .flatMap(
                worldState ->
                    rollMutableArchiveStateToBlockHash( // This is a tiny action for archive
                        // state
                        (PathBasedWorldState) worldState, queryParams.getBlockHeader().getHash()))
            .map(MutableWorldState::freezeStorage);
      }
      return super.getWorldState(queryParams);
    }
  }

  /**
   * Updates WORLD_BLOCK_NUMBER_KEY to the parent block number (target - 1) before rolling to target
   * block. This ensures the archive flat DB write context is correct: - Write context =
   * WORLD_BLOCK_NUMBER_KEY + 1 - When writing state for block N, we want write context = N - So
   * WORLD_BLOCK_NUMBER_KEY must be N-1 before writes
   */
  private void updateWorldBlockNumber(final Hash blockHash) {
    var maybeTargetBlockNumber = blockchain.getBlockHeader(blockHash).map(BlockHeader::getNumber);
    if (maybeTargetBlockNumber.isEmpty()) {
      LOG.warn("[DIAG] updateWorldBlockNumber: could not find block header for {}", blockHash);
      return;
    }

    var targetBlockNumber = maybeTargetBlockNumber.get();
    // We need WORLD_BLOCK_NUMBER_KEY to be at target-1 so writes use context target
    var requiredBlockNumber = targetBlockNumber - 1;
    var currentWorldStateBlockNumber = worldStateKeyValueStorage.getWorldStateBlockNumber();

    if (currentWorldStateBlockNumber.isEmpty()
        || !currentWorldStateBlockNumber.get().equals(requiredBlockNumber)) {
      LOG.info(
          "[DIAG] updateWorldBlockNumber: changing WORLD_BLOCK_NUMBER_KEY from {} to {} (target block={}, hash={})",
          currentWorldStateBlockNumber.orElse(-1L),
          requiredBlockNumber,
          targetBlockNumber,
          blockHash.toShortHexString());
      var updater = worldStateKeyValueStorage.updater();
      var worldStateTransaction = updater.getWorldStateTransaction();
      worldStateTransaction.put(
          TRIE_BRANCH_STORAGE,
          WORLD_BLOCK_NUMBER_KEY,
          Bytes.ofUnsignedLong(requiredBlockNumber).toArrayUnsafe());
      updater.commitComposedOnly();

      // Verify the update was successful by reading it back
      var verifyWBN = worldStateKeyValueStorage.getWorldStateBlockNumber();
      LOG.info(
          "[DIAG] updateWorldBlockNumber: verified write - WORLD_BLOCK_NUMBER_KEY now reads as {}",
          verifyWBN.orElse(-1L));
    } else {
      LOG.info(
          "[DIAG] updateWorldBlockNumber: no change needed, current={}, required={} (target={})",
          currentWorldStateBlockNumber.orElse(-1L),
          requiredBlockNumber,
          targetBlockNumber);
    }
  }

  // Archive-specific rollback behaviour. There is no trie-log roll forward/backward, we just roll
  // back the state root, block hash and block number
  protected Optional<MutableWorldState> rollMutableArchiveStateToBlockHash(
      final PathBasedWorldState mutableState, final Hash blockHash) {
    LOG.trace("Rolling mutable archive world state to block hash " + blockHash.toHexString());
    try {
      // Simply persist the block hash/number and state root for this archive state
      mutableState.persist(blockchain.getBlockHeader(blockHash).get());

      LOG.trace(
          "Archive rolling finished, {} now at {}",
          mutableState.getWorldStateStorage().getClass().getSimpleName(),
          blockHash);
      return Optional.of(mutableState);
    } catch (final MerkleTrieException re) {
      // need to throw to trigger the heal
      throw re;
    } catch (final Exception e) {
      LOG.atInfo()
          .setMessage("State rolling failed on {} for block hash {}: {}")
          .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
          .addArgument(blockHash)
          .addArgument(e)
          .log();

      return Optional.empty();
    }
  }
}
