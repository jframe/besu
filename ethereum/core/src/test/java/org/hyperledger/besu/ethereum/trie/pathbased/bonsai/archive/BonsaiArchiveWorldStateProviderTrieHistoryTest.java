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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

// Note: ArchiveNodeKey, ArchiveTrieNodeCodec, TrieNodeHistoryProgress, TrieNodeHistoryStore need no
// import — this test class is in the same
// org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive
// package as BonsaiArchiveWorldStateProvider, and importing a same-package type is a checkstyle
// RedundantImport violation in this codebase.
class BonsaiArchiveWorldStateProviderTrieHistoryTest {

  private static final long MAX_LAYERS = 512L;
  private static final long CHAIN_HEAD = 10_000L;

  @Test
  void getAccountProofRoutesThroughHistoryWhenBlockIsCovered() {
    final Address address = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final PmtStateTrieAccountValue accountValue =
        new PmtStateTrieAccountValue(1L, Wei.of(2L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    // StoredMerklePatriciaTrie (not SimpleMerklePatriciaTrie) is required here: SimpleMerkleTrie's
    // commit(NodeUpdater) is a no-op ("nothing to do here" -- it's a pure in-memory trie), so it
    // never invokes the NodeUpdater callback we rely on below to capture (location, hash, value).
    // Starting from an empty root, the nodeLoader is never actually consulted.
    final MerkleTrie<Bytes, Bytes> accountTrie =
        new StoredMerklePatriciaTrie<>((location, hash) -> Optional.empty(), b -> b, b -> b);
    accountTrie.put(address.addressHash().getBytes(), RLP.encode(accountValue::writeTo));
    final Bytes32 rootHashAtBlock50 = accountTrie.getRootHash();

    final BlockHeader headerAtBlock50 =
        new BlockHeaderTestFixture()
            .number(50L)
            .stateRoot(Hash.wrap(rootHashAtBlock50))
            .buildHeader();
    final BlockHeader chainHeadHeader =
        new BlockHeaderTestFixture().number(CHAIN_HEAD).buildHeader();
    final Blockchain blockchain = mockBlockchain(chainHeadHeader);

    final BonsaiWorldStateKeyValueStorage worldStateStorage = newStorage();
    final SegmentedKeyValueStorage composed = worldStateStorage.getComposedWorldStateStorage();
    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(composed);
    final TrieNodeHistoryReader historyReader = new TrieNodeHistoryReader(historyStore);
    // One strategy instance per block: write history entries at block 50.
    final BonsaiArchiveTrieNodeStrategy archiveStrategy =
        new BonsaiArchiveTrieNodeStrategy(historyReader, historyStore, 50L);
    worldStateStorage.setTrieNodeStrategy(archiveStrategy);

    accountTrie.commit(
        (location, nodeHash, value) -> {
          final SegmentedKeyValueStorageTransaction tx = composed.startTransaction();
          archiveStrategy.putFlatAccountTrieNode(composed, tx, location, nodeHash, value);
          tx.commit();
        });

    // Strategy no longer advances TrieNodeHistoryProgress; save it to storage manually so the
    // provider (which loads progress from disk at construction time) sees block 50 as covered.
    final TrieNodeHistoryProgress progress = new TrieNodeHistoryProgress();
    progress.setLastIndexedBlock(50L);
    progress.setIndexStartBlock(50L);
    final SegmentedKeyValueStorageTransaction progressTx = composed.startTransaction();
    progress.save(progressTx);
    progressTx.commit();

    final BonsaiArchiveWorldStateProvider provider =
        newProvider(worldStateStorage, blockchain, chainHeadHeader);

    final Optional<WorldStateProof> result =
        provider.getAccountProof(headerAtBlock50, address, List.of(), Function.identity());

    assertThat(result).isPresent();
    assertThat(result.get().getStateTrieAccountValue()).contains(accountValue);
    // Hash.hash(...) returns Hash (a BytesHolder wrapper), matching BlockHeader#getStateRoot()'s
    // return type exactly -- do not unwrap with .getBytes() here, since that yields a plain
    // Bytes32 (e.g. ArrayWrappingBytes32) which compares unequal to a Hash despite identical bytes.
    assertThat(Hash.hash(result.get().getAccountProof().getFirst()))
        .isEqualTo(headerAtBlock50.getStateRoot());
  }

  @Test
  void getAccountProofFallsThroughToSuperWhenBlockNotCovered() {
    final BlockHeader chainHeadHeader =
        new BlockHeaderTestFixture().number(CHAIN_HEAD).buildHeader();
    final Blockchain blockchain = mockBlockchain(chainHeadHeader);

    final BonsaiWorldStateKeyValueStorage worldStateStorage = newStorage();
    final SegmentedKeyValueStorage composed = worldStateStorage.getComposedWorldStateStorage();
    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(composed);
    // Archive strategy installed, but no TrieNodeHistoryProgress has been saved to storage:
    // the provider loads an empty progress → covers() is false for every block.
    worldStateStorage.setTrieNodeStrategy(
        new BonsaiArchiveTrieNodeStrategy(
            new TrieNodeHistoryReader(historyStore), historyStore, 0L));

    final BonsaiArchiveWorldStateProvider provider =
        newProvider(worldStateStorage, blockchain, chainHeadHeader);

    // A query far enough in the past that super's own (unmodified) path cannot serve it either:
    // beyond maxLayersToLoad via trie-log rollback, and the archive-migration-progress supplier
    // defaults to -1 (see BonsaiArchiveWorldStateProviderTest's identical
    // "migratorBehindQueryBlock"
    // scenario). getAccountProof must fall through to that existing, already-empty behaviour.
    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));

    final Optional<WorldStateProof> result =
        provider.getAccountProof(
            historicalHeader,
            Address.fromHexString("0x2222222222222222222222222222222222222222"),
            List.of(),
            Function.identity());

    assertThat(result).isEmpty();
  }

  @Test
  void getAccountProofFallsThroughToSuperWhenArchiveStrategyNotInstalled() {
    final BlockHeader chainHeadHeader =
        new BlockHeaderTestFixture().number(CHAIN_HEAD).buildHeader();
    final Blockchain blockchain = mockBlockchain(chainHeadHeader);

    // Plain BonsaiTrieNodeStrategy left in place (the storage's own default) -- flag off, per
    // Task 12 -- so trieHistoryReader/trieHistoryProgress stay null and the override never engages.
    final BonsaiWorldStateKeyValueStorage worldStateStorage = newStorage();

    final BonsaiArchiveWorldStateProvider provider =
        newProvider(worldStateStorage, blockchain, chainHeadHeader);

    final long queryBlockNumber = CHAIN_HEAD - MAX_LAYERS - 1;
    final BlockHeader historicalHeader =
        new BlockHeaderTestFixture().number(queryBlockNumber).buildHeader();
    when(blockchain.getBlockHeader(historicalHeader.getHash()))
        .thenReturn(Optional.of(historicalHeader));

    final Optional<WorldStateProof> result =
        provider.getAccountProof(
            historicalHeader,
            Address.fromHexString("0x3333333333333333333333333333333333333333"),
            List.of(),
            Function.identity());

    assertThat(result).isEmpty();
  }

  private BonsaiWorldStateKeyValueStorage newStorage() {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), archiveConfig());
  }

  private ImmutableDataStorageConfiguration archiveConfig() {
    return ImmutableDataStorageConfiguration.builder()
        .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
        .pathBasedExtraStorageConfiguration(
            ImmutablePathBasedExtraStorageConfiguration.builder()
                .maxLayersToLoad(MAX_LAYERS)
                .build())
        .build();
  }

  private Blockchain mockBlockchain(final BlockHeader chainHeadHeader) {
    final Blockchain blockchain = mock(Blockchain.class);
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadHeader);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(chainHeadHeader.getNumber());
    when(blockchain.getBlockHeader(chainHeadHeader.getHash()))
        .thenReturn(Optional.of(chainHeadHeader));
    return blockchain;
  }

  /**
   * Mirrors {@code BonsaiArchiveWorldStateProviderTest#createProvider}: seeds the chain head block
   * hash so non-historical queries can find the cached head world state, then constructs the
   * provider (which reads {@code worldStateKeyValueStorage.getTrieNodeStrategy()} at construction
   * time, per Task 13's wiring).
   */
  private BonsaiArchiveWorldStateProvider newProvider(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final Blockchain blockchain,
      final BlockHeader chainHeadHeader) {
    final SegmentedKeyValueStorageTransaction tx =
        worldStateStorage.getComposedWorldStateStorage().startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_HASH_KEY,
        chainHeadHeader.getHash().getBytes().toArrayUnsafe());
    tx.commit();

    return new BonsaiArchiveWorldStateProvider(
        worldStateStorage,
        blockchain,
        archiveConfig(),
        null,
        null,
        EvmConfiguration.DEFAULT,
        () -> null,
        new PathBasedCodeCache(),
        new NoOpMetricsSystem());
  }
}
