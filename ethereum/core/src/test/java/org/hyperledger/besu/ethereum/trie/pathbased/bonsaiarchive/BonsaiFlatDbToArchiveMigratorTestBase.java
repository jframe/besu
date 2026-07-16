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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Shared fixtures for {@link BonsaiFlatDbToArchiveMigrator} tests. Extracted from {@link
 * BonsaiFlatDbToArchiveMigratorTest} so that tests requiring the same storage, blockchain,
 * trie-log-manager, and helper methods ({@link #appendBlocks}, {@link
 * #createMigratorWithRealTrieLogsAndArchiveTrieBuilder}) can extend this class without duplicating
 * setup code.
 */
abstract class BonsaiFlatDbToArchiveMigratorTestBase {

  protected static final Address TEST_ADDRESS =
      Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");
  protected static final long BOUNDARY_DISABLED = 0L;
  protected static final long MIGRATION_TIMEOUT_SECONDS = 10L;

  protected BonsaiWorldStateKeyValueStorage worldStateStorage;
  protected TrieLogManager trieLogManager;
  protected MutableBlockchain blockchain;
  protected SegmentedKeyValueStorage storage;
  protected BlockDataGenerator blockDataGenerator;
  protected final List<BonsaiFlatDbToArchiveMigrator> migrators = new ArrayList<>();

  @BeforeEach
  public void setUp() {
    worldStateStorage = mock(BonsaiWorldStateKeyValueStorage.class);
    trieLogManager = mock(TrieLogManager.class);
    storage = new SegmentedInMemoryKeyValueStorage();
    blockDataGenerator = new BlockDataGenerator();
    blockchain = createInMemoryBlockchain(blockDataGenerator.genesisBlock());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(storage);
    when(trieLogManager.getTrieLogLayer(any()))
        .thenReturn(Optional.of(createAccountTrieLog(Wei.ONE)));
  }

  @AfterEach
  public void tearDown() {
    migrators.forEach(
        m -> {
          try {
            m.close();
          } catch (final Exception ignored) {
            // Ignore exceptions during close
          }
        });
  }

  /**
   * Appends {@code count} blocks to the test blockchain. Each block carries a state root matching
   * {@link #computeTestAccountStateRoot()} so that {@link ArchiveTrieBuilder}-enabled migrators do
   * not throw a state-root mismatch when replaying the default {@code
   * createAccountTrieLog(Wei.ONE)} trie log. Applying that trie log any number of times is
   * idempotent: {@code TEST_ADDRESS} at balance=1 always produces the same MPT root, so every block
   * in the sequence has an identical, correct state root.
   */
  protected void appendBlocks(final int count) {
    final Hash stateRoot = computeTestAccountStateRoot();
    Block head = blockchain.getBlockByNumber(blockchain.getChainHeadBlockNumber()).get();
    for (int i = 0; i < count; i++) {
      final Block block =
          blockDataGenerator.block(
              BlockDataGenerator.BlockOptions.create()
                  .setParentHash(head.getHash())
                  .setBlockNumber(head.getHeader().getNumber() + 1)
                  .setStateRoot(stateRoot));
      blockchain.appendBlock(block, blockDataGenerator.receipts(block));
      head = block;
    }
  }

  /**
   * Creates a migrator with {@link ArchiveTrieBuilder} enabled (trie-node history capture on),
   * configured so that genesis (block 0) receives an empty trie log (no account changes) and all
   * subsequent blocks receive the default account trie log ({@link
   * #createAccountTrieLog(Wei.ONE)}).
   */
  protected BonsaiFlatDbToArchiveMigrator createMigratorWithRealTrieLogsAndArchiveTrieBuilder() {
    when(trieLogManager.getMaxLayersToLoad()).thenReturn(BOUNDARY_DISABLED);
    // Genesis must have an empty trie log so ArchiveTrieBuilder starts at EMPTY_TRIE_HASH and the
    // root check passes (applying no changes leaves the root unchanged).
    when(trieLogManager.getTrieLogLayer(hashAt(0L))).thenReturn(Optional.of(new TrieLogLayer()));
    final BonsaiFlatDbToArchiveMigrator migrator =
        new BonsaiFlatDbToArchiveMigrator(
            worldStateStorage, blockchain, trieLogManager, /* trieNodeHistoryEnabled= */ true);
    migrators.add(migrator);
    return migrator;
  }

  protected MutableBlockchain createInMemoryBlockchain(final Block genesisBlock) {
    return DefaultBlockchain.createMutable(
        genesisBlock,
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false),
        new NoOpMetricsSystem(),
        0);
  }

  protected Hash hashAt(final long blockNumber) {
    return blockchain.getBlockHeader(blockNumber).orElseThrow().getHash();
  }

  protected TrieLogLayer createAccountTrieLog(final Wei balance) {
    final TrieLogLayer trieLog = new TrieLogLayer();
    final PmtStateTrieAccountValue value =
        new PmtStateTrieAccountValue(1, balance, Hash.EMPTY, Hash.EMPTY);
    trieLog.addAccountChange(TEST_ADDRESS, null, value);
    return trieLog;
  }

  /**
   * Computes the MPT state root for a world state containing only {@code TEST_ADDRESS} with
   * balance=1, matching the account created by {@code createAccountTrieLog(Wei.ONE)}.
   */
  protected Hash computeTestAccountStateRoot() {
    return computeAccountStateRoot(Wei.ONE);
  }

  protected Hash computeAccountStateRoot(final Wei balance) {
    final PmtStateTrieAccountValue account =
        new PmtStateTrieAccountValue(1, balance, Hash.EMPTY, Hash.EMPTY);
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    account.writeTo(out);
    final SimpleMerklePatriciaTrie<Bytes, Bytes> trie =
        new SimpleMerklePatriciaTrie<>(Function.identity());
    trie.put(TEST_ADDRESS.addressHash().getBytes(), out.encoded());
    return Hash.wrap(trie.getRootHash());
  }
}
