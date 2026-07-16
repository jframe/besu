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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE_V2;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.RepeatedTest;

/**
 * Identical randomized trie-log sequences (creations, updates, deletions, and re-creations) applied
 * to a fresh {@code StoredMerklePatriciaTrie} directly (the oracle) and to {@link
 * ArchiveTrieBuilder}. Asserts identical account-trie state roots per block and, after all blocks,
 * that every history entry written to {@code TRIE_NODE_HISTORY_ARCHIVE_V2} can be reconstructed via
 * {@link TrieNodeHistoryReader} -- including DIFF entries that arise when a node at depth > 2 is
 * mutated a second time.
 *
 * <p>The oracle is a second, independent {@code StoredMerklePatriciaTrie} built by this test's own
 * minimal apply-and-commit helper. It does not model storage tries; the account values written to
 * both the oracle and {@link ArchiveTrieBuilder} always carry {@code Hash.EMPTY_TRIE_HASH} as
 * storage root, so both engines compute identical account-trie roots regardless of any storage-trie
 * computation in {@link ArchiveTrieBuilder}.
 *
 * <p>This test is the primary correctness guard for Tasks 6-9 (design §8 risk 1 and §9.2).
 */
class ArchiveTrieBuilderDifferentialTest {

  @RepeatedTest(20)
  void archiveTrieBuilderMatchesDirectTrieApplicationAcrossARandomBlockSequence() {
    final long seed = new Random().nextLong();
    final Random random = new Random(seed);
    System.err.printf("ArchiveTrieBuilderDifferentialTest seed=%d%n", seed);
    final List<Address> accounts = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      accounts.add(Address.fromHexString("0x" + "1".repeat(39) + i));
    }

    final SegmentedKeyValueStorage builderStorage = inMemoryHistoryStorage();
    final OracleTrieModel oracle = new OracleTrieModel();
    final ArchiveTrieBuilder builder = new ArchiveTrieBuilder(builderStorage, 0L);

    Hash oracleRoot = Hash.EMPTY_TRIE_HASH;
    for (long block = 1; block <= 30; block++) {
      final TrieLogLayer trieLog = new TrieLogLayer();
      final Address touched = accounts.get(random.nextInt(accounts.size()));
      final boolean delete = oracle.exists(touched) && random.nextInt(5) == 0;

      if (delete) {
        trieLog.addAccountChange(touched, oracle.priorValue(touched), null);
        oracle.delete(touched);
      } else {
        final long nonce = oracle.nonceOf(touched) + 1;
        final AccountValue prior = oracle.priorValue(touched);
        final PmtStateTrieAccountValue updated =
            new PmtStateTrieAccountValue(nonce, Wei.of(nonce), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
        trieLog.addAccountChange(touched, prior, updated);
        final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(random.nextInt(3)));
        trieLog.addStorageChange(touched, slotKey, null, UInt256.valueOf(block));
        oracle.update(touched, updated);
      }
      trieLog.freeze();

      oracleRoot = oracle.apply(trieLog, oracleRoot);
      final var header =
          new BlockHeaderTestFixture().number(block).stateRoot(oracleRoot).buildHeader();
      final var tx = builderStorage.startTransaction();
      builder.applyBlock(trieLog, header, tx);
      tx.commit();

      assertThat(builder.currentAccountRoot())
          .as("account trie root mismatch at block %d", block)
          .isEqualTo(oracleRoot);
    }

    // Second pass: verify history-entry readability via TrieNodeHistoryReader.
    // Checking the root (Bytes.EMPTY) at every block confirms FULL-entry chain integrity.
    // Scanning ALL history entries (both DOMAIN_ACCOUNT and DOMAIN_STORAGE) verifies that
    // DIFF entries are correctly encoded and reconstructable.  DOMAIN_STORAGE entries always
    // have naturalKey.size() >= 32 (accountHash) >> FULL_ABOVE_DEPTH=2, so they are written
    // as DIFF after their first FULL -- making them the primary DIFF-correctness signal.
    // The in-memory HistoryNodeCache means DIFF entries are never used during applyBlock
    // itself, so only an out-of-band read via the reader confirms correctness.
    final TrieNodeHistoryReader reader = new TrieNodeHistoryReader(builderStorage);

    for (long block = 1; block <= 30; block++) {
      assertThat(reader.nodeAt(HistoryKey.DOMAIN_ACCOUNT, Bytes.EMPTY, block))
          .as("account trie root not readable at block %d", block)
          .isPresent();
    }

    builderStorage.stream(TRIE_NODE_HISTORY_ARCHIVE_V2)
        .forEach(
            kv -> {
              final Bytes key = Bytes.wrap(kv.getLeft());
              final byte domain = HistoryKey.domainOf(key);
              final Bytes naturalKey = HistoryKey.naturalKeyOf(key);
              final long block = HistoryKey.blockOf(key);
              assertThat(reader.nodeAt(domain, naturalKey, block))
                  .as(
                      "history entry not reconstructable: domain=%d naturalKey=%s block=%d",
                      domain, naturalKey, block)
                  .isPresent();
            });
  }

  private static SegmentedKeyValueStorage inMemoryHistoryStorage() {
    return new SegmentedInMemoryKeyValueStorage(
        List.of(TRIE_NODE_HISTORY_ARCHIVE_V2, TRIE_BRANCH_STORAGE));
  }

  /**
   * Minimal independent oracle: a bare {@code StoredMerklePatriciaTrie} backed by a local HashMap
   * that applies account mutations from the same trie-log sequence and computes the "true" account
   * state root. Storage tries are not modelled; account values always carry {@code
   * Hash.EMPTY_TRIE_HASH} as their storage root, which matches the values written to the trie logs
   * in the test.
   */
  private static final class OracleTrieModel {
    private final Map<Address, AccountValue> accounts = new HashMap<>();
    private final Map<Bytes, Bytes> trieBacking = new HashMap<>();

    boolean exists(final Address a) {
      return accounts.containsKey(a);
    }

    long nonceOf(final Address a) {
      final AccountValue v = accounts.get(a);
      return v == null ? 0 : v.getNonce();
    }

    AccountValue priorValue(final Address a) {
      return accounts.get(a);
    }

    void update(final Address a, final AccountValue v) {
      accounts.put(a, v);
    }

    void delete(final Address a) {
      accounts.remove(a);
    }

    Hash apply(final TrieLogLayer trieLog, final Hash priorRoot) {
      final StoredMerklePatriciaTrie<Bytes, Bytes> trie =
          new StoredMerklePatriciaTrie<>(
              (location, hash) -> Optional.ofNullable(trieBacking.get(location)),
              Bytes32.wrap(priorRoot.getBytes()),
              Function.identity(),
              Function.identity());

      trieLog
          .getAccountChanges()
          .forEach(
              (address, change) -> {
                final Bytes accountHash = address.addressHash().getBytes();
                if (change.getUpdated() == null) {
                  trie.remove(accountHash);
                } else {
                  trie.put(accountHash, RLP.encode(change.getUpdated()::writeTo));
                }
              });

      trie.commit((location, hash, value) -> trieBacking.put(location, value));
      return Hash.wrap(trie.getRootHash());
    }
  }
}
