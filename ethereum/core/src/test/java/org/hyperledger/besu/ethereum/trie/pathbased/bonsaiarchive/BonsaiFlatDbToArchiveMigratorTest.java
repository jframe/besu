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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_FRONTIER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy.calculateNaturalSlotKey;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeChangeIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeIndexProgress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BonsaiFlatDbToArchiveMigratorTest {

  private static final Address TEST_ADDRESS =
      Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");
  private static final long BOUNDARY_DISABLED = 0L;
  private static final long MIGRATION_TIMEOUT_SECONDS = 10L;
  private static final long AWAIT_TIMEOUT_SECONDS = 5L;

  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;
  @Mock private TrieLogManager trieLogManager;
  private MutableBlockchain blockchain;
  private SegmentedKeyValueStorage storage;
  private BlockDataGenerator blockDataGenerator;
  private final List<BonsaiFlatDbToArchiveMigrator> migrators = new ArrayList<>();

  @BeforeEach
  public void setup() {
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

  @Test
  public void migratesAccountChangesFromTrieLogs() throws Exception {
    appendBlocks(2);

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(getArchivedAccountKey(1L)).isPresent();
    assertThat(getArchivedAccountKey(2L)).isPresent();
  }

  @Test
  public void migratesStorageChangesFromTrieLogs() throws Exception {
    appendBlocks(1);
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final TrieLogLayer trieLog = new TrieLogLayer();
    trieLog.addStorageChange(TEST_ADDRESS, slotKey, UInt256.ZERO, UInt256.valueOf(42));
    when(trieLogManager.getTrieLogLayer(hashAt(1L))).thenReturn(Optional.of(trieLog));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(getArchivedStorageKey(1L, slotKey)).isPresent();
  }

  @Test
  public void futureCompletesExceptionallyOnFailure() {
    appendBlocks(1);

    when(trieLogManager.getTrieLogLayer(any())).thenThrow(new RuntimeException("Test failure"));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();

    assertThat(migrator.migrate())
        .failsWithin(1, TimeUnit.SECONDS)
        .withThrowableThat()
        .havingRootCause()
        .withMessage("Test failure");
  }

  @Test
  public void rejectsConcurrentMigrations() throws Exception {
    appendBlocks(1);
    final PausedMigration paused = pauseAtTrieLogLookup(hashAt(1L));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    final CompletableFuture<Void> first = migrator.migrate();

    paused.awaitStart();
    assertThat(migrator.migrationRunning).isTrue();

    // Second migration should return immediately without running
    final CompletableFuture<Void> second = migrator.migrate();
    second.get(1, TimeUnit.SECONDS);

    // Second migration must not have interacted with the database
    verify(trieLogManager).getTrieLogLayer(hashAt(1L));

    paused.release();
    first.get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(migrator.migrationRunning).isFalse();
  }

  @Test
  public void tracksRunningState() throws Exception {
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    assertThat(migrator.migrationRunning).isFalse();

    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(migrator.migrationRunning).isFalse();
  }

  @Test
  public void savesProgressToStorage() throws Exception {
    appendBlocks(5);

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    assertThat(migrator.getMigrationProgress()).isEmpty();

    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(migrator.getMigrationProgress()).hasValue(5L);
  }

  @Test
  public void failsMigrationWhenTrieLogIsMissing() {
    appendBlocks(1);
    when(trieLogManager.getTrieLogLayer(hashAt(1L))).thenReturn(Optional.empty());

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();

    assertThat(migrator.migrate())
        .failsWithin(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .withThrowableThat()
        .havingRootCause()
        .withMessage("No trie log found for block 1");
    verify(worldStateStorage, never()).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void migratesNewBlocksAddedDuringMigration() throws Exception {
    appendBlocks(2);
    final PausedMigration paused = pauseAtTrieLogLookup(hashAt(1L));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    final CompletableFuture<Void> future = migrator.migrate();

    paused.awaitStart();

    // Append block 3 while migration is paused — target should update to cover it
    appendBlocks(1);

    paused.release();
    future.get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(getArchivedAccountKey(1L)).isPresent();
    assertThat(getArchivedAccountKey(2L)).isPresent();
    assertThat(getArchivedAccountKey(3L)).isPresent();
  }

  @Test
  public void switchesToArchiveModeOnCompletion() throws Exception {
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    verify(worldStateStorage).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void resumesFromNextBlockAfterSavedProgress() throws Exception {
    appendBlocks(3);

    // Run first migration over blocks 1-3
    final BonsaiFlatDbToArchiveMigrator firstMigrator = createMigrator();
    firstMigrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(firstMigrator.getMigrationProgress()).hasValue(3L);
    firstMigrator.close(); // simulate node restart — deregisters ongoing observer

    // Append a new block and run a second migrator (simulating a restart)
    appendBlocks(1);

    final BonsaiFlatDbToArchiveMigrator secondMigrator = createMigrator();
    secondMigrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Blocks 1-3 must not be re-processed — each queried exactly once across both migrations
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(1L));
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(2L));
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(3L));
    // Block 4 must be processed by the second migration
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(4L));
  }

  @Test
  public void usesLowPriorityTransactionsForMigration() throws Exception {
    appendBlocks(1);
    final SegmentedInMemoryKeyValueStorage spyStorage = spy(new SegmentedInMemoryKeyValueStorage());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(spyStorage);

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    verify(spyStorage, atLeastOnce()).startLowPriorityTransaction();
  }

  // -------------------------------------------------------------------------
  // Batch overlay: writes within a batch are visible to subsequent gets without commit.
  // -------------------------------------------------------------------------

  @Test
  public void migrationTrieStorageReadsSeeWritesWithinBatch() {
    final SegmentedKeyValueStorage real = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorageTransaction sharedTx =
        mock(SegmentedKeyValueStorageTransaction.class);
    final BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage trieStorage =
        new BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage(real);

    trieStorage.beginBatch(sharedTx);
    final SegmentedKeyValueStorageTransaction tx = trieStorage.startTransaction();

    final byte[] key = Bytes.fromHexString("0x1234").toArrayUnsafe();
    final byte[] node = Bytes.fromHexString("0xabcd").toArrayUnsafe();
    tx.put(TRIE_BRANCH_STORAGE, key, node);

    // get must return the just-written value from the overlay, without touching real.
    assertThat(trieStorage.get(TRIE_BRANCH_STORAGE, key)).contains(node);
    verify(real, never()).get(eq(TRIE_BRANCH_FRONTIER), eq(key));
    verify(real, never()).get(eq(TRIE_BRANCH_STORAGE), eq(key));

    // a metadata key written in the batch is also visible
    final byte[] worldBlockVal = Bytes.ofUnsignedLong(42).toArrayUnsafe();
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, worldBlockVal);
    assertThat(trieStorage.get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY))
        .contains(worldBlockVal);

    trieStorage.endBatch();
  }

  // -------------------------------------------------------------------------
  // Crash-safety: persist() shares the migrator's per-block transaction so the
  // frontier, diff-index, flat state and progress commit atomically.
  // -------------------------------------------------------------------------

  @Test
  public void migrationTrieStorageRoutesWritesToSharedTransactionAndDefersCommit() {
    final SegmentedKeyValueStorage real = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorageTransaction sharedTx =
        mock(SegmentedKeyValueStorageTransaction.class);
    final BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage trieStorage =
        new BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage(real);

    trieStorage.beginBatch(sharedTx);
    final SegmentedKeyValueStorageTransaction tx = trieStorage.startTransaction();

    final byte[] key = Bytes.fromHexString("0x1234").toArrayUnsafe();
    final byte[] node = Bytes.fromHexString("0xabcd").toArrayUnsafe();
    tx.put(TRIE_BRANCH_STORAGE, key, node);

    // Writes go into the shared transaction (TRIE_BRANCH_STORAGE redirected to FRONTIER); no
    // separate real transaction is opened by persist().
    verify(sharedTx).put(eq(TRIE_BRANCH_FRONTIER), eq(key), eq(node));
    verify(real, never()).startLowPriorityTransaction();

    // commit()/rollback() are deferred to the migrator — the shared transaction is left untouched
    // so the whole block commits exactly once, atomically.
    tx.commit();
    tx.rollback();
    verify(sharedTx, never()).commit();
    verify(sharedTx, never()).rollback();
  }

  @Test
  public void migrationTrieStorageOwnsItsTransactionWhenNoSharedTransactionSet() {
    final SegmentedKeyValueStorage real = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorageTransaction ownTx =
        mock(SegmentedKeyValueStorageTransaction.class);
    when(real.startLowPriorityTransaction()).thenReturn(ownTx);
    final BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage trieStorage =
        new BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage(real);

    // No shared transaction (e.g. recovery replay) → opens and commits its own low-priority tx.
    final SegmentedKeyValueStorageTransaction tx = trieStorage.startTransaction();
    tx.commit();

    verify(real).startLowPriorityTransaction();
    verify(ownTx).commit();
  }

  @Test
  public void migrationTrieStorageServesRepeatedTrieBranchReadsFromPerBlockCache() {
    // Within a block the put/commit walk and the diff-index prior-node capture read the same trie
    // node location from TRIE_BRANCH_STORAGE. The per-block read cache must collapse these to a
    // single underlying read, and a block boundary (resetBlockCache) must re-read.
    final SegmentedKeyValueStorage real = mock(SegmentedKeyValueStorage.class);
    final byte[] key = Bytes.fromHexString("0x0102").toArrayUnsafe();
    final byte[] value = Bytes.fromHexString("0xdeadbeef").toArrayUnsafe();
    when(real.get(eq(TRIE_BRANCH_FRONTIER), any())).thenReturn(Optional.empty());
    when(real.get(eq(TRIE_BRANCH_STORAGE), any())).thenReturn(Optional.of(value));

    final BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage trieStorage =
        new BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage(real);

    assertThat(trieStorage.get(TRIE_BRANCH_STORAGE, key)).contains(value);
    assertThat(trieStorage.get(TRIE_BRANCH_STORAGE, key)).contains(value);

    // Both the frontier probe and the storage fallthrough happen exactly once for the two reads.
    verify(real, times(1)).get(eq(TRIE_BRANCH_FRONTIER), any());
    verify(real, times(1)).get(eq(TRIE_BRANCH_STORAGE), any());

    // New block: cache cleared, so the next read goes back to the underlying storage.
    trieStorage.resetBlockCache();
    assertThat(trieStorage.get(TRIE_BRANCH_STORAGE, key)).contains(value);
    verify(real, times(2)).get(eq(TRIE_BRANCH_FRONTIER), any());
    verify(real, times(2)).get(eq(TRIE_BRANCH_STORAGE), any());
  }

  @Test
  public void migrationTrieStorageCachesFrontierHitsAndTombstonesWithinBlock() {
    final SegmentedKeyValueStorage real = mock(SegmentedKeyValueStorage.class);
    final byte[] presentKey = Bytes.fromHexString("0x0a").toArrayUnsafe();
    final byte[] presentVal = Bytes.fromHexString("0xc0ffee").toArrayUnsafe();
    final byte[] deletedKey = Bytes.fromHexString("0x0b").toArrayUnsafe();
    when(real.get(eq(TRIE_BRANCH_FRONTIER), eq(presentKey))).thenReturn(Optional.of(presentVal));
    // Zero-length frontier sentinel = explicitly deleted node -> resolves to empty, no fallthrough.
    when(real.get(eq(TRIE_BRANCH_FRONTIER), eq(deletedKey))).thenReturn(Optional.of(new byte[0]));

    final BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage trieStorage =
        new BonsaiFlatDbToArchiveMigrator.MigrationTrieStorage(real);

    assertThat(trieStorage.get(TRIE_BRANCH_STORAGE, presentKey)).contains(presentVal);
    assertThat(trieStorage.get(TRIE_BRANCH_STORAGE, presentKey)).contains(presentVal);
    assertThat(trieStorage.get(TRIE_BRANCH_STORAGE, deletedKey)).isEmpty();
    assertThat(trieStorage.get(TRIE_BRANCH_STORAGE, deletedKey)).isEmpty();

    // One frontier read per distinct key; the deleted sentinel never falls through to storage.
    verify(real, times(1)).get(eq(TRIE_BRANCH_FRONTIER), eq(presentKey));
    verify(real, times(1)).get(eq(TRIE_BRANCH_FRONTIER), eq(deletedKey));
    verify(real, never()).get(eq(TRIE_BRANCH_STORAGE), any());
  }

  @Test
  public void doesNotSwitchToArchiveModeOnFailure() {
    appendBlocks(1);
    when(trieLogManager.getTrieLogLayer(any())).thenThrow(new RuntimeException("Test failure"));
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();

    assertThat(migrator.migrate()).failsWithin(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    verify(worldStateStorage, never()).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void migrationStopsAtHeadMinusBoundaryDistance() throws Exception {
    // head=5, boundaryDistance=3 → target = 5-3 = 2; blocks 1 and 2 migrated, 3 not
    appendBlocks(5);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(getArchivedAccountKey(2L)).isPresent();
    assertThat(getArchivedAccountKey(3L)).isEmpty();
  }

  @Test
  public void migrateHandsOffObserversFromInitialToOngoingWithoutGap() throws Exception {
    appendBlocks(3);
    final MutableBlockchain spyBlockchain = spy(blockchain);
    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigrator(spyBlockchain, /*boundaryDistance*/ 1);

    final AtomicBoolean injected = new AtomicBoolean(false);
    doAnswer(
            invocation -> {
              final Object result = invocation.callRealMethod();
              if (injected.compareAndSet(false, true)) {
                // Now the old observer has just been removed. Fire a canonical head event.
                appendBlocks(1);
              }
              return result;
            })
        .when(spyBlockchain)
        .removeObserver(anyLong());

    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // head after the injected block = 4; boundaryDistance=1 → archiveTarget = 3.
    // Block 3 must be migrated via catch-up triggered by the injected block 4 event.
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(3L)).isPresent());
  }

  @Test
  public void migrateObserverIgnoresForkEvents() throws Exception {
    appendBlocks(5); // canonical head = 5
    final MutableBlockchain spyBlockchain = spy(blockchain);
    final PausedMigration paused = pauseAtAnyTrieLogLookup();

    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigrator(spyBlockchain, /*boundaryDistance*/ 2);
    final CompletableFuture<Void> future = migrator.migrate();

    final ArgumentCaptor<BlockAddedObserver> captor =
        ArgumentCaptor.forClass(BlockAddedObserver.class);
    verify(spyBlockchain, atLeastOnce()).observeBlockAdded(captor.capture());
    final BlockAddedObserver migrateObserver = captor.getAllValues().get(0);

    paused.awaitStart();

    // Fire a FORK event at block height 1
    final Block forkBlock =
        blockDataGenerator.block(BlockDataGenerator.BlockOptions.create().setBlockNumber(1L));
    migrateObserver.onBlockAdded(BlockAddedEvent.createForFork(forkBlock));

    paused.release();
    future.get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // head=5, boundaryDistance=2 → canonical target = 3; block 3 must still be migrated.
    assertThat(getArchivedAccountKey(3L)).isPresent();
  }

  @Test
  public void closeDuringMigrationInterruptsAndSkipsArchiveUpgrade() throws Exception {
    appendBlocks(3);
    final PausedMigration paused = pauseAtAnyTrieLogLookup();

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(BOUNDARY_DISABLED);
    final CompletableFuture<Void> future = migrator.migrate();

    paused.awaitStart();
    assertThat(migrator.blockObserverId).isPresent();

    migrator.close();

    assertThatThrownBy(() -> future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class);
    assertThat(migrator.blockObserverId).isEmpty();
    assertThat(migrator.migrationRunning.get()).isFalse();
    verify(worldStateStorage, never()).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void blockObserverPersistsAndMigratesBlockAtBoundary() throws Exception {
    // head=3, boundaryDistance=3 → initial target=0, nothing migrated initially
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(getArchivedAccountKey(1L)).isEmpty();

    // block 4 arrives → observer submits migration of block 4-3=1
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(1L)).isPresent());
  }

  @Test
  public void startOngoingMigrationRegistersObserverAndMigratesBlocks() {
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    assertThat(migrator.blockObserverId).isEmpty();

    migrator.startOngoingMigration();
    assertThat(migrator.blockObserverId).isPresent();

    // block 4 arrives → observer migrates block 4-3=1
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(1L)).isPresent());
  }

  @Test
  public void ongoingMigrationCatchesUpMultipleBlocksBehind() throws Exception {
    // head=3, boundaryDistance=1 → initial target=2; migrate blocks 1 and 2
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(1);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(migrator.getMigrationProgress()).hasValue(2L);

    // blocks 4 and 5 arrive while executor is idle; block 6 triggers catch-up of 4-1=3 up to 5-1=4
    appendBlocks(3);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(3L)).isPresent());
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(4L)).isPresent());
  }

  @Test
  public void ongoingMigrationCoalescesBurstOfHeadEventsIntoOneSubmit() throws Exception {
    // boundaryDistance=1 → target = head - 1
    when(trieLogManager.getMaxLayersToLoad()).thenReturn(1L);
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final ScheduledExecutorService spyExecutor = spy(Executors.newScheduledThreadPool(1));
    final BonsaiFlatDbToArchiveMigrator migrator =
        new BonsaiFlatDbToArchiveMigrator(
            worldStateStorage,
            trieLogManager,
            blockchain,
            spyExecutor,
            metricsSystem,
            new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy()));
    migrators.add(migrator);
    migrator.startOngoingMigration();

    final PausedMigration pause = pauseAtAnyTrieLogLookup();
    // head=2 → target=1: triggers a single drain submission, paused at block 1's trie-log lookup.
    appendBlocks(2);
    pause.awaitStart();

    // Burst of head events while drain is paused. Each bumps ongoingTarget; the single-flight
    // CAS prevents additional submissions.
    appendBlocks(20);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(migrator.ongoingTarget.get()).isEqualTo(21L));
    assertThat(migrator.catchUpRunning.get()).isTrue();
    verify(spyExecutor, times(1)).submit(any(Runnable.class));

    // Release: the same in-flight drain reads the live target and walks all the way to block 21.
    pause.release();
    Awaitility.await()
        .atMost(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(migrator.migratedBlockNumber.get()).isEqualTo(21L));
    // No additional submissions after the burst — the moving target absorbed everything.
    verify(spyExecutor, times(1)).submit(any(Runnable.class));
  }

  @Test
  public void ongoingMigrationUpdatesMetric() throws Exception {
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(1);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    final long metricAfterMigration = migrator.migratedBlockNumber.get();

    // block 4 arrives → observer migrates block 3; metric should increment
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(migrator.migratedBlockNumber.get()).isGreaterThan(metricAfterMigration));
  }

  @Test
  public void startOngoingMigrationAfterCloseIsNoOp() {
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    migrator.close();

    migrator.startOngoingMigration();
    assertThat(migrator.blockObserverId).isEmpty();
  }

  @Test
  public void migrateAfterCloseIsNoOp() throws Exception {
    appendBlocks(3);
    final MutableBlockchain spyBlockchain = spy(blockchain);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(spyBlockchain, BOUNDARY_DISABLED);
    migrator.close();

    final CompletableFuture<Void> future = migrator.migrate();

    assertThat(future).isCompletedWithValue(null);
    assertThat(migrator.blockObserverId).isEmpty();
    assertThat(migrator.migrationRunning.get()).isFalse();
    verify(spyBlockchain, never()).observeBlockAdded(any());
  }

  @Test
  public void startOngoingMigrationIsIdempotent() {
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);

    migrator.startOngoingMigration();
    final OptionalLong firstId = migrator.blockObserverId;
    assertThat(firstId).isPresent();

    migrator.startOngoingMigration(); // second call — should no-op
    assertThat(migrator.blockObserverId).isEqualTo(firstId);
  }

  @Test
  public void startOngoingMigrationInitializesMetricFromSavedProgress() throws Exception {
    appendBlocks(5);
    final BonsaiFlatDbToArchiveMigrator firstMigrator = createMigrator(BOUNDARY_DISABLED);
    firstMigrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    firstMigrator.close();

    // Second migrator simulates a restart — metric should be initialized from saved progress
    final BonsaiFlatDbToArchiveMigrator secondMigrator = createMigrator(BOUNDARY_DISABLED);
    assertThat(secondMigrator.migratedBlockNumber.get()).isEqualTo(0L);
    secondMigrator.startOngoingMigration();
    assertThat(secondMigrator.migratedBlockNumber.get()).isEqualTo(5L);
    secondMigrator.close();
  }

  // --- trie-node index tests ---

  /**
   * Task 5.1: migrating blocks with the trie-node index enabled populates the differential index so
   * that {@link TrieNodeHistoryReader#nodeAt} returns the root trie node whose keccak256 matches
   * block 1's {@code stateRoot}.
   *
   * <p>In the per-block-persist design, every block is persisted in sequence. Block 0 (empty trie)
   * is persisted first, committing {@code WORLD_BLOCK_NUMBER_KEY=0} to the in-memory layer. When
   * block 1 is persisted, {@code putFlatAccountTrieNode} is called for each trie node including the
   * root (location = empty bytes). The root node location has size ≤ {@code FULL_ABOVE_DEPTH=2}, so
   * it is always stored as a FULL codec entry. The strategy reads {@code WORLD_BLOCK_NUMBER_KEY=0}
   * from the committed layer (before the transaction commits) and adds 1, indexing the root node at
   * block 1. {@code nodeAt(Bytes.EMPTY, 1)} resolves directly to this entry.
   */
  @Test
  public void trieMigratorWithIndexEnabled_populatesDiffIndexAtCheckpoint() throws Exception {
    // Set up block 1 with the correct stateRoot so persist() does not throw a mismatch error.
    final Hash stateRoot = computeTestAccountStateRoot();
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    // Build index components backed by the same storage as the migrator.
    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(storage);
    final TrieNodeChangeIndex changeIndex =
        new TrieNodeChangeIndex(storage, ArchiveNodeKey.RANGE_SIZE);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);

    // Run migration with index enabled (interval=2 → checkpoint at block 1).
    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // In the per-block-persist design every block is persisted, so WORLD_BLOCK_NUMBER_KEY=0 is in
    // the in-memory layer when block 1's trie nodes are written. getCurrentBlockNumber() reads the
    // committed layer value (0) and adds 1, indexing block 1's root node at block 1.

    // Structural assertion: the raw history entry at block 1 must be present.
    // This verifies that MigrationTransaction is actually routing TRIE_NODE_HISTORY_ARCHIVE
    // writes to realTx (persistent storage) rather than silently dropping them.
    assertThat(historyStore.get(Bytes.EMPTY, 1L))
        .withFailMessage(
            "History store should contain a diff entry for root key at block 1 after migration")
        .isPresent();

    // Semantic assertion: the history reader reconstructs the node and its keccak matches
    // stateRoot.
    final TrieNodeHistoryReader reader = new TrieNodeHistoryReader(historyStore, changeIndex);
    final Optional<Bytes> rootNodeOpt = reader.nodeAt(Bytes.EMPTY, 1);

    assertThat(rootNodeOpt)
        .withFailMessage("Root trie node should be present in the diff index after migration")
        .isPresent();

    // The keccak256 of the root node RLP must equal the block's stateRoot.
    final Hash computedRoot = Hash.hash(rootNodeOpt.get());
    assertThat(computedRoot)
        .withFailMessage(
            "Root node keccak does not match block 1 stateRoot: expected %s got %s",
            stateRoot, computedRoot)
        .isEqualTo(stateRoot);
  }

  // --- Task 5.2: indexStartBlock / lastIndexedBlock / covers tests ---

  /**
   * Task 5.2 (core): after migrating block 1, the migrator must have advanced {@code
   * migrationIndexProgress} such that:
   *
   * <ul>
   *   <li>{@code lastIndexedBlock() == 1}
   *   <li>{@code indexStartBlock() == 0} (range 0 starts at block 0)
   *   <li>{@code covers(0)} and {@code covers(1)} are true; {@code covers(2)} is false
   * </ul>
   *
   * <p>In the per-block-persist design both blocks 0 and 1 are persisted. {@code
   * flushIndexIfEnabled()} advances progress to each block during its {@code persist()} call.
   * {@code indexStartBlock} is computed using {@code ArchiveNodeKey.RANGE_SIZE} (1 000 000), so
   * both blocks fall in range 0 and {@code covers()} uses the window [{@code indexStartBlock=0},
   * {@code lastIndexedBlock=1}].
   */
  @Test
  public void trieMigratorWithIndexEnabled_advancesIndexProgressAtCheckpoint() throws Exception {
    final Hash stateRoot = computeTestAccountStateRoot();
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(storage);
    final TrieNodeChangeIndex changeIndex = new TrieNodeChangeIndex(storage, 2L);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(2L);

    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // lastIndexedBlock advances to the checkpoint block.
    assertThat(progress.lastIndexedBlock())
        .as("lastIndexedBlock should be 1 after migrating block 1")
        .isEqualTo(1L);

    // indexStartBlock extends downward to the start of the range containing block 1 (range 0 →
    // start = 0).
    assertThat(progress.indexStartBlock())
        .as("indexStartBlock should be 0 (start of range 0)")
        .isEqualTo(0L);

    // Block 1 is the last block of range 0 (rangeSize=2, (1+1)%2==0) → range 0 is complete.
    assertThat(progress.covers(0L))
        .as("covers(0) should be true — block 0 is in the completed range 0")
        .isTrue();
    assertThat(progress.covers(1L))
        .as("covers(1) should be true — block 1 is in the completed range 0")
        .isTrue();
    // Range 1 (blocks 2-3) has never been indexed → not complete.
    assertThat(progress.covers(2L))
        .as("covers(2) should be false — range 1 has not been indexed")
        .isFalse();
  }

  /**
   * Task 5.2 (partial range): after migrating block 1, when block 1 is NOT the last block of its
   * range (rangeSize=4: blocks 0–3), {@code lastIndexedBlock} is still 1 and {@code covers(1)} is
   * true (window-check semantics: any block in [indexStartBlock, lastIndexedBlock] is serveable).
   */
  @Test
  public void trieMigratorWithIndexEnabled_indexProgressPartialRange() throws Exception {
    final Hash stateRoot = computeTestAccountStateRoot();
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(storage);
    final TrieNodeChangeIndex changeIndex = new TrieNodeChangeIndex(storage, 4L);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(4L);

    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(progress.lastIndexedBlock())
        .as("lastIndexedBlock should advance to block 1")
        .isEqualTo(1L);

    assertThat(progress.indexStartBlock())
        .as("indexStartBlock should be 0 (start of range 0)")
        .isEqualTo(0L);

    // Block 1 is within [indexStartBlock=0, lastIndexedBlock=1] — covers() returns true.
    assertThat(progress.covers(1L))
        .as("covers(1) should be true — block 1 is within the indexed window")
        .isTrue();
    // Block 2 is beyond lastIndexedBlock — not yet covered.
    assertThat(progress.covers(2L))
        .as("covers(2) should be false — block 2 is beyond lastIndexedBlock")
        .isFalse();
  }

  /**
   * Task 5.2 (parallel range independence): two simulated range workers building independent
   * TrieNodeIndexProgress instances over disjoint block ranges do not interfere with each other.
   *
   * <p>Worker A processes blocks 0-1 (range 0 with rangeSize=2). Worker B processes blocks 2-3
   * (range 1 with rangeSize=2). After both complete:
   *
   * <ul>
   *   <li>Worker A: {@code covers(0)} and {@code covers(1)} are true; {@code covers(2)} is false.
   *   <li>Worker B: {@code covers(2)} and {@code covers(3)} are true; {@code covers(0)} is false.
   * </ul>
   *
   * <p>Since the CF keys are keyed by (naturalKey, rangeId), workers writing to different rangeIds
   * never collide. The bloom CF is keyed by rangeId alone, so different ranges also have distinct
   * bloom keys.
   */
  @Test
  public void trieMigratorWithIndexEnabled_parallelRangesAreIndependent() throws Exception {
    // Tests data isolation (separate TrieNodeIndexProgress objects per range), not thread-safety.
    // Worker A: range 0 with rangeSize=2 — processes block 1 as the checkpoint.
    // (Block 0 = genesis with empty trie log; block 1 has the account state root.)
    final Hash stateRoot = computeTestAccountStateRoot();
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    // Progress A covers [0, 1] (window check: indexStartBlock=0, lastIndexedBlock=1).
    final TrieNodeIndexProgress progressA = new TrieNodeIndexProgress(2L);
    progressA.setLastIndexedBlock(1L);
    progressA.setIndexStartBlock(0L);

    // Progress B covers [2, 3] (window check: indexStartBlock=2, lastIndexedBlock=3).
    final TrieNodeIndexProgress progressB = new TrieNodeIndexProgress(2L);
    progressB.setLastIndexedBlock(3L);
    progressB.setIndexStartBlock(2L);

    // Worker A covers blocks 0-1 but NOT 2-3.
    assertThat(progressA.covers(0L)).as("A covers block 0").isTrue();
    assertThat(progressA.covers(1L)).as("A covers block 1").isTrue();
    assertThat(progressA.covers(2L)).as("A does not cover block 2 (range 1)").isFalse();
    assertThat(progressA.covers(3L)).as("A does not cover block 3 (range 1)").isFalse();

    // Worker B covers blocks 2-3 but NOT 0-1.
    assertThat(progressB.covers(0L)).as("B does not cover block 0 (range 0)").isFalse();
    assertThat(progressB.covers(1L)).as("B does not cover block 1 (range 0)").isFalse();
    assertThat(progressB.covers(2L)).as("B covers block 2").isTrue();
    assertThat(progressB.covers(3L)).as("B covers block 3").isTrue();

    // indexStartBlock is monotonically non-increasing within each worker.
    assertThat(progressA.indexStartBlock()).as("A indexStartBlock").isEqualTo(0L);
    assertThat(progressB.indexStartBlock()).as("B indexStartBlock").isEqualTo(2L);

    // Neither worker's progress affects the other: marking range 0 in A has no effect on B,
    // and marking range 1 in B has no effect on A.
    assertThat(progressA.covers(2L))
        .as("A still does not cover range 1 after B marks it")
        .isFalse();
    assertThat(progressB.covers(0L))
        .as("B still does not cover range 0 after A marks it")
        .isFalse();
  }

  // --- frontier CF tests ---

  /**
   * After index-mode migration, trie node metadata (WORLD_ROOT_HASH_KEY) must be present in
   * TRIE_BRANCH_FRONTIER. This verifies that MigrationTransaction routes TRIE_BRANCH_STORAGE writes
   * to the persistent frontier CF rather than an in-memory layer.
   */
  @Test
  public void trieMigratorWithIndexEnabled_writesMetadataToCFrontier() throws Exception {
    final Hash stateRoot = computeTestAccountStateRoot();
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(storage);
    final TrieNodeChangeIndex changeIndex =
        new TrieNodeChangeIndex(storage, ArchiveNodeKey.RANGE_SIZE);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);

    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(storage.get(TRIE_BRANCH_FRONTIER, WORLD_ROOT_HASH_KEY))
        .as("WORLD_ROOT_HASH_KEY must be written to TRIE_BRANCH_FRONTIER by MigrationTransaction")
        .isPresent();
    assertThat(storage.stream(TRIE_BRANCH_FRONTIER).findAny())
        .as("TRIE_BRANCH_FRONTIER must contain trie nodes after migration")
        .isPresent();
  }

  /**
   * Metadata keys written via MigrationTransaction to TRIE_BRANCH_FRONTIER must NOT be present in
   * live TRIE_BRANCH_STORAGE — migration writes must not leak into live HEAD storage.
   */
  @Test
  public void migrationTrieStorage_metadataKeyDoesNotFallThroughToLiveStorage() throws Exception {
    final Hash stateRoot = computeTestAccountStateRoot();
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(storage);
    final TrieNodeChangeIndex changeIndex =
        new TrieNodeChangeIndex(storage, ArchiveNodeKey.RANGE_SIZE);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);

    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(storage.get(TRIE_BRANCH_FRONTIER, WORLD_ROOT_HASH_KEY))
        .as("WORLD_ROOT_HASH_KEY should be in TRIE_BRANCH_FRONTIER")
        .isPresent();
    assertThat(storage.get(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY))
        .as("WORLD_ROOT_HASH_KEY must NOT be in live TRIE_BRANCH_STORAGE")
        .isEmpty();
  }

  /**
   * The zero-byte tombstone sentinel written by MigrationTransaction.remove() must be present in
   * TRIE_BRANCH_FRONTIER, and the live TRIE_BRANCH_STORAGE value must be unaffected.
   */
  @Test
  public void migrationTrieStorage_tombstonePreventsLiveFallthrough() {
    final byte[] nodeKey = new byte[] {0x01, 0x02, 0x03};
    final byte[] liveValue = new byte[] {0x10, 0x20, 0x30};

    final var liveTx = storage.startTransaction();
    liveTx.put(TRIE_BRANCH_STORAGE, nodeKey, liveValue);
    liveTx.commit();

    assertThat(storage.get(TRIE_BRANCH_STORAGE, nodeKey)).hasValue(liveValue);

    // Simulate what MigrationTransaction.remove() does: write FRONTIER_TOMBSTONE = new byte[0]
    final var frontierTx = storage.startTransaction();
    frontierTx.put(TRIE_BRANCH_FRONTIER, nodeKey, new byte[0]);
    frontierTx.commit();

    assertThat(storage.get(TRIE_BRANCH_FRONTIER, nodeKey))
        .as("sentinel (tombstone) should be in TRIE_BRANCH_FRONTIER")
        .hasValue(new byte[0]);
    assertThat(storage.get(TRIE_BRANCH_STORAGE, nodeKey))
        .as("live TRIE_BRANCH_STORAGE is unaffected by frontier tombstone")
        .hasValue(liveValue);
  }

  /**
   * On restart with a populated frontier CF, the second migrator must NOT re-query trie logs for
   * already-migrated blocks. Each block's trie log is fetched exactly once across both migrators.
   */
  @Test
  public void trieMigratorWithIndexEnabled_restartUsesPersistedFrontierWithoutReRoll()
      throws Exception {
    final Hash stateRoot = computeTestAccountStateRoot();
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    when(trieLogManager.getTrieLogLayer(hashAt(0L))).thenReturn(Optional.of(new TrieLogLayer()));

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(storage);
    final TrieNodeChangeIndex changeIndex =
        new TrieNodeChangeIndex(storage, ArchiveNodeKey.RANGE_SIZE);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);

    final BonsaiFlatDbToArchiveMigrator firstMigrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);
    firstMigrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(firstMigrator.getMigrationProgress()).hasValue(1L);
    firstMigrator.close();

    assertThat(storage.get(TRIE_BRANCH_FRONTIER, WORLD_ROOT_HASH_KEY))
        .as("WORLD_ROOT_HASH_KEY must be in frontier after first migration")
        .isPresent();

    // Second migrator simulates a restart with same backing storage (frontier CF populated).
    // recoverTrieState() is now a no-op: BonsaiWorldState reads its root from TRIE_BRANCH_FRONTIER
    // during construction — no trie log re-roll needed.
    final BonsaiFlatDbToArchiveMigrator secondMigrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);

    // Each block's trie log must be fetched exactly once across both migrators.
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(0L));
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(1L));

    secondMigrator.close();
  }

  @Test
  public void byteSizeGuardFlushesMidRange() throws Exception {
    // Use one block with a matching state root (same setup as migratesTrieLogsWithRealWorldState).
    final Hash stateRoot = computeTestAccountStateRoot();
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    // Spy on storage for transaction counting; reassign so getArchivedAccountKey reads from it.
    storage = spy(new SegmentedInMemoryKeyValueStorage());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(storage);

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(storage);
    final TrieNodeChangeIndex changeIndex =
        new TrieNodeChangeIndex(storage, ArchiveNodeKey.RANGE_SIZE);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);
    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);
    // 1-byte limit: even WORLD_BLOCK_NUMBER_KEY (8 bytes) exceeds the limit. Block 0 (genesis)
    // and block 1 each commit in their own batch.
    migrator.setMaxBatchBytesForTesting(1L);
    clearInvocations(storage);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Block 0 and block 1 each in their own batch → at least 2 low-priority transactions.
    verify(storage, atLeast(2)).startLowPriorityTransaction();
    assertThat(getArchivedAccountKey(1L)).isPresent();
  }

  @Test
  public void resumeAfterMidBatchCrashReplaysPartialBatchWithoutCorruption() throws Exception {
    appendBlocks(4);

    // Pause when block 3's trie log is fetched so we can stop after batch 1 commits.
    final PausedMigration paused = pauseAtTrieLogLookup(hashAt(3));
    final BonsaiFlatDbToArchiveMigrator first = createMigrator();
    first.setMaxBlocksPerBatchForTesting(2); // batch1=blocks1-2, batch2=blocks3-4
    final CompletableFuture<Void> firstFuture = first.migrate();

    // Block-3 prefetch is in-flight — wait for batch 1 to commit.
    // Batch 1 covers blocks 0-1 (genesis + block 1); progress is saved as the last block in the
    // batch. The block-3 prefetch fires during batch 2 processing of block 2, so progress=1 in
    // committed storage when we check.
    paused.awaitStart();
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .until(() -> first.getMigratedBlockNumber() >= 1L);

    // Simulate crash: close() aborts the migration; paused prefetch times out and fails batch 2.
    first.close();
    assertThatThrownBy(() -> firstFuture.get(AWAIT_TIMEOUT_SECONDS + 2, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class);

    // Restore block-3 mock for the second migrator using doReturn to avoid invoking the
    // still-active pause stub (which would block again for AWAIT_TIMEOUT_SECONDS).
    doReturn(Optional.of(createAccountTrieLog(Wei.ONE)))
        .when(trieLogManager)
        .getTrieLogLayer(hashAt(3));

    // Second migrator resumes from block 3 (progress=2 → startBlock=3).
    final BonsaiFlatDbToArchiveMigrator second = createMigrator();
    second.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // All 4 blocks must be present; no corruption from the partial batch 2.
    assertThat(getArchivedAccountKey(1L)).isPresent();
    assertThat(getArchivedAccountKey(2L)).isPresent();
    assertThat(getArchivedAccountKey(3L)).isPresent();
    assertThat(getArchivedAccountKey(4L)).isPresent();
  }

  @Test
  public void progressAdvancesOnlyAtCommittedBatchBoundaries() throws Exception {
    appendBlocks(5);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    // After a full migration, progress == last migrated block.
    assertThat(migrator.getMigrationProgress()).hasValue(migrator.getMigratedBlockNumber());
  }

  @Test
  public void migratesMultipleBlocksInASingleBatchTransaction() throws Exception {
    appendBlocks(3);
    final SegmentedInMemoryKeyValueStorage spyStorage = spy(new SegmentedInMemoryKeyValueStorage());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(spyStorage);

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    clearInvocations(spyStorage);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // All 3 blocks fit in one batch → at most one low-priority transaction is opened for the bulk
    // loop (completion may open one more for upgradeToArchiveFlatDbMode).
    verify(spyStorage, atMost(2)).startLowPriorityTransaction();
  }

  /**
   * Regression test: within a per-batch migration in index mode, block N creates an account and
   * block N+1 applies an UPDATE to it. Block N+1's {@code rollForward} must be able to read block
   * N's flat account write from the {@code flatAccountOverlay} even though the batch transaction
   * has not yet committed. Without the fix this throws "Expected to update account, but the account
   * does not exist".
   */
  @Test
  public void batchMigrationWithIndex_flatAccountVisibleToNextBlockRollForward() throws Exception {
    // Block 1: CREATE test account with balance=1
    final Hash stateRoot1 = computeAccountStateRoot(Wei.ONE);
    final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
    final Block block1 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1)
                .setStateRoot(stateRoot1));
    blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

    // Block 2: UPDATE test account from balance=1 to balance=2
    final Hash stateRoot2 = computeAccountStateRoot(Wei.of(2L));
    final Block block2 =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(block1.getHash())
                .setBlockNumber(2)
                .setStateRoot(stateRoot2));
    blockchain.appendBlock(block2, blockDataGenerator.receipts(block2));

    final PmtStateTrieAccountValue accountAfterBlock1 =
        new PmtStateTrieAccountValue(1, Wei.ONE, Hash.EMPTY, Hash.EMPTY);
    final PmtStateTrieAccountValue accountAfterBlock2 =
        new PmtStateTrieAccountValue(1, Wei.of(2L), Hash.EMPTY, Hash.EMPTY);
    final TrieLogLayer updateTrieLog = new TrieLogLayer();
    updateTrieLog.addAccountChange(TEST_ADDRESS, accountAfterBlock1, accountAfterBlock2);
    when(trieLogManager.getTrieLogLayer(hashAt(2L))).thenReturn(Optional.of(updateTrieLog));

    final TrieNodeHistoryStore historyStore = new TrieNodeHistoryStore(storage);
    final TrieNodeChangeIndex changeIndex =
        new TrieNodeChangeIndex(storage, ArchiveNodeKey.RANGE_SIZE);
    final TrieNodeIndexProgress progress = new TrieNodeIndexProgress(ArchiveNodeKey.RANGE_SIZE);

    // Both blocks fit in one batch — rollForward for block 2 must see block 1's flat account.
    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigratorWithRealTrieLogsAndIndex(historyStore, changeIndex, progress);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(getArchivedAccountKey(1L))
        .as("Block 1 account must be in archive after migration")
        .isPresent();
    assertThat(getArchivedAccountKey(2L))
        .as("Block 2 account update must be in archive after migration")
        .isPresent();
  }

  // --- test helpers ---

  private MutableBlockchain createInMemoryBlockchain(final Block genesisBlock) {
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

  private void appendBlocks(final int count) {
    final Block head = blockchain.getBlockByNumber(blockchain.getChainHeadBlockNumber()).get();
    final List<Block> blocks = blockDataGenerator.blockSequence(head, count);
    for (Block block : blocks) {
      blockchain.appendBlock(block, blockDataGenerator.receipts(block));
    }
  }

  private Hash hashAt(final long blockNumber) {
    return blockchain.getBlockHeader(blockNumber).orElseThrow().getHash();
  }

  private BonsaiFlatDbToArchiveMigrator createMigrator() {
    return createMigrator(BOUNDARY_DISABLED);
  }

  // Wires the trie-node differential index for integration tests.
  private BonsaiFlatDbToArchiveMigrator createMigratorWithRealTrieLogsAndIndex(
      final TrieNodeHistoryStore historyStore,
      final TrieNodeChangeIndex changeIndex,
      final TrieNodeIndexProgress progress) {
    when(trieLogManager.getMaxLayersToLoad()).thenReturn(BOUNDARY_DISABLED);
    when(trieLogManager.getTrieLogLayer(hashAt(0L))).thenReturn(Optional.of(new TrieLogLayer()));
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final BonsaiArchiveFlatDbStrategy archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy());
    final BonsaiFlatDbToArchiveMigrator migrator =
        new BonsaiFlatDbToArchiveMigrator(
            worldStateStorage,
            trieLogManager,
            blockchain,
            Executors.newScheduledThreadPool(1),
            metricsSystem,
            archiveStrategy,
            historyStore,
            changeIndex,
            progress);
    migrators.add(migrator);
    return migrator;
  }

  private BonsaiFlatDbToArchiveMigrator createMigrator(final long boundaryDistance) {
    return createMigrator(this.blockchain, boundaryDistance);
  }

  private BonsaiFlatDbToArchiveMigrator createMigrator(
      final MutableBlockchain blockchain, final long boundaryDistance) {
    when(trieLogManager.getMaxLayersToLoad()).thenReturn(boundaryDistance);
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final BonsaiArchiveFlatDbStrategy archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy());
    final BonsaiFlatDbToArchiveMigrator migrator =
        new BonsaiFlatDbToArchiveMigrator(
            worldStateStorage,
            trieLogManager,
            blockchain,
            Executors.newScheduledThreadPool(1),
            metricsSystem,
            archiveStrategy);
    migrators.add(migrator);
    return migrator;
  }

  private TrieLogLayer createAccountTrieLog(final Wei balance) {
    final TrieLogLayer trieLog = new TrieLogLayer();
    final PmtStateTrieAccountValue value =
        new PmtStateTrieAccountValue(1, balance, Hash.EMPTY, Hash.EMPTY);
    trieLog.addAccountChange(TEST_ADDRESS, null, value);
    return trieLog;
  }

  // Compute the MPT state root for a world state containing only TEST_ADDRESS with balance=1,
  // matching the account created by createAccountTrieLog(Wei.ONE). Used to set block header
  // stateRoot so that BonsaiWorldState.persist() passes state root verification.
  private Hash computeTestAccountStateRoot() {
    return computeAccountStateRoot(Wei.ONE);
  }

  private Hash computeAccountStateRoot(final Wei balance) {
    final PmtStateTrieAccountValue account =
        new PmtStateTrieAccountValue(1, balance, Hash.EMPTY, Hash.EMPTY);
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    account.writeTo(out);
    final SimpleMerklePatriciaTrie<org.apache.tuweni.bytes.Bytes, org.apache.tuweni.bytes.Bytes>
        trie = new SimpleMerklePatriciaTrie<>(Function.identity());
    trie.put(TEST_ADDRESS.addressHash().getBytes(), out.encoded());
    return Hash.wrap(trie.getRootHash());
  }

  private Optional<byte[]> getArchivedAccountKey(final long blockNumber) {
    final byte[] key =
        calculateArchiveKeyWithMinSuffix(
            new BonsaiContext(blockNumber), TEST_ADDRESS.addressHash().getBytes().toArrayUnsafe());
    return storage.get(ACCOUNT_INFO_STATE_ARCHIVE, key);
  }

  private Optional<byte[]> getArchivedStorageKey(
      final long blockNumber, final StorageSlotKey slotKey) {
    final byte[] naturalKey =
        calculateNaturalSlotKey(TEST_ADDRESS.addressHash(), slotKey.getSlotHash());
    final byte[] key = calculateArchiveKeyWithMinSuffix(new BonsaiContext(blockNumber), naturalKey);
    return storage.get(ACCOUNT_STORAGE_ARCHIVE, key);
  }

  private PausedMigration pauseAtTrieLogLookup(final Hash blockHash) {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    when(trieLogManager.getTrieLogLayer(blockHash))
        .thenAnswer(invocation -> waitThenReturnTrieLog(started, proceed));
    return new PausedMigration(started, proceed);
  }

  private PausedMigration pauseAtAnyTrieLogLookup() {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    when(trieLogManager.getTrieLogLayer(any()))
        .thenAnswer(invocation -> waitThenReturnTrieLog(started, proceed));
    return new PausedMigration(started, proceed);
  }

  private Optional<TrieLogLayer> waitThenReturnTrieLog(
      final CountDownLatch started, final CountDownLatch proceed) {
    started.countDown();
    try {
      if (!proceed.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new AssertionError("release() was not called within " + AWAIT_TIMEOUT_SECONDS + "s");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("migration interrupted", e);
    }
    return Optional.of(createAccountTrieLog(Wei.ONE));
  }

  private record PausedMigration(CountDownLatch started, CountDownLatch proceed) {

    void awaitStart() throws InterruptedException {
      if (!started.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new AssertionError("Migration did not reach the paused trie-log lookup in time");
      }
    }

    void release() {
      proceed.countDown();
    }
  }
}
