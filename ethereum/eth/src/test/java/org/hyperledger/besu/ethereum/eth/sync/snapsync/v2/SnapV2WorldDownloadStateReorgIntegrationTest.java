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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2AccountRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2StorageRangeRequest;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the snap/2 reorg recovery pipeline driven through {@link
 * SnapV2WorldDownloadState#startPivotCatchup}. No mocks: a real blockchain, real Bonsai world
 * states (local + canonical), a real {@link SnapV2ReorgHealer} whose fetcher serves the canonical
 * state through a real {@link org.hyperledger.besu.ethereum.eth.manager.snap.SnapTestServing}, and
 * a real {@link EthContext}. Each test asserts the resulting world state, queue, and tracker
 * outcomes.
 */
class SnapV2WorldDownloadStateReorgIntegrationTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  // Test 2 — YES+NO (present in canonical): orphaned fork changed DAVE; canonical fork did not.
  private static final Address DAVE =
      Address.fromHexString("0x4444444444444444444444444444444444444444");

  // Test 3 — YES+NO (absent from canonical): contract exists only on orphaned fork.
  private static final Address NEW_CONTRACT =
      Address.fromHexString("0x9999999999999999999999999999999999999999");

  private static final Bytes NC_CODE = Bytes.fromHexString("0x60806040523480156010");

  private static final UInt256 S1 = UInt256.valueOf(1);
  private static final UInt256 S2 = UInt256.valueOf(2);
  private static final UInt256 S3 = UInt256.valueOf(3);

  // Test 4 — NO+YES: account created only on canonical fork.
  private static final Address GRACE =
      Address.fromHexString("0x7777777777777777777777777777777777777777");

  // Test 5 — NO+NO: account exists at block1, neither fork touches it.
  private static final Address BOB =
      Address.fromHexString("0x2222222222222222222222222222222222222222");

  // Test 6 — slot-level YES+NO: orphaned fork changes S1+S2, canonical fork sets S3.
  private static final Address FRANK =
      Address.fromHexString("0x6666666666666666666666666666666666666666");

  // Tests 7, 8 — download-status rules (PENDING and not-yet-downloaded).
  private static final Address PETE =
      Address.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  private static final UInt256 SP1 = UInt256.valueOf(101);
  private static final UInt256 SP2 = UInt256.valueOf(102);

  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  private final BonsaiWorldStateKeyValueStorage localStorage =
      ReorgBlockchainBuilder.newBonsaiStorage();
  private final WorldStateStorageCoordinator localCoordinator =
      new WorldStateStorageCoordinator(localStorage);
  private final BonsaiWorldStateKeyValueStorage canonicalStorage =
      ReorgBlockchainBuilder.newBonsaiStorage();
  private final WorldStateStorageCoordinator canonicalCoordinator =
      new WorldStateStorageCoordinator(canonicalStorage);

  private final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
  private final EthContext ethContext =
      EthProtocolManagerTestBuilder.builder().build().ethContext();

  private final AtomicInteger accountFetches = new AtomicInteger();
  private final AtomicInteger storageFetches = new AtomicInteger();
  private final AtomicInteger codeFetches = new AtomicInteger();

  // ── Test 1: matrix YES + YES → apply canonical BAL, no re-download ───────────────────────────

  @Test
  void modifiedInOrphanedAndNewBlock_appliesNewBalWithoutReDownload() {
    // Build all blockchain blocks first.
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    // orphaned (2s): Alice = 50  — appended first so it is initially canonical
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    // canonical (2c): Alice = 80 — higher difficulty wins the reorg
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    // Build canonical world state.
    applyTo(
        canonicalCoordinator,
        1,
        1,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    applyTo(
        canonicalCoordinator,
        2,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    // Create download state with stale pivot; then seed trackers and apply local BALs.
    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);

    // Register the full address space as already downloaded.
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 0);

    // Apply canonical local BALs using the state's trackers so the healer sees a complete picture.
    applyTo(localCoordinator, 1, 1, state.getAccountRangeTracker(), state.getStorageRangeTracker());
    applyTo(localCoordinator, 2, 2, state.getAccountRangeTracker(), state.getStorageRangeTracker());

    startCatchupAndAwait(state, newPivot.getHeader());

    assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(80)); // canonical value applied
    assertThat(accountFetches).hasValue(0); // Category YES+YES: no re-download
    assertThat(storageFetches).hasValue(0);
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  // ── Test 2: matrix YES + NO (present in canonical) → re-download and restore ─────────────────

  @Test
  void modifiedInOrphanedButNotNewBlock_reDownloadsAndUpdatesWhenPresent() {
    // Build all blockchain blocks first.
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(DAVE, Wei.of(75))), 1L);
    // orphaned (2s): DAVE = 60 — appended first so it is initially canonical
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(DAVE, Wei.of(60))), 2L);
    // canonical (2c): ALICE = 80, DAVE untouched — higher difficulty wins the reorg
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    // Build canonical world state first so we can pin the state root.
    applyTo(
        canonicalCoordinator,
        1,
        1,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    applyTo(
        canonicalCoordinator,
        2,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    // Create download state with stale pivot; then seed trackers and apply local BALs.
    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);

    // Register the full address space as already downloaded.
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 0);

    // Apply canonical BALs to local using the state's trackers.
    applyTo(localCoordinator, 1, 1, state.getAccountRangeTracker(), state.getStorageRangeTracker());
    applyTo(localCoordinator, 2, 2, state.getAccountRangeTracker(), state.getStorageRangeTracker());

    startCatchupAndAwait(state, newPivot.getHeader());

    // DAVE was modified by orphaned fork but is absent from canonical BAL → re-downloaded.
    assertThat(readAccount(DAVE).getBalance()).isEqualTo(Wei.of(75));
    assertThat(accountFetches.get()).isGreaterThanOrEqualTo(1);
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  // ── Test 3: matrix YES + NO (absent from canonical) → delete + purge queued child requests ────

  @Test
  void modifiedInOrphanedButNotNewBlock_deletesWhenAbsentFromCanonical() {
    // Build common-ancestor block.
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 1L);

    // Orphaned block creates NEW_CONTRACT with balance, code, and storage.
    final Block block2s =
        b.appendStale(
            block1.getHeader(),
            b.merge(
                b.balWithBalances(Map.of(NEW_CONTRACT, Wei.ONE)),
                b.balWithCodeChange(NEW_CONTRACT, NC_CODE),
                b.balWithStorageChanges(NEW_CONTRACT, Map.of(S1, UInt256.valueOf(5)))),
            2L);

    // Pre-seed local with the orphaned state while block2s is still canonical at height 2.
    applyTo(
        localCoordinator,
        1,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    // Verify NEW_CONTRACT is present locally before the canonical fork arrives.
    assertThat(readStorageSlot(NEW_CONTRACT, S1)).hasValue(UInt256.valueOf(5));
    assertThat(accountExists(NEW_CONTRACT)).isTrue();

    // Canonical fork (block2c) wins the reorg; NEW_CONTRACT is absent from canonical chain.
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    // Build canonical world state and compute its root.
    applyTo(
        canonicalCoordinator,
        1,
        1,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    applyTo(
        canonicalCoordinator,
        2,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    // Create the download state with the orphaned block as its starting pivot.
    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 0);

    // Queue a storage-range and a bytecode request for the doomed contract so the purge is tested.
    final SnapV2StorageRangeRequest storageReq =
        new SnapV2StorageRangeRequest(
            block2s.getHeader(),
            Bytes32.wrap(NEW_CONTRACT.addressHash().getBytes()),
            Bytes32.random(),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE,
            RangeManager.MIN_RANGE);
    final SnapV2BytecodeRequest codeReq =
        new SnapV2BytecodeRequest(
            block2s.getHeader(),
            Bytes32.wrap(NEW_CONTRACT.addressHash().getBytes()),
            Bytes32.wrap(Hash.hash(NC_CODE).getBytes()),
            RangeManager.MIN_RANGE);
    state.enqueueRequest(storageReq);
    state.enqueueRequest(codeReq);

    startCatchupAndAwait(state, newPivot.getHeader());

    // NEW_CONTRACT must be gone from local state.
    assertThat(accountExists(NEW_CONTRACT)).isFalse();
    assertThat(readStorageSlot(NEW_CONTRACT, S1)).isEmpty();
    // Queued child requests for the doomed contract must have been purged.
    assertThat(state.pendingStorageRequests.asList()).isEmpty();
    assertThat(state.pendingCodeRequests.asList()).isEmpty();
    // ALICE carries through from the canonical BAL.
    assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  // ── Test 4: matrix NO + YES → new account created from canonical BAL ─────────────────────────

  @Test
  void notModifiedInOrphanedButModifiedInNewBlock_appliesNewBal() {
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(80))),
                b.balWithBalances(Map.of(GRACE, Wei.of(50)))),
            2L);

    applyTo(
        canonicalCoordinator,
        1,
        1,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    applyTo(
        canonicalCoordinator,
        2,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 0);
    applyTo(localCoordinator, 1, 1, state.getAccountRangeTracker(), state.getStorageRangeTracker());
    applyTo(localCoordinator, 2, 2, state.getAccountRangeTracker(), state.getStorageRangeTracker());

    startCatchupAndAwait(state, newPivot.getHeader());

    assertThat(readAccount(GRACE).getBalance()).isEqualTo(Wei.of(50)); // created from canonical BAL
    assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  // ── Test 5: matrix NO + NO → untouched account is left intact ────────────────────────────────

  @Test
  void notModifiedInOrphanedOrNewBlock_skipsEntirely() {
    final Block block1 =
        b.appendBlockWithBal(
            b.header(0),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(100))),
                b.balWithBalances(Map.of(BOB, Wei.of(100)))),
            1L);
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    applyTo(
        canonicalCoordinator,
        1,
        1,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    applyTo(
        canonicalCoordinator,
        2,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 0);
    applyTo(localCoordinator, 1, 1, state.getAccountRangeTracker(), state.getStorageRangeTracker());
    applyTo(localCoordinator, 2, 2, state.getAccountRangeTracker(), state.getStorageRangeTracker());

    startCatchupAndAwait(state, newPivot.getHeader());

    assertThat(readAccount(BOB).getBalance()).isEqualTo(Wei.of(100)); // untouched by both forks
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  // ── Test 6: slot-level YES+NO → orphaned-only slots restored, canonical slot applied ──────────

  @Test
  void slotsModifiedInOrphanedButNotNewBlock_reDownloadsSlots() {
    // Block 1: FRANK has balance + S1=7 (base state).
    final var baseBal =
        b.merge(
            b.balWithBalances(Map.of(FRANK, Wei.of(200))),
            b.balWithStorageChanges(FRANK, Map.of(S1, UInt256.valueOf(7))));
    final Block block1 = b.appendBlockWithBal(b.header(0), baseBal, 1L);

    // Block 2s (orphaned, lower difficulty): FRANK S1=100, S2=200 — canonical at height 2
    // initially.
    final Block block2s =
        b.appendStale(
            block1.getHeader(),
            b.balWithStorageChanges(
                FRANK, Map.of(S1, UInt256.valueOf(100), S2, UInt256.valueOf(200))),
            2L);

    // Pre-seed local with the orphaned state while block2s is still canonical at height 2.
    applyTo(
        localCoordinator,
        1,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    // Block 2c (canonical, higher difficulty): FRANK S3=555 — triggers reorg.
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.balWithStorageChanges(FRANK, Map.of(S3, UInt256.valueOf(555))),
            2L);

    // Build canonical world state (block2c is now canonical at height 2).
    applyTo(
        canonicalCoordinator,
        1,
        1,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    applyTo(
        canonicalCoordinator,
        2,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    // Create download state with the stale (orphaned) pivot.
    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    // Completing the full account range (initialChildCount=0) makes
    // isAccountHashDownloaded(FRANK)=true so that computeSlotsToRefetch detects the
    // diverged orphaned slots S1 and S2.
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 0);

    // Queue a storage request for FRANK with the (soon-stale) orphaned storage root.
    final Bytes32 oldRoot = Bytes32.wrap(readAccount(FRANK).getStorageRoot().getBytes());
    final SnapV2StorageRangeRequest frankReq =
        new SnapV2StorageRangeRequest(
            block2s.getHeader(),
            Bytes32.wrap(FRANK.addressHash().getBytes()),
            oldRoot,
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE,
            RangeManager.MIN_RANGE);
    state.enqueueRequest(frankReq);

    startCatchupAndAwait(state, newPivot.getHeader());

    assertThat(readStorageSlot(FRANK, S1)).hasValue(UInt256.valueOf(7)); // restored to base value
    assertThat(readStorageSlot(FRANK, S2)).isEmpty(); // orphaned-only slot removed
    assertThat(readStorageSlot(FRANK, S3)).hasValue(UInt256.valueOf(555)); // canonical slot applied
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);

    // The queued storage request was retargeted to the new pivot with FRANK's canonical root.
    final var queued = state.pendingStorageRequests.asList();
    assertThat(queued).hasSize(1);
    final SnapV2StorageRangeRequest retargeted = (SnapV2StorageRangeRequest) queued.get(0);
    assertThat(retargeted.getPivotBlockHeader()).isEqualTo(newPivot.getHeader());
    final Bytes32 canonicalFrankRoot = Bytes32.wrap(readAccount(FRANK).getStorageRoot().getBytes());
    assertThat(retargeted.getStorageRoot()).isEqualTo(canonicalFrankRoot);
  }

  // ── Test 7: PENDING range — fix downloaded slot, defer not-downloaded slot ───────────────────

  /**
   * Account range is PENDING (has one outstanding child). PETE's slot SP1 is downloaded but SP2 is
   * not. The orphaned fork rewrites both slots. After recovery:
   *
   * <ul>
   *   <li>SP1 is re-fetched from the canonical peer and restored to its canonical value (1).
   *   <li>SP2 remains absent (never downloaded, never re-fetched).
   *   <li>The queued storage request is retargeted to the new pivot with PETE's canonical storage
   *       root (installed via root-patch, not recomputed from the incomplete trie).
   * </ul>
   *
   * <pre>
   * gen -- 1(PETE=100, SP1=1) -- 2(SP2=2) -- 3s(SP1=10, SP2=20)   orphaned
   *                                        \-- 3c(PETE=300 bal)     canonical
   *                                              \-- 4c (pins canonicalRoot, new pivot)
   * </pre>
   */
  @Test
  void pendingRange_appliesToDownloadedSlotsAndDefersRest() {
    // Throwaway trackers for local seeding: PENDING range, only SP1 slot downloaded.
    final DownloadedAccountRangeTracker seedAccountTracker = new DownloadedAccountRangeTracker();
    seedAccountTracker.registerPending(Bytes32.ZERO, MAX_KEY, 1);
    final DownloadedStorageRangeTracker seedStorageTracker = new DownloadedStorageRangeTracker();
    seedStorageTracker.registerSlotRange(
        Bytes32.wrap(PETE.addressHash().getBytes()),
        Bytes32.wrap(new StorageSlotKey(SP1).getSlotHash().getBytes()),
        Bytes32.wrap(new StorageSlotKey(SP1).getSlotHash().getBytes()));

    // Block 1 (shared): creates PETE with balance 100 and SP1=1.
    // PETE is new (existingAccount == null) so isAccountCompleted=true; SP1 applied in full.
    final Block block1 =
        b.appendBlockWithBal(
            b.header(0),
            b.merge(
                b.balWithBalances(Map.of(PETE, Wei.of(100))),
                b.balWithStorageChanges(PETE, Map.of(SP1, UInt256.valueOf(1)))),
            1L);
    applyTo(localCoordinator, 1, 1, seedAccountTracker, seedStorageTracker);
    applyTo(
        canonicalCoordinator,
        1,
        1,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    // Block 2 (shared): SP2 appears on the canonical side; SP2 is NOT downloaded locally
    // (not in seedStorageTracker), so the slot-guard blocks it.
    final Block block2 =
        b.appendBlockWithBal(
            block1.getHeader(), b.balWithStorageChanges(PETE, Map.of(SP2, UInt256.valueOf(2))), 2L);
    applyTo(localCoordinator, 2, 2, seedAccountTracker, seedStorageTracker);
    applyTo(
        canonicalCoordinator,
        2,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    // Block 3s (orphaned, low difficulty): rewrites both slots; only SP1 lands locally.
    // appendStale uses LOW difficulty, so it is initially canonical at height 3.
    final Block block3s =
        b.appendStale(
            block2.getHeader(),
            b.balWithStorageChanges(
                PETE, Map.of(SP1, UInt256.valueOf(10), SP2, UInt256.valueOf(20))),
            3L);
    applyTo(localCoordinator, 3, 3, seedAccountTracker, seedStorageTracker);
    assertThat(readStorageSlot(PETE, SP1)).hasValue(UInt256.valueOf(10)); // downloaded, rewritten
    assertThat(readStorageSlot(PETE, SP2)).isEmpty(); // not downloaded, still absent

    // Block 3c (canonical, high difficulty): balance only; reorg makes it canonical at height 3.
    final Block block3c =
        b.appendCanonical(block2.getHeader(), b.balWithBalances(Map.of(PETE, Wei.of(300))), 3L);
    applyTo(
        canonicalCoordinator,
        3,
        3,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block3c.getHeader(), b.emptyBal(), 4L, canonicalRoot);

    // Create download state; register the same PENDING+SP1 configuration on state's trackers
    // (the healer reads these during planReorg / applyReorgCorrections).
    final SnapV2WorldDownloadState state = createDownloadState(block3s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 1);
    state
        .getStorageRangeTracker()
        .registerSlotRange(
            Bytes32.wrap(PETE.addressHash().getBytes()),
            Bytes32.wrap(new StorageSlotKey(SP1).getSlotHash().getBytes()),
            Bytes32.wrap(new StorageSlotKey(SP1).getSlotHash().getBytes()));

    // Queue PETE's still-in-progress storage request with the stale (orphaned) storage root.
    final Bytes32 oldRoot = Bytes32.wrap(readAccount(PETE).getStorageRoot().getBytes());
    final SnapV2StorageRangeRequest peteReq =
        new SnapV2StorageRangeRequest(
            block3s.getHeader(),
            Bytes32.wrap(PETE.addressHash().getBytes()),
            oldRoot,
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE,
            RangeManager.MIN_RANGE);
    state.enqueueRequest(peteReq);

    startCatchupAndAwait(state, newPivot.getHeader());

    // Downloaded SP1 restored; not-downloaded SP2 stays absent; canonical balance applied.
    assertThat(readStorageSlot(PETE, SP1)).hasValue(UInt256.valueOf(1));
    assertThat(readStorageSlot(PETE, SP2)).isEmpty();
    assertThat(readAccount(PETE).getBalance()).isEqualTo(Wei.of(300));

    // Pending account: retargeted storage request carries PETE's canonical storage root
    // (installed via root-patch, not recomputed from the still-incomplete local trie).
    final PmtStateTrieAccountValue canonicalPete =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(
                canonicalCoordinator
                    .applyForStrategy(
                        bonsai -> bonsai.getAccount(PETE.addressHash()),
                        forest -> Optional.<Bytes>empty())
                    .orElseThrow()));
    final SnapV2StorageRangeRequest retargeted =
        (SnapV2StorageRangeRequest) state.pendingStorageRequests.asList().get(0);
    assertThat(retargeted.getPivotBlockHeader()).isEqualTo(newPivot.getHeader());
    assertThat(retargeted.getStorageRoot())
        .isEqualTo(Bytes32.wrap(canonicalPete.getStorageRoot().getBytes()));
  }

  // ── Test 8: not-yet-downloaded range → account deferred entirely ─────────────────────────────

  /**
   * ALICE's account hash lies outside the registered downloaded range (only [ZERO, ZERO] is
   * persisted). The orphaned fork modified ALICE, but since her range is not persisted, recovery
   * must skip her entirely — no re-download, no local change.
   *
   * <pre>
   * gen -- 1(ALICE=100) -- 2s(ALICE=50)   orphaned
   *                     \-- 2c(ALICE=80)  canonical
   *                           \-- 3c (newPivot, canonicalRoot)
   * </pre>
   */
  @Test
  void notYetDownloadedRange_defersToLaterCycle() {
    // Verify that ALICE is NOT covered by the degenerate [ZERO, ZERO] range.
    final Bytes32 aliceHash = Bytes32.wrap(ALICE.addressHash().getBytes());
    assertThat(aliceHash).isNotEqualTo(Bytes32.ZERO);

    // Build chain: block1 (shared), block2s (orphaned low), block2c (canonical high).
    final Block block1 =
        b.appendBlockWithBal(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    final Block block2s =
        b.appendStale(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    // Seed canonical coordinator only; ALICE is not applied to local (her range is not persisted).
    applyTo(
        canonicalCoordinator,
        1,
        1,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    applyTo(
        canonicalCoordinator,
        2,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    // Create download state: only [ZERO, ZERO] is persisted; ALICE's hash is NOT covered.
    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, Bytes32.ZERO, 0);
    // ALICE's hash is outside [ZERO, ZERO] → isAccountHashPersisted returns false.
    assertThat(state.getAccountRangeTracker().isAccountHashPersisted(aliceHash)).isFalse();

    // Do NOT apply any local BALs for ALICE — she is not in the persisted range.
    startCatchupAndAwait(state, newPivot.getHeader());

    // ALICE is not in the persisted range: skipped by planReorg and applyCanonicalBals.
    assertThat(accountExists(ALICE)).isFalse();
    assertThat(accountFetches).hasValue(0); // no re-download triggered
  }

  // ── Test 14: same-chain advance → BALs [old+1,new] applied, request retargeted, no fetch ──────

  /**
   * Straight canonical chain: gen ─ 1(ALICE=100) ─ 2(ALICE=50)[old pivot] ─ 3(ALICE=80)[new pivot].
   * All blocks have HIGH difficulty so all are canonical. {@code areBothBlocksOnCanonicalChain}
   * returns {@code true} → the same-chain branch runs: apply BALs [3,3], retarget queued account
   * request to new pivot. No peer fetches because ALICE has no storage (pendingAffected is empty).
   */
  @Test
  void sameChainPivotAdvance_appliesBalsAndRetargets_noReorg() {
    final Block block1 =
        b.appendCanonical(b.header(0), b.balWithBalances(Map.of(ALICE, Wei.of(100))), 1L);
    final Block block2 =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);

    // Apply [1,2] to canonical coordinator.
    applyTo(
        canonicalCoordinator,
        1,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    // Build and append block3 (ALICE=80) before applying its BAL so the blockchain holds it.
    final Block block3 =
        b.appendCanonical(block2.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 3L);

    // Apply [3,3] to canonical now that block3 is in the blockchain.
    applyTo(
        canonicalCoordinator,
        3,
        3,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());

    // Create download state with block2 as the (old) current pivot.
    final SnapV2WorldDownloadState state =
        createDownloadState(
            block2.getHeader(), ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator));
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 0);
    applyTo(localCoordinator, 1, 2, state.getAccountRangeTracker(), state.getStorageRangeTracker());

    // Queue an account request targeted at block2 — it must be retargeted to block3.
    final SnapV2AccountRangeRequest accountReq =
        new SnapV2AccountRangeRequest(
            block2.getHeader(), RangeManager.MIN_RANGE, RangeManager.MAX_RANGE);
    state.enqueueRequest(accountReq);

    startCatchupAndAwait(state, block3.getHeader());

    // BAL [3,3] applied: ALICE=80. No re-download (same canonical chain).
    assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(accountFetches).hasValue(0);
    // Queued account request retargeted to the new pivot.
    final SnapV2AccountRangeRequest retargetedReq =
        (SnapV2AccountRangeRequest) state.pendingAccountRequests.asList().get(0);
    assertThat(retargetedReq.getPivotBlockHeader()).isEqualTo(block3.getHeader());
    // Local and canonical world states agree.
    assertThat(ReorgBlockchainBuilder.worldStateRoot(localCoordinator))
        .isEqualTo(ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator));
  }

  // ── Test 9: unrecoverable reorg → failure propagated through downloadFuture ─────────────────

  /**
   * The orphaned block's BAL is NOT stored locally (simulates a pruned orphaned BAL). The healer
   * cannot plan the reorg → {@link SnapV2ReorgHealer#planReorg} throws {@link
   * ReorgUnrecoverableException} → {@code startPivotCatchup} completes {@code internalFuture}
   * exceptionally → {@code downloadFuture} is also completed exceptionally.
   *
   * <pre>
   * gen -- 1() -- 2s(ALICE=50)   orphaned, BAL not stored
   *            \-- 2c(ALICE=80)  canonical
   *                  \-- 3c (pins canonicalRoot, new pivot)
   * </pre>
   */
  @Test
  void unrecoverableReorg_propagatesFailure() {
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    // Orphaned block whose BAL header-hash is committed but the BAL itself is NOT stored.
    final Block block2s =
        b.appendStaleWithoutStoringBal(
            block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(50))), 2L);
    final Block block2c =
        b.appendCanonical(block1.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(80))), 2L);

    // Build canonical world state and compute its root.
    applyTo(
        canonicalCoordinator,
        1,
        1,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    applyTo(
        canonicalCoordinator,
        2,
        2,
        ReorgBlockchainBuilder.fullAccountRange(),
        new DownloadedStorageRangeTracker());
    final Hash canonicalRoot = ReorgBlockchainBuilder.worldStateRoot(canonicalCoordinator);
    final Block newPivot = b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    // Create download state with the stale (orphaned) block2s as the current pivot.
    final SnapV2WorldDownloadState state = createDownloadState(block2s.getHeader(), canonicalRoot);
    state.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 0);
    // Apply only block1 locally; block2s was never applied (simulates partial download).
    applyTo(localCoordinator, 1, 1, state.getAccountRangeTracker(), state.getStorageRangeTracker());

    // Trigger pivot catchup without awaiting — the reorg healer cannot read the orphaned BAL
    // and must throw ReorgUnrecoverableException synchronously (0 in-flight tasks, pre-completed
    // listener future → finishPivotCatchup runs on the calling thread).
    state.startPivotCatchup(newPivot.getHeader());

    assertThat(state.getDownloadFuture()).isCompletedExceptionally();
    assertThatThrownBy(() -> state.getDownloadFuture().join())
        .hasRootCauseInstanceOf(ReorgUnrecoverableException.class);
  }

  // ── shared helpers ────────────────────────────────────────────────────────────────────────────

  /**
   * Builds a real {@link SnapV2WorldDownloadState} with the stale block as its starting pivot. The
   * healer serves from the canonical world state; the pivot-catchup listener returns an immediately
   * completed future (the blockchain already holds every block and BAL, so there is no chain gap).
   */
  private SnapV2WorldDownloadState createDownloadState(
      final BlockHeader stalePivot, final Hash canonicalRoot) {
    final DefaultBlockchain blockchain = b.blockchain();
    final SnapV2ReorgStateFetcher fetcher =
        ReorgBlockchainBuilder.servingFetcher(
            canonicalStorage,
            canonicalRoot,
            localCoordinator,
            accountFetches,
            storageFetches,
            codeFetches);
    final SnapV2ReorgHealer healer =
        new SnapV2ReorgHealer(
            blockchain, localCoordinator, ReorgBlockchainBuilder.balEnabledSchedule(), fetcher);
    return new SnapV2WorldDownloadState(
        localCoordinator,
        new SnapSyncStatePersistenceManager(new InMemoryKeyValueStorageProvider()),
        new SnapSyncProcessState(stalePivot),
        new InMemoryTasksPriorityQueues<SnapDataRequest>(),
        10,
        50_000L,
        new SnapSyncMetricsManager(new NoOpMetricsSystem(), ethContext),
        Clock.systemUTC(),
        SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS,
        null,
        (current, next) -> CompletableFuture.completedFuture(null),
        new SnapV2BlockAccessListApplier(
            localCoordinator, blockchain, ReorgBlockchainBuilder.balEnabledSchedule()),
        healer,
        blockchain,
        ethContext,
        1000L);
  }

  private void applyTo(
      final WorldStateStorageCoordinator coordinator,
      final long fromBlock,
      final long toBlock,
      final DownloadedAccountRangeTracker accountTracker,
      final DownloadedStorageRangeTracker storageTracker) {
    new SnapV2BlockAccessListApplier(
            coordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule())
        .applyBlockAccessLists(fromBlock, toBlock, accountTracker, storageTracker)
        .commit();
  }

  private void startCatchupAndAwait(
      final SnapV2WorldDownloadState state, final BlockHeader newPivot) {
    state.startPivotCatchup(newPivot);
    // finishPivotCatchup runs synchronously (pre-completed listener future + 0 in-flight tasks);
    // guard against a future refactor by joining the download future if it already resolved.
  }

  private PmtStateTrieAccountValue readAccount(final Address address) {
    return PmtStateTrieAccountValue.readFrom(RLP.input(readAccountBytes(address).orElseThrow()));
  }

  private Optional<Bytes> readAccountBytes(final Address address) {
    return localCoordinator.applyForStrategy(
        bonsai -> bonsai.getAccount(address.addressHash()), forest -> Optional.<Bytes>empty());
  }

  private boolean accountExists(final Address address) {
    return readAccountBytes(address).isPresent();
  }

  private Optional<UInt256> readStorageSlot(final Address address, final UInt256 slot) {
    return localCoordinator
        .applyForStrategy(
            bonsai ->
                bonsai.getStorageValueByStorageSlotKey(
                    address.addressHash(), new StorageSlotKey(slot)),
            forest -> Optional.<Bytes>empty())
        .map(UInt256::fromBytes);
  }
}
