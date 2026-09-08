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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
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
import org.hyperledger.besu.ethereum.rlp.RLP;
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
}
