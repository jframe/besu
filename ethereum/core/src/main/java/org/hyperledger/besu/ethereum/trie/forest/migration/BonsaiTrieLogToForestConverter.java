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
package org.hyperledger.besu.ethereum.trie.forest.migration;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.worldview.ForestMutableWorldState;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.util.cache.MemoryBoundCache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Rebuilds a Forest world-state node set from Bonsai {@link
 * org.hyperledger.besu.plugin.services.trielogs.TrieLog}s by replaying each block's state diff
 * directly into Merkle-Patricia Tries at the hash level, without re-executing any EVM transactions.
 *
 * <p>Each applied trie log mutates the account state trie (and, where required, per-account storage
 * tries) and writes the resulting nodes into the supplied {@link ForestWorldStateKeyValueStorage}.
 * After applying a layer the reconstructed state root is verified against the expected state root
 * carried by the block; a mismatch indicates the replay diverged from the canonical chain and the
 * changes for that layer are rolled back.
 */
public class BonsaiTrieLogToForestConverter {
  private static final Bytes32 EMPTY_TRIE_ROOT = Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes());
  // Per-entry JVM overhead beyond raw content bytes: Caffeine PSMS node (~120B) + Bytes32 object
  // overhead (~56B vs 32B counted) + Bytes value object header + backing array header (~52B).
  // Without this, the weigher underestimates by ~2.5x and the cache exhaust heap before hitting
  // its weight limit (43.9M entries × 182 counted ≈ 8GB, but actual JVM cost was ~18-20GB).
  private static final int CACHE_ENTRY_JVM_OVERHEAD = 300;

  private final ForestWorldStateKeyValueStorage forestStorage;
  // Cross-block node cache (hash -> encoded node). Null when disabled (cacheMaxBytes <= 0).
  private final MemoryBoundCache<Bytes32, Bytes> nodeCache;
  // Pool of reader threads used to warm the node cache ahead of replay; null when prefetch is
  // disabled (no cache, or prefetchThreads <= 0).
  private final ExecutorService prefetchExecutor;
  // Single-thread coordinator that drives a window's parallel warming off the apply thread, so the
  // next window warms while the current one is replayed. Null when prefetch is disabled.
  private final ExecutorService prefetchCoordinator;
  // Written only by the single apply thread, but read by the background prefetch threads at
  // task-execution time so warming traverses from the freshest available root (see prefetchFrom).
  // volatile guarantees those threads see the latest published root rather than a stale snapshot.
  private volatile Bytes32 currentRootHash;

  /**
   * Creates a converter that writes reconstructed Forest trie nodes into the given storage, with no
   * cross-block node cache.
   *
   * @param forestStorage the Forest world-state storage to populate
   */
  public BonsaiTrieLogToForestConverter(final ForestWorldStateKeyValueStorage forestStorage) {
    this(forestStorage, 0, 0);
  }

  /**
   * Creates a converter that writes reconstructed Forest trie nodes into the given storage and
   * caches trie nodes across blocks to avoid re-reading hot nodes from disk.
   *
   * @param forestStorage the Forest world-state storage to populate
   * @param cacheMaxBytes maximum on-heap size in bytes of the cross-block node cache; values &lt;=
   *     0 disable the cache
   */
  public BonsaiTrieLogToForestConverter(
      final ForestWorldStateKeyValueStorage forestStorage, final long cacheMaxBytes) {
    this(forestStorage, cacheMaxBytes, 0);
  }

  /**
   * Creates a converter that writes reconstructed Forest trie nodes into the given storage, caches
   * trie nodes across blocks, and optionally warms that cache using a pool of parallel reader
   * threads ahead of replay.
   *
   * <p>The trie traversal is inherently pointer-chasing, so a single replay thread can never have
   * more than one disk read outstanding, leaving the NVMe's parallelism idle. The prefetch pool
   * issues the path reads for an upcoming window of blocks concurrently, raising the disk queue
   * depth so reads are served in parallel rather than serially.
   *
   * @param forestStorage the Forest world-state storage to populate
   * @param cacheMaxBytes maximum on-heap size in bytes of the cross-block node cache; values &lt;=
   *     0 disable the cache
   * @param prefetchThreads number of parallel reader threads used to warm the cache ahead of
   *     replay; values &lt;= 0 (or a disabled cache) disable prefetch
   */
  public BonsaiTrieLogToForestConverter(
      final ForestWorldStateKeyValueStorage forestStorage,
      final long cacheMaxBytes,
      final int prefetchThreads) {
    this.forestStorage = forestStorage;
    this.currentRootHash = EMPTY_TRIE_ROOT;
    this.nodeCache =
        cacheMaxBytes > 0
            ? new MemoryBoundCache<>(
                cacheMaxBytes,
                (hash, node) -> node.size() + Bytes32.SIZE + CACHE_ENTRY_JVM_OVERHEAD)
            : null;
    final boolean prefetchEnabled = this.nodeCache != null && prefetchThreads > 0;
    this.prefetchExecutor =
        prefetchEnabled
            ? Executors.newFixedThreadPool(
                prefetchThreads,
                runnable -> {
                  final Thread thread = new Thread(runnable, "forest-convert-prefetch");
                  thread.setDaemon(true);
                  return thread;
                })
            : null;
    this.prefetchCoordinator =
        prefetchEnabled
            ? Executors.newSingleThreadExecutor(
                runnable -> {
                  final Thread thread = new Thread(runnable, "forest-convert-prefetch-coord");
                  thread.setDaemon(true);
                  return thread;
                })
            : null;
  }

  /** Releases the prefetch thread pools, if any. Safe to call more than once. */
  public void close() {
    if (prefetchCoordinator != null) {
      prefetchCoordinator.shutdownNow();
    }
    if (prefetchExecutor != null) {
      prefetchExecutor.shutdownNow();
    }
  }

  /**
   * Returns the current account state trie root hash reconstructed so far.
   *
   * @return the current state root hash
   */
  public Hash currentRootHash() {
    return Hash.wrap(currentRootHash);
  }

  /**
   * Sets the running account state trie root hash so replay continues from an already-converted
   * block instead of from genesis.
   *
   * @param root the account state trie root hash to resume from
   */
  public void resumeFrom(final Hash root) {
    this.currentRootHash = Bytes32.wrap(root.getBytes());
  }

  /**
   * Returns the cross-block node cache hit rate, or {@code -1.0} if the cache is disabled.
   *
   * @return the cache hit rate, or -1.0 when disabled
   */
  public double cacheHitRate() {
    return nodeCache == null ? -1.0 : nodeCache.hitRate();
  }

  /**
   * Returns the estimated number of entries in the cross-block node cache, or {@code 0} if the
   * cache is disabled.
   *
   * @return the estimated cache size, or 0 when disabled
   */
  public long cacheEstimatedSize() {
    return nodeCache == null ? 0L : nodeCache.estimatedSize();
  }

  private Optional<Bytes> cachingLoad(
      final Bytes32 hash, final Supplier<Optional<Bytes>> storageLoader) {
    if (nodeCache == null) {
      return storageLoader.get();
    }
    final Bytes cached = nodeCache.getIfPresent(hash);
    if (cached != null) {
      return Optional.of(cached);
    }
    final Optional<Bytes> fromStorage = storageLoader.get();
    fromStorage.ifPresent(node -> nodeCache.put(hash, node));
    return fromStorage;
  }

  private NodeLoader accountNodeLoader() {
    return (location, hash) -> cachingLoad(hash, () -> forestStorage.getAccountStateTrieNode(hash));
  }

  private NodeLoader storageNodeLoader() {
    return (location, hash) ->
        cachingLoad(hash, () -> forestStorage.getAccountStorageTrieNode(hash));
  }

  /**
   * Warms the cross-block node cache for the keys changed across a window of upcoming trie logs, by
   * traversing their root&rarr;leaf paths concurrently from the current (already-converted) state
   * root. This raises the disk queue depth so the otherwise-serialized path reads are issued in
   * parallel; the subsequent single-threaded {@link #applyTrieLog} then finds the nodes resident in
   * the cache.
   *
   * <p>All traversals start from the converter's current root (the window base), which is the only
   * root guaranteed to be present on disk before the window is applied. For a key whose path is not
   * modified earlier in the window, the path nodes under the base root are byte-identical to those
   * under the key's true pre-state root, so warming from the base root warms exactly the right
   * nodes; for a key whose path is modified earlier in the window, the earlier block's
   * write-through keeps those nodes cached. Prefetch only populates the cache (keyed by node hash)
   * and can never change replay output; the per-block state-root verification in {@link
   * #applyTrieLog} remains the correctness net. Read failures are swallowed: a prefetch miss is
   * never fatal, as replay reads the authoritative node by hash anyway.
   *
   * <p>No-op when prefetch is disabled or the window is empty.
   *
   * @param layers the upcoming trie logs whose changed keys should be warmed
   */
  public void prefetch(final List<TrieLog> layers) {
    prefetchFrom(layers);
  }

  /**
   * Submits the warming of {@code layers} to run on the prefetch coordinator, returning immediately
   * so the caller can replay an earlier window while this window warms. This pipelines the parallel
   * reads of the next window with the single-threaded apply of the current one, keeping the disk
   * busy continuously instead of alternating disk-saturated prefetch with disk-idle apply.
   *
   * <p>Each warming task reads {@link #currentRootHash} when it actually executes (not when the
   * window was submitted), so it traverses from the freshest root the apply thread has reached
   * rather than a snapshot captured windows earlier. With a deep lookahead the submission-time root
   * lags the warmed window's true pre-state by hundreds of blocks; every account modified in that
   * gap would be warmed along an already-orphaned path, wasting the read and never producing an
   * apply-time hit. Reading the live root instead keeps warming aimed at nodes apply will actually
   * touch. This is safe because warming only populates the hash-keyed cache and can never change
   * replay output (the per-block state-root verification in {@link #applyTrieLog} is the
   * correctness net); reading a root that is slightly ahead of the window is likewise harmless.
   *
   * @param layers the upcoming trie logs whose changed keys should be warmed
   * @return a future that completes once warming is submitted; an already-complete future when
   *     prefetch is disabled or the window is empty
   */
  public Future<?> prefetchAsync(final List<TrieLog> layers) {
    if (prefetchCoordinator == null || layers.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return prefetchCoordinator.submit(() -> prefetchFrom(layers));
  }

  private void prefetchFrom(final List<TrieLog> layers) {
    if (prefetchExecutor == null || layers.isEmpty()) {
      return;
    }

    // Union the changed accounts (and their changed storage slot hashes) across the whole window.
    final Map<Address, Set<Bytes32>> slotHashesByAccount = new HashMap<>();
    for (final TrieLog layer : layers) {
      for (final Address address : layer.getAccountChanges().keySet()) {
        slotHashesByAccount.computeIfAbsent(address, a -> new HashSet<>());
      }
      for (final var storageEntry : layer.getStorageChanges().entrySet()) {
        final Set<Bytes32> slotHashes =
            slotHashesByAccount.computeIfAbsent(storageEntry.getKey(), a -> new HashSet<>());
        for (final StorageSlotKey slotKey : storageEntry.getValue().keySet()) {
          slotHashes.add(Bytes32.wrap(slotKey.getSlotHash().getBytes()));
        }
      }
    }

    final NodeLoader nodeLoader = accountNodeLoader();
    final List<Callable<Void>> tasks = new ArrayList<>(slotHashesByAccount.size());
    for (final var entry : slotHashesByAccount.entrySet()) {
      final Address address = entry.getKey();
      final Set<Bytes32> slotHashes = entry.getValue();
      tasks.add(
          () -> {
            // Read the live root at execution time so warming follows the path apply is actually
            // about to walk, not a root snapshotted when this window was submitted (windows ago).
            warmAccount(currentRootHash, address, slotHashes, nodeLoader);
            return null;
          });
    }
    for (final Callable<Void> task : tasks) {
      prefetchExecutor.submit(task);
    }
  }

  /**
   * Warms the account-trie path to {@code address} from {@code baseRoot}, and (if the account
   * exists and has changed slots) the storage-trie paths to those slots. Best-effort: any read
   * failure is swallowed so prefetch never aborts the conversion.
   */
  private void warmAccount(
      final Bytes32 baseRoot,
      final Address address,
      final Set<Bytes32> slotHashes,
      final NodeLoader nodeLoader) {
    try {
      final StoredMerklePatriciaTrie<Bytes32, Bytes> accountTrie =
          new StoredMerklePatriciaTrie<>(nodeLoader, baseRoot, b -> b, b -> b);
      final Bytes32 addressHash = Bytes32.wrap(address.addressHash().getBytes());
      final Optional<Bytes> accountRlp = accountTrie.get(addressHash);
      if (slotHashes.isEmpty() || accountRlp.isEmpty()) {
        return;
      }
      final PmtStateTrieAccountValue account =
          PmtStateTrieAccountValue.readFrom(RLP.input(accountRlp.get()));
      final Bytes32 storageRoot = Bytes32.wrap(account.getStorageRoot().getBytes());
      if (storageRoot.equals(EMPTY_TRIE_ROOT)) {
        return;
      }
      final StoredMerklePatriciaTrie<Bytes32, Bytes> storageTrie =
          new StoredMerklePatriciaTrie<>(nodeLoader, storageRoot, b -> b, b -> b);
      for (final Bytes32 slotHash : slotHashes) {
        storageTrie.get(slotHash);
      }
    } catch (final RuntimeException e) {
      // Best-effort warming; a failed read must never abort the conversion.
    }
  }

  /**
   * Seeds the Forest storage with the genesis world state and sets the running root to the genesis
   * state root. This must be called before replaying any trie logs so that block-1 replay starts
   * from the correct base state.
   *
   * @param genesisState the genesis state to write
   * @param preimageStorage preimage storage used while writing the genesis world state
   * @param evmConfiguration the EVM configuration to use for the transient world state
   * @throws IllegalStateException if the written genesis state root does not match the genesis
   *     block header's state root
   */
  public void seedGenesis(
      final GenesisState genesisState,
      final WorldStatePreimageStorage preimageStorage,
      final EvmConfiguration evmConfiguration) {
    final ForestMutableWorldState genesisWorldState =
        new ForestMutableWorldState(forestStorage, preimageStorage, evmConfiguration);
    genesisState.writeStateTo(genesisWorldState);
    final Hash genesisRoot = genesisWorldState.rootHash();
    final Hash expected = genesisState.getBlock().getHeader().getStateRoot();
    if (!genesisRoot.getBytes().equals(expected.getBytes())) {
      throw new IllegalStateException(
          "Genesis state root " + genesisRoot + " does not match header " + expected);
    }
    currentRootHash = Bytes32.wrap(genesisRoot.getBytes());
  }

  /**
   * Replays a single Bonsai trie log into the Forest account state trie, persists the resulting
   * trie nodes, and verifies that the reconstructed state root matches the block's expected state
   * root.
   *
   * @param layer the Bonsai trie log describing the block's state diff
   * @param expectedStateRoot the canonical post-block state root to verify against
   * @return the reconstructed (and verified) state root
   * @throws IllegalStateException if the reconstructed state root does not match the expected root
   */
  public Hash applyTrieLog(final TrieLog layer, final Hash expectedStateRoot) {
    // Conversion writes bypass the WAL: the replay is idempotent and resumes from the last
    // persisted state root, so memtable data lost on a crash is simply re-derived. Skipping the
    // WAL frees write bandwidth, which is scarce on throughput-capped volumes during the bulk load.
    final ForestWorldStateKeyValueStorage.Updater updater = forestStorage.updater(true);
    try {
      final StoredMerklePatriciaTrie<Bytes32, Bytes> accountTrie =
          new StoredMerklePatriciaTrie<>(accountNodeLoader(), currentRootHash, b -> b, b -> b);

      final Map<Address, ? extends TrieLog.LogTuple<Bytes>> codeChanges = layer.getCodeChanges();
      for (final var entry : codeChanges.entrySet()) {
        final Bytes updatedCode = entry.getValue().getUpdated();
        if (updatedCode != null && !updatedCode.isEmpty()) {
          updater.putCode(updatedCode);
        }
      }

      final Map<Address, ? extends TrieLog.LogTuple<AccountValue>> accountChanges =
          layer.getAccountChanges();
      final Map<Address, ? extends Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>>>
          storageChangesByAddress = layer.getStorageChanges();

      // Phase 1: Rebuild per-account storage tries. Each account's storage trie is independent,
      // so when the prefetch pool is available we rebuild them in parallel — dispatching the
      // pointer-chasing reads across all threads rather than serialising them on the apply thread.
      final Map<Address, Bytes32> newStorageRoots;
      if (prefetchExecutor != null) {
        final ConcurrentLinkedQueue<Map.Entry<Bytes32, Bytes>> collectedNodes =
            new ConcurrentLinkedQueue<>();
        final ConcurrentHashMap<Address, Bytes32> parallelRoots = new ConcurrentHashMap<>();
        final List<Callable<Void>> tasks = new ArrayList<>();
        for (final var entry : accountChanges.entrySet()) {
          final Address address = entry.getKey();
          final TrieLog.LogTuple<AccountValue> change = entry.getValue();
          if (change.getUpdated() == null) {
            continue;
          }
          final Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>> slotChanges =
              storageChangesByAddress.get(address);
          if (slotChanges == null || slotChanges.isEmpty()) {
            continue;
          }
          final AccountValue prior = change.getPrior();
          final Bytes32 priorStorageRoot =
              prior == null ? EMPTY_TRIE_ROOT : Bytes32.wrap(prior.getStorageRoot().getBytes());
          final boolean cleared =
              prior == null
                  || slotChanges.values().stream().anyMatch(TrieLog.LogTuple::isClearedAtLeastOnce);
          tasks.add(
              () -> {
                parallelRoots.put(
                    address,
                    rebuildStorageRootCollecting(
                        priorStorageRoot, cleared, slotChanges, collectedNodes::add));
                return null;
              });
        }
        if (!tasks.isEmpty()) {
          try {
            prefetchExecutor.invokeAll(tasks);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        for (final Map.Entry<Bytes32, Bytes> nodeEntry : collectedNodes) {
          updater.putAccountStorageTrieNode(nodeEntry.getKey(), nodeEntry.getValue());
        }
        newStorageRoots = parallelRoots;
      } else {
        newStorageRoots = null;
      }

      // Phase 2: Update the shared account trie sequentially. Storage roots computed in phase 1
      // are reused; accounts without a prefetch pool fall back to inline sequential rebuild.
      for (final var entry : accountChanges.entrySet()) {
        final Address address = entry.getKey();
        final TrieLog.LogTuple<AccountValue> change = entry.getValue();
        final AccountValue updated = change.getUpdated();
        final Bytes32 addressHash = Bytes32.wrap(address.addressHash().getBytes());

        if (updated == null) {
          accountTrie.remove(addressHash);
          continue;
        }

        final Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>> slotChanges =
            storageChangesByAddress.get(address);
        if (slotChanges != null && !slotChanges.isEmpty()) {
          final Bytes32 storageRoot;
          if (newStorageRoots != null) {
            storageRoot = newStorageRoots.get(address);
          } else {
            final AccountValue prior = change.getPrior();
            final Bytes32 priorStorageRoot =
                prior == null ? EMPTY_TRIE_ROOT : Bytes32.wrap(prior.getStorageRoot().getBytes());
            final boolean cleared =
                prior == null
                    || slotChanges.values().stream()
                        .anyMatch(TrieLog.LogTuple::isClearedAtLeastOnce);
            storageRoot = rebuildStorageRoot(updater, priorStorageRoot, cleared, slotChanges);
          }
          if (!storageRoot.equals(Bytes32.wrap(updated.getStorageRoot().getBytes()))) {
            throw new IllegalStateException(
                "Reconstructed storage root for "
                    + address
                    + " ("
                    + Hash.wrap(storageRoot)
                    + ") does not match account storageRoot "
                    + updated.getStorageRoot());
          }
        }
        accountTrie.put(addressHash, RLP.encode(updated::writeTo));
      }

      accountTrie.commit(
          (location, hash, value) -> {
            updater.putAccountStateTrieNode(hash, value);
            if (nodeCache != null) {
              nodeCache.put(hash, value);
            }
          });
      final Bytes32 newRoot = accountTrie.getRootHash();
      if (!newRoot.equals(Bytes32.wrap(expectedStateRoot.getBytes()))) {
        throw new IllegalStateException(
            "Reconstructed state root "
                + Hash.wrap(newRoot)
                + " does not match expected "
                + expectedStateRoot);
      }
      updater.commit();
      currentRootHash = newRoot;
      return Hash.wrap(newRoot);
    } catch (final RuntimeException e) {
      updater.rollback();
      throw e;
    }
  }

  private Bytes32 rebuildStorageRootCollecting(
      final Bytes32 priorStorageRoot,
      final boolean cleared,
      final Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>> slotChanges,
      final Consumer<Map.Entry<Bytes32, Bytes>> nodeCollector) {
    final Bytes32 startRoot = cleared ? EMPTY_TRIE_ROOT : priorStorageRoot;
    final StoredMerklePatriciaTrie<Bytes32, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(storageNodeLoader(), startRoot, b -> b, b -> b);
    for (final var slot : slotChanges.entrySet()) {
      final Bytes32 slotHash = Bytes32.wrap(slot.getKey().getSlotHash().getBytes());
      final UInt256 value = slot.getValue().getUpdated();
      if (value == null || value.isZero()) {
        storageTrie.remove(slotHash);
      } else {
        storageTrie.put(slotHash, RLP.encode(o -> o.writeBytes(value.toMinimalBytes())));
      }
    }
    storageTrie.commit(
        (location, hash, value) -> {
          nodeCollector.accept(Map.entry(hash, value));
          if (nodeCache != null) {
            nodeCache.put(hash, value);
          }
        });
    return storageTrie.getRootHash();
  }

  private Bytes32 rebuildStorageRoot(
      final ForestWorldStateKeyValueStorage.Updater updater,
      final Bytes32 priorStorageRoot,
      final boolean cleared,
      final Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>> slotChanges) {
    return rebuildStorageRootCollecting(
        priorStorageRoot,
        cleared,
        slotChanges,
        e -> updater.putAccountStorageTrieNode(e.getKey(), e.getValue()));
  }
}
