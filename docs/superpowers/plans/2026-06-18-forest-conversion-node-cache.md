# Forest Conversion Node Cache + Resume Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `x-convert-to-forest` fast and resumable by caching trie nodes across blocks (read-through + write-through) and resuming from the last committed block, with no on-disk format change.

**Architecture:** A persistent memory-bounded Caffeine cache (`Bytes32 node hash → Bytes encoded node`) lives on `BonsaiTrieLogToForestConverter` across all blocks. The per-block `StoredMerklePatriciaTrie` is still created fresh (its `commit()` discards in-memory nodes), but its `NodeLoader`s read through the cache and committed nodes are written through, so the hot upper account-trie nodes stay in RAM instead of being re-read from disk. On startup the subcommand binary-searches the largest block whose account-state root node is already on disk and resumes from the next block.

**Tech Stack:** Java 21+, Gradle, Caffeine (`com.github.ben-manes.caffeine`), Besu `MemoryBoundCache`, JUnit 5 + AssertJ.

## Global Constraints

- Source data-storage-format must be BONSAI (existing `checkArgument` in the subcommand — unchanged).
- No change to the on-disk Forest format or any persisted schema; resume relies only on data already on disk.
- No new public methods on `SegmentedKeyValueStorage` / `plugin-api` (so the `plugin-api` `knownHash` does NOT change).
- Cache is on-heap and memory-bounded by bytes; default 1024 MB; a size of 0 disables it and reproduces current behaviour exactly.
- Per-block state-root verification in `applyTrieLog` must remain intact (it is the resume safety net).
- License header on every new file (copy verbatim from an existing file in the same module).
- Run `./gradlew :ethereum:core:spotlessApply :app:spotlessApply` before building.

---

### Task 1: Cross-block node cache in `BonsaiTrieLogToForestConverter`

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverter.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverterTest.java`

**Interfaces:**
- Consumes: `org.hyperledger.besu.util.cache.MemoryBoundCache<K,V>` — constructor `MemoryBoundCache(long maxBytes, BiToIntFunction<K,V> weigher)`, methods `void put(K,V)`, `@Nullable V getIfPresent(K)`, `double hitRate()`, `long estimatedSize()`.
- Consumes: `ForestWorldStateKeyValueStorage.getAccountStateTrieNode(Bytes32)`, `getAccountStorageTrieNode(Bytes32)`, both returning `Optional<Bytes>`.
- Produces (used by Task 3):
  - `BonsaiTrieLogToForestConverter(ForestWorldStateKeyValueStorage forestStorage, long cacheMaxBytes)` — new 2-arg constructor; `cacheMaxBytes <= 0` disables the cache.
  - `BonsaiTrieLogToForestConverter(ForestWorldStateKeyValueStorage forestStorage)` — existing 1-arg constructor preserved, delegates with `cacheMaxBytes = 0`.
  - `void resumeFrom(Hash root)` — sets the running root hash (used to resume).
  - `double cacheHitRate()` — returns the cache hit rate, or `-1.0` if the cache is disabled.
  - `long cacheEstimatedSize()` — returns the cache entry estimate, or `0` if disabled.

- [ ] **Step 1: Write the failing tests**

Add these imports to the test file (after the existing imports):

```java
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
```
(skip if already present)

Add these test methods to `BonsaiTrieLogToForestConverterTest`:

```java
  @Test
  void cacheEnabledProducesSameRootAcrossTwoBlocksAsOracle() {
    // Oracle: block 1 creates ALICE, block 2 bumps her nonce.
    final ForestMutableWorldState oracle = oracle(forestStorage());
    final WorldUpdater u1 = oracle.updater();
    final MutableAccount a1 = u1.createAccount(ALICE);
    a1.setNonce(7);
    a1.setBalance(Wei.of(1234));
    u1.commit();
    oracle.persist(null);
    final Hash root1 = oracle.rootHash();

    final WorldUpdater u2 = oracle.updater();
    final MutableAccount a2 = u2.getAccount(ALICE);
    a2.setNonce(8);
    u2.commit();
    oracle.persist(null);
    final Hash root2 = oracle.rootHash();

    final TrieLogLayer layer1 = new TrieLogLayer();
    layer1.addAccountChange(ALICE, null, account(7, 1234));
    final TrieLogLayer layer2 = new TrieLogLayer();
    layer2.addAccountChange(ALICE, account(7, 1234), account(8, 1234));

    // cacheMaxBytes = 1 MiB so the cache is active during the test
    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage(), 1024 * 1024);
    assertThat(converter.applyTrieLog(layer1, root1)).isEqualTo(root1);
    assertThat(converter.applyTrieLog(layer2, root2)).isEqualTo(root2);
  }

  @Test
  void cacheIsPopulatedAndServesCrossBlockReads() {
    final ForestMutableWorldState oracle = oracle(forestStorage());
    final WorldUpdater u1 = oracle.updater();
    final MutableAccount a1 = u1.createAccount(ALICE);
    a1.setNonce(7);
    a1.setBalance(Wei.of(1234));
    u1.commit();
    oracle.persist(null);
    final Hash root1 = oracle.rootHash();

    final WorldUpdater u2 = oracle.updater();
    final MutableAccount a2 = u2.getAccount(ALICE);
    a2.setNonce(8);
    u2.commit();
    oracle.persist(null);
    final Hash root2 = oracle.rootHash();

    final TrieLogLayer layer1 = new TrieLogLayer();
    layer1.addAccountChange(ALICE, null, account(7, 1234));
    final TrieLogLayer layer2 = new TrieLogLayer();
    layer2.addAccountChange(ALICE, account(7, 1234), account(8, 1234));

    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage(), 1024 * 1024);
    converter.applyTrieLog(layer1, root1);
    assertThat(converter.cacheEstimatedSize()).isGreaterThan(0L);

    // Block 2 re-traverses block-1 nodes; they must be served from the cache.
    converter.applyTrieLog(layer2, root2);
    assertThat(converter.cacheHitRate()).isGreaterThan(0.0);
  }

  @Test
  void disabledCacheReportsSentinels() {
    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage(), 0);
    assertThat(converter.cacheHitRate()).isEqualTo(-1.0);
    assertThat(converter.cacheEstimatedSize()).isEqualTo(0L);
  }

  @Test
  void resumeFromSetsRunningRoot() {
    final ForestMutableWorldState oracle = oracle(forestStorage());
    final WorldUpdater u1 = oracle.updater();
    final MutableAccount a1 = u1.createAccount(ALICE);
    a1.setNonce(7);
    a1.setBalance(Wei.of(1234));
    u1.commit();
    oracle.persist(null);
    final Hash root1 = oracle.rootHash();

    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage(), 0);
    converter.resumeFrom(root1);
    assertThat(converter.currentRootHash()).isEqualTo(root1);
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :ethereum:core:test --tests "BonsaiTrieLogToForestConverterTest"`
Expected: FAIL — compilation error (no 2-arg constructor / no `cacheHitRate` / `cacheEstimatedSize` / `resumeFrom`).

- [ ] **Step 3: Implement the cache in the converter**

In `BonsaiTrieLogToForestConverter.java`, add imports:

```java
import org.hyperledger.besu.util.cache.MemoryBoundCache;
```

Replace the fields and constructor (lines ~49–62) with:

```java
  private static final Bytes32 EMPTY_TRIE_ROOT = Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes());

  private final ForestWorldStateKeyValueStorage forestStorage;
  // Cross-block node cache (hash -> encoded node). Null when disabled (cacheMaxBytes <= 0).
  private final MemoryBoundCache<Bytes32, Bytes> nodeCache;
  private Bytes32 currentRootHash;

  /**
   * Creates a converter that writes reconstructed Forest trie nodes into the given storage, with no
   * cross-block node cache.
   *
   * @param forestStorage the Forest world-state storage to populate
   */
  public BonsaiTrieLogToForestConverter(final ForestWorldStateKeyValueStorage forestStorage) {
    this(forestStorage, 0);
  }

  /**
   * Creates a converter that writes reconstructed Forest trie nodes into the given storage and
   * caches trie nodes across blocks to avoid re-reading hot nodes from disk.
   *
   * @param forestStorage the Forest world-state storage to populate
   * @param cacheMaxBytes maximum on-heap size in bytes of the cross-block node cache; values &lt;= 0
   *     disable the cache
   */
  public BonsaiTrieLogToForestConverter(
      final ForestWorldStateKeyValueStorage forestStorage, final long cacheMaxBytes) {
    this.forestStorage = forestStorage;
    this.currentRootHash = EMPTY_TRIE_ROOT;
    this.nodeCache =
        cacheMaxBytes > 0
            ? new MemoryBoundCache<>(cacheMaxBytes, (hash, node) -> node.size() + Bytes32.SIZE)
            : null;
  }
```

Add, just after the `currentRootHash()` method (~line 71):

```java
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
```

Replace the account loader (lines ~113–116) so it reads through the cache:

```java
      final NodeLoader accountLoader =
          (location, hash) -> cachingLoad(forestStorage.getAccountStateTrieNode(hash), hash);
```

Replace the storage loader in `rebuildStorageRoot` (lines ~189–190) so it reads through the cache:

```java
    final NodeLoader storageLoader =
        (location, hash) -> cachingLoad(forestStorage.getAccountStorageTrieNode(hash), hash);
```

Add a private read-through helper method to the class (e.g. just below `cacheEstimatedSize()`):

```java
  private Optional<Bytes> cachingLoad(final Optional<Bytes> storageLookup, final Bytes32 hash) {
    if (nodeCache == null) {
      return storageLookup;
    }
    final Bytes cached = nodeCache.getIfPresent(hash);
    if (cached != null) {
      return Optional.of(cached);
    }
    storageLookup.ifPresent(node -> nodeCache.put(hash, node));
    return storageLookup;
  }
```

Note: the `NodeLoader` interface returns `Optional<Bytes>` keyed on `(location, hash)`. We key the cache on `hash` only, matching Forest's hash-only storage keying.

Replace the account-trie commit (line ~165) with a write-through callback:

```java
      accountTrie.commit(
          (location, hash, value) -> {
            updater.putAccountStateTrieNode(hash, value);
            if (nodeCache != null) {
              nodeCache.put(hash, value);
            }
          });
```

Replace the storage-trie commit (line ~202) with a write-through callback:

```java
    storageTrie.commit(
        (location, hash, value) -> {
          updater.putAccountStorageTrieNode(hash, value);
          if (nodeCache != null) {
            nodeCache.put(hash, value);
          }
        });
```

- [ ] **Step 4: Format, then run tests to verify they pass**

Run: `./gradlew :ethereum:core:spotlessApply && ./gradlew :ethereum:core:test --tests "BonsaiTrieLogToForestConverterTest"`
Expected: PASS (all existing tests plus the four new ones).

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverter.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverterTest.java
git commit -m "feat: cross-block node cache for bonsai-to-forest conversion

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Resume-point search helper

**Files:**
- Create: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/forest/migration/ForestConversionResume.java`
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/forest/migration/ForestConversionResumeTest.java`

**Interfaces:**
- Produces (used by Task 3):
  - `static long ForestConversionResume.findResumeBlock(long head, java.util.function.LongFunction<Hash> stateRootByBlock, java.util.function.Predicate<Hash> rootPresent)` — returns the largest block number `K` in `[0, head]` whose state root is present on disk; `0` means only genesis is present (start replay at block 1). `stateRootByBlock.apply(0)` must return the genesis state root.

- [ ] **Step 1: Write the failing test**

Create `ForestConversionResumeTest.java`:

```java
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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;

import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

class ForestConversionResumeTest {

  // Each block n maps to a distinct synthetic state root derived from n.
  private static final LongFunction<Hash> ROOT_BY_BLOCK =
      n -> Hash.hash(org.apache.tuweni.bytes.Bytes.ofUnsignedLong(n));

  private static Predicate<Hash> presentForBlocks(final long highestPresent) {
    final Set<Hash> present = new java.util.HashSet<>();
    for (long n = 0; n <= highestPresent; n++) {
      present.add(ROOT_BY_BLOCK.apply(n));
    }
    return present::contains;
  }

  @Test
  void resumesAtHighestPresentBlock() {
    assertThat(ForestConversionResume.findResumeBlock(10, ROOT_BY_BLOCK, presentForBlocks(5)))
        .isEqualTo(5L);
  }

  @Test
  void resumesAtGenesisWhenOnlyGenesisPresent() {
    assertThat(ForestConversionResume.findResumeBlock(10, ROOT_BY_BLOCK, presentForBlocks(0)))
        .isEqualTo(0L);
  }

  @Test
  void resumesAtHeadWhenAllPresent() {
    assertThat(ForestConversionResume.findResumeBlock(10, ROOT_BY_BLOCK, presentForBlocks(10)))
        .isEqualTo(10L);
  }

  @Test
  void headZeroReturnsZero() {
    assertThat(ForestConversionResume.findResumeBlock(0, ROOT_BY_BLOCK, presentForBlocks(0)))
        .isEqualTo(0L);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "ForestConversionResumeTest"`
Expected: FAIL — `ForestConversionResume` does not exist (compilation error).

- [ ] **Step 3: Implement the helper**

Create `ForestConversionResume.java`:

```java
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

import org.hyperledger.besu.datatypes.Hash;

import java.util.function.LongFunction;
import java.util.function.Predicate;

/**
 * Determines where a Forest conversion should resume by probing which blocks' account-state root
 * nodes are already present in the Forest storage. Because Forest nodes are content-addressed,
 * presence of a block's state-root node is a reliable marker that the block was committed.
 */
public final class ForestConversionResume {

  private ForestConversionResume() {}

  /**
   * Returns the largest block number {@code K} in {@code [0, head]} whose state root is present in
   * Forest storage. {@code 0} means only genesis is present, so replay should start at block 1.
   *
   * <p>A binary search locates the boundary assuming presence is monotonic (blocks 1..K committed,
   * the rest not). A forward scan then extends past any coincidental state-root reuse so the result
   * is never below the true highest committed block.
   *
   * @param head the chain head block number
   * @param stateRootByBlock maps a block number to its canonical state root; block 0 is genesis
   * @param rootPresent tests whether a given state root's account node exists in Forest storage
   * @return the resume block number K (start replay at K+1)
   */
  public static long findResumeBlock(
      final long head,
      final LongFunction<Hash> stateRootByBlock,
      final Predicate<Hash> rootPresent) {
    long lo = 0;
    long hi = head;
    long boundary = 0;
    while (lo <= hi) {
      final long mid = lo + (hi - lo) / 2;
      if (rootPresent.test(stateRootByBlock.apply(mid))) {
        boundary = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    // Extend past any coincidental root reuse beyond the binary-search boundary.
    while (boundary < head && rootPresent.test(stateRootByBlock.apply(boundary + 1))) {
      boundary++;
    }
    return boundary;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :ethereum:core:spotlessApply && ./gradlew :ethereum:core:test --tests "ForestConversionResumeTest"`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/forest/migration/ForestConversionResume.java \
        ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/forest/migration/ForestConversionResumeTest.java
git commit -m "feat: resume-point search for bonsai-to-forest conversion

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Wire cache flag, resume, and cache logging into the subcommand

**Files:**
- Modify: `app/src/main/java/org/hyperledger/besu/cli/subcommands/storage/ConvertToForestSubCommand.java`

**Interfaces:**
- Consumes: `BonsaiTrieLogToForestConverter(ForestWorldStateKeyValueStorage, long)`, `converter.resumeFrom(Hash)`, `converter.cacheHitRate()`, `converter.cacheEstimatedSize()` (Task 1); `ForestConversionResume.findResumeBlock(long, LongFunction<Hash>, Predicate<Hash>)` (Task 2).
- Consumes: `ForestWorldStateKeyValueStorage.isWorldStateAvailable(Bytes32)` (existing; `Hash` is a `Bytes32`, so a `Hash` may be passed directly).
- Consumes: `genesisState.getBlock().getHeader().getStateRoot()` for block 0's root.

- [ ] **Step 1: Add the cache-size option field**

Add imports near the existing imports:

```java
import org.hyperledger.besu.ethereum.trie.forest.migration.ForestConversionResume;

import java.util.function.LongFunction;
```

Add the option field after `shouldLogProgress` (~line 89):

```java
  @CommandLine.Option(
      names = {"--Xx-convert-cache-size-mb"},
      description =
          "EXPERIMENTAL: on-heap size (MB) of the cross-block trie-node cache used during conversion; 0 disables it (default: ${DEFAULT-VALUE})",
      hidden = true)
  private long convertCacheSizeMb = 1024;
```

- [ ] **Step 2: Construct the converter with the cache size**

Replace the converter construction (line ~159):

```java
      final BonsaiTrieLogToForestConverter converter =
          new BonsaiTrieLogToForestConverter(forest, convertCacheSizeMb * 1024L * 1024L);
```

- [ ] **Step 3: Compute the resume point and start the loop there**

After `final long head = blockchain.getChainHeadBlockNumber();` (~line 169), add:

```java
      final Hash genesisStateRoot = genesisState.getBlock().getHeader().getStateRoot();
      final LongFunction<Hash> stateRootByBlock =
          number ->
              number == 0
                  ? genesisStateRoot
                  : blockchain
                      .getBlockHeader(
                          blockchain
                              .getBlockHashByNumber(number)
                              .orElseThrow(
                                  () ->
                                      new IllegalStateException(
                                          "Missing block hash for block " + number)))
                      .orElseThrow(
                          () ->
                              new IllegalStateException("Missing block header for block " + number))
                      .getStateRoot();

      final long resumeBlock =
          ForestConversionResume.findResumeBlock(
              head, stateRootByBlock, forest::isWorldStateAvailable);
      if (resumeBlock > 0) {
        final Hash resumeRoot = stateRootByBlock.apply(resumeBlock);
        converter.resumeFrom(resumeRoot);
        LOG.info("Resuming conversion from block {} (root={})", resumeBlock, resumeRoot);
      }
      if (resumeBlock >= head) {
        LOG.info("Conversion already complete to head {}", head);
        flipMetadataToForest(dataDir);
        LOG.info("Flipped database metadata to FOREST format");
        return;
      }
```

Change the loop start (line ~173) from `for (long number = 1; ...)` to:

```java
      for (long number = resumeBlock + 1; number <= head; number++) {
```

Note: there is a `try { ... } finally { controller.close(); }` around the loop. The early `return` above is inside that `try`, so `controller.close()` still runs.

- [ ] **Step 4: Add cache stats to the progress log**

Replace the `LOG.info("Converted ...")` call inside the throttled block (lines ~208–214) with:

```java
              LOG.info(
                  "Converted {} / {} blocks ({}%), {} blocks/s, ETA {}, cache hit-rate {} ({} entries)",
                  blockNumber,
                  head,
                  String.format("%.1f", percentComplete),
                  String.format("%.0f", blocksPerSecond),
                  eta,
                  String.format("%.3f", converter.cacheHitRate()),
                  converter.cacheEstimatedSize());
```

- [ ] **Step 5: Format and compile**

Run: `./gradlew :app:spotlessApply && ./gradlew :app:compileJava :ethereum:core:compileJava`
Expected: BUILD SUCCESSFUL, no compile errors.

- [ ] **Step 6: Build the modules and run the affected tests**

Run: `./gradlew :ethereum:core:build :app:build -x test && ./gradlew :ethereum:core:test --tests "BonsaiTrieLogToForestConverterTest" --tests "ForestConversionResumeTest"`
Expected: BUILD SUCCESSFUL; both test classes PASS.

- [ ] **Step 7: Commit**

```bash
git add app/src/main/java/org/hyperledger/besu/cli/subcommands/storage/ConvertToForestSubCommand.java
git commit -m "feat: cache-size flag, resume, and cache logging for x-convert-to-forest

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Deployment / resume verification (manual, post-implementation)

1. Build the distribution (`./gradlew installDist` or the project's standard build).
2. Stop the running conversion (PID 10336 on `dev-elc-bu-nb-mainnet-jason-forest-full-1`).
3. Restart with the new build, recommended JVM/flags: `-Xmx4g` and `--Xx-convert-cache-size-mb=1024`.
4. Confirm the log shows `Resuming conversion from block {K}` at roughly the prior progress (not block 1), a rising `cache hit-rate`, and improved `blocks/s` vs the pre-change baseline.

## Self-Review Notes

- **Spec coverage:** node cache (Task 1: read-through loaders + write-through commits + memory-bound size + sentinels), cache-size flag with 0-disables (Task 3 option + Task 1 constructor), resume by root-presence binary search with forward-scan boundary safety (Task 2 + Task 3 wiring), already-complete handling (Task 3 Step 3), cache hit-rate logging (Task 3 Step 4), no schema change / `plugin-api` hash untouched (Global Constraints), per-block verification preserved (untouched in `applyTrieLog`).
- **Type consistency:** `BonsaiTrieLogToForestConverter(ForestWorldStateKeyValueStorage, long)`, `resumeFrom(Hash)`, `cacheHitRate()→double`, `cacheEstimatedSize()→long`, `findResumeBlock(long, LongFunction<Hash>, Predicate<Hash>)→long` are used identically across tasks.
- **Predicate type:** `forest::isWorldStateAvailable` is `Predicate<Bytes32>`; `Hash` implements `Bytes32`, so it satisfies `Predicate<Hash>` via method reference target typing.
