# JIT Account-Path Pre-Warm Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the phase-2 account-trie coverage gap in high-churn regions by warming each
changed account's trie path from the exact pre-mutation root, in parallel, as part of the
same per-block batch phase 1 already uses for storage-trie rebuild.

**Architecture:** Extend `BonsaiTrieLogToForestConverter.applyTrieLog`'s existing
`prefetchExecutor.invokeAll(tasks)` batch (currently storage-trie rebuild only) with one
additional read-only warm task per changed account, keyed off `currentRootHash` captured at
the start of the call. Gate the new tasks behind a dedicated boolean flag
(`warmAccountPaths`, default `true`) threaded through a new constructor overload and a new
hidden CLI option, so the behavior can be disabled independently of existing prefetch/cache
flags.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Gradle. No new dependencies.

**Design doc:** `docs/superpowers/specs/2026-07-24-forest-conversion-jit-account-warm-design.md`

## Global Constraints

- Do not change replay semantics: the account-trie mutation loop, RLP encoding, and the
  post-commit state-root comparison against `expectedStateRoot` must be byte-for-byte
  identical to today. Warming is read-only and best-effort (swallow exceptions).
- Preserve all 19 existing call sites of `BonsaiTrieLogToForestConverter`'s constructors
  (`app/.../ConvertToForestSubCommand.java` and the test file) — add a new constructor
  overload, do not change existing signatures.
- New CLI flag name: `--Xx-convert-warm-account-paths` (hidden, boolean, default `true`).
- Run `./gradlew :ethereum:core:spotlessApply :app:spotlessApply -q` before each compile
  check; this codebase enforces Spotless formatting.

---

### Task 1: Add the `warmAccountPaths` field and new constructor overload

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverter.java:96` (field), `:136-168` (constructors)
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverterTest.java`

**Interfaces:**
- Produces: `new BonsaiTrieLogToForestConverter(ForestWorldStateKeyValueStorage, long cacheMaxBytes, int prefetchThreads, boolean warmAccountPaths)` — the new 4-arg constructor later tasks and the CLI wiring depend on. The existing 3-arg constructor continues to work unchanged (delegates to the 4-arg one with `warmAccountPaths=true`).

- [ ] **Step 1: Write the failing test**

Add to `BonsaiTrieLogToForestConverterTest.java`, immediately after the existing
`emptyConverterReportsEmptyTrieRoot` test (after line 111):

```java
  @Test
  void constructorWithWarmAccountPathsFlagReportsEmptyTrieRoot() {
    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage(), 1024 * 1024, 4, false);
    try {
      assertThat(converter.currentRootHash()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    } finally {
      converter.close();
    }
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ethereum:core:test --tests "*BonsaiTrieLogToForestConverterTest*" 2>&1 | tail -40`

Expected: compile error — `The constructor BonsaiTrieLogToForestConverter(ForestWorldStateKeyValueStorage, int, int, boolean) is undefined` (or similar 4-arg mismatch).

- [ ] **Step 3: Add the field**

In `BonsaiTrieLogToForestConverter.java`, immediately after the existing field block
(after `private volatile Bytes32 currentRootHash;` at line 86, before the
`applyAccountHits`/`applyAccountMisses` counters comment at line 88), add:

```java
  // Gates the JIT account-path warm tasks added to phase 1's parallel batch in applyTrieLog.
  // Independent of prefetchThreads (which, at 0, disables ALL parallel work including the
  // already-working storage-trie rebuild) so this specific behavior can be disabled on its own.
  private final boolean warmAccountPaths;
```

- [ ] **Step 4: Add the 4-arg constructor and update the 3-arg one to delegate**

Replace the existing 3-arg constructor (lines 136-168):

```java
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
```

with:

```java
  public BonsaiTrieLogToForestConverter(
      final ForestWorldStateKeyValueStorage forestStorage,
      final long cacheMaxBytes,
      final int prefetchThreads) {
    this(forestStorage, cacheMaxBytes, prefetchThreads, true);
  }

  /**
   * Creates a converter identical to {@link #BonsaiTrieLogToForestConverter(
   * ForestWorldStateKeyValueStorage, long, int)}, with explicit control over whether phase 1's
   * parallel batch also warms each changed account's trie path from the exact current root ahead
   * of phase 2 (see {@link #applyTrieLog}).
   *
   * @param forestStorage the Forest world-state storage to populate
   * @param cacheMaxBytes maximum on-heap size in bytes of the cross-block node cache; values &lt;=
   *     0 disable the cache
   * @param prefetchThreads number of parallel reader threads used to warm the cache ahead of
   *     replay; values &lt;= 0 (or a disabled cache) disable all parallel warming
   * @param warmAccountPaths whether phase 1's parallel batch also warms each changed account's
   *     trie path from the exact current root; has no effect when parallel warming is disabled
   */
  public BonsaiTrieLogToForestConverter(
      final ForestWorldStateKeyValueStorage forestStorage,
      final long cacheMaxBytes,
      final int prefetchThreads,
      final boolean warmAccountPaths) {
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
    this.warmAccountPaths = warmAccountPaths;
  }
```

- [ ] **Step 5: Format, then run test to verify it passes**

Run:
```bash
./gradlew :ethereum:core:spotlessApply -q
./gradlew :ethereum:core:test --tests "*BonsaiTrieLogToForestConverterTest*" 2>&1 | tail -40
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 6: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverter.java ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverterTest.java
git commit -m "feat: add warmAccountPaths constructor overload to BonsaiTrieLogToForestConverter"
```

---

### Task 2: Wire the JIT account-path warm into `applyTrieLog`'s phase-1 batch

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverter.java` (new private helper method; phase-1 block inside `applyTrieLog`)
- Test: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverterTest.java`

**Interfaces:**
- Consumes: `warmAccountPaths` field and 4-arg constructor from Task 1; existing `accountNodeLoader()` (returns `NodeLoader`), `currentRootHash` (`Bytes32`), `prefetchExecutor` (`ExecutorService`), all already defined in this class.
- Produces: `private void warmAccountPath(Bytes32 root, Address address, NodeLoader loader)` — a private helper, not consumed outside this class; documented here so Task 3's CLI wiring (which doesn't touch this method) and any future reader understand its shape.

- [ ] **Step 1: Write the failing tests**

Add to `BonsaiTrieLogToForestConverterTest.java`, immediately after
`prefetchIsNoOpWhenThreadsZero` (after line 548, before `seedGenesisMatchesGenesisStateRoot`):

```java
  @Test
  void warmAccountPathsWarmsMultipleAccountsAndReplayMatches() {
    // Oracle: block 1 creates ALICE and CONTRACT; block 2 bumps both nonces.
    final ForestMutableWorldState oracle = oracle(forestStorage());
    final WorldUpdater u1 = oracle.updater();
    final MutableAccount a1 = u1.createAccount(ALICE);
    a1.setNonce(1);
    a1.setBalance(Wei.of(100));
    final MutableAccount c1 = u1.createAccount(CONTRACT);
    c1.setNonce(1);
    c1.setBalance(Wei.of(200));
    u1.commit();
    oracle.persist(null);
    final Hash root1 = oracle.rootHash();

    final WorldUpdater u2 = oracle.updater();
    final MutableAccount a2 = u2.getAccount(ALICE);
    a2.setNonce(2);
    final MutableAccount c2 = u2.getAccount(CONTRACT);
    c2.setNonce(2);
    u2.commit();
    oracle.persist(null);
    final Hash root2 = oracle.rootHash();

    final TrieLogLayer layer1 = new TrieLogLayer();
    layer1.addAccountChange(ALICE, null, account(1, 100));
    layer1.addAccountChange(CONTRACT, null, account(1, 200));
    final TrieLogLayer layer2 = new TrieLogLayer();
    layer2.addAccountChange(ALICE, account(1, 100), account(2, 100));
    layer2.addAccountChange(CONTRACT, account(1, 200), account(2, 200));

    // Cache + 4 prefetch threads enabled, warmAccountPaths defaults to true — block 2's own
    // phase-1 batch should warm BOTH ALICE's and CONTRACT's account-trie paths from root1 before
    // phase 2 walks them. Neither account has storage changes, so this is the only work in the
    // batch; it must still run and must not affect the reconstructed roots.
    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage(), 1024 * 1024, 4);
    try {
      assertThat(converter.applyTrieLog(layer1, root1)).isEqualTo(root1);
      assertThat(converter.applyTrieLog(layer2, root2)).isEqualTo(root2);
    } finally {
      converter.close();
    }
  }

  @Test
  void warmAccountPathsDisabledStillProducesCorrectRoots() {
    // Same scenario as above but with warmAccountPaths=false — the kill switch must skip the new
    // warm tasks without affecting correctness (phase 2 still reads whatever it needs, just
    // without the JIT pre-warm).
    final ForestMutableWorldState oracle = oracle(forestStorage());
    final WorldUpdater u1 = oracle.updater();
    final MutableAccount a1 = u1.createAccount(ALICE);
    a1.setNonce(1);
    a1.setBalance(Wei.of(100));
    final MutableAccount c1 = u1.createAccount(CONTRACT);
    c1.setNonce(1);
    c1.setBalance(Wei.of(200));
    u1.commit();
    oracle.persist(null);
    final Hash root1 = oracle.rootHash();

    final WorldUpdater u2 = oracle.updater();
    final MutableAccount a2 = u2.getAccount(ALICE);
    a2.setNonce(2);
    final MutableAccount c2 = u2.getAccount(CONTRACT);
    c2.setNonce(2);
    u2.commit();
    oracle.persist(null);
    final Hash root2 = oracle.rootHash();

    final TrieLogLayer layer1 = new TrieLogLayer();
    layer1.addAccountChange(ALICE, null, account(1, 100));
    layer1.addAccountChange(CONTRACT, null, account(1, 200));
    final TrieLogLayer layer2 = new TrieLogLayer();
    layer2.addAccountChange(ALICE, account(1, 100), account(2, 100));
    layer2.addAccountChange(CONTRACT, account(1, 200), account(2, 200));

    final BonsaiTrieLogToForestConverter converter =
        new BonsaiTrieLogToForestConverter(forestStorage(), 1024 * 1024, 4, false);
    try {
      assertThat(converter.applyTrieLog(layer1, root1)).isEqualTo(root1);
      assertThat(converter.applyTrieLog(layer2, root2)).isEqualTo(root2);
    } finally {
      converter.close();
    }
  }
```

- [ ] **Step 2: Run tests to verify current behavior**

Run: `./gradlew :ethereum:core:test --tests "*BonsaiTrieLogToForestConverterTest*" 2>&1 | tail -40`

Expected: both new tests **PASS** already — the warm tasks aren't wired in yet, so this only
exercises the existing phase-1/phase-2 logic (which is already correct). This is expected;
these tests exist to catch a regression once the wiring is added in Step 3, not to fail now.
Confirm `BUILD SUCCESSFUL` before proceeding so you have a clean baseline.

- [ ] **Step 3: Add the `warmAccountPath` helper method**

In `BonsaiTrieLogToForestConverter.java`, immediately after the `warmAccount` method (after
line 415, before the `seedGenesis` method), add:

```java
  /**
   * Warms the account-trie path to {@code address} from {@code root} by traversing it with a
   * fresh trie instance over the shared node cache. Read-only and best-effort: any failure is
   * swallowed, since phase 2 re-reads the authoritative node by hash on a subsequent miss anyway.
   * Unlike {@link #warmAccount}, this warms only the account path (no storage slots) and is
   * intended to run synchronously, once per block, from the exact pre-mutation root — see the
   * phase-1 batch in {@link #applyTrieLog}.
   */
  private void warmAccountPath(final Bytes32 root, final Address address, final NodeLoader loader) {
    try {
      new StoredMerklePatriciaTrie<>(loader, root, b -> b, b -> b)
          .get(Bytes32.wrap(address.addressHash().getBytes()));
    } catch (final RuntimeException e) {
      // Best-effort warming; phase 2 re-reads authoritatively on a miss.
    }
  }
```

- [ ] **Step 4: Wire the warm tasks into `applyTrieLog`'s phase-1 batch**

In `applyTrieLog`, replace the existing phase-1 block:

```java
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
```

with:

```java
        // JIT account-path warm: read every changed account's trie path from the exact
        // pre-mutation root (captured now, before phase 2 mutates anything), in the same parallel
        // batch as the storage-trie rebuild above. Unlike the window-level prefetch (which warms
        // from a root a window or more behind), this has zero staleness — it warms exactly what
        // phase 2 is about to walk.
        if (warmAccountPaths) {
          final Bytes32 warmRoot = currentRootHash;
          final NodeLoader accountLoader = accountNodeLoader();
          for (final Address address : accountChanges.keySet()) {
            tasks.add(
                () -> {
                  warmAccountPath(warmRoot, address, accountLoader);
                  return null;
                });
          }
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
```

- [ ] **Step 5: Format, then run the full converter test suite to verify it passes**

Run:
```bash
./gradlew :ethereum:core:spotlessApply -q
./gradlew :ethereum:core:test --tests "*BonsaiTrieLogToForestConverterTest*" 2>&1 | tail -40
```
Expected: `BUILD SUCCESSFUL`, all tests (including the two new ones and all pre-existing
ones — `prefetchAsyncWarmsFromCurrentRootAndReplayMatches`,
`prefetchAcrossWholeWindowProducesSameRootsAsOracle`, etc.) pass. The pre-existing tests now
also exercise the new warm tasks by default (`warmAccountPaths` defaults to `true`), so a
regression here would show up as a root mismatch in any of them.

- [ ] **Step 6: Commit**

```bash
git add ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverter.java ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/forest/migration/BonsaiTrieLogToForestConverterTest.java
git commit -m "feat: JIT account-path pre-warm in applyTrieLog phase-1 batch"
```

---

### Task 3: CLI flag wiring and final verification

**Files:**
- Modify: `app/src/main/java/org/hyperledger/besu/cli/subcommands/storage/ConvertToForestSubCommand.java:124-129` (new option, after `convertPrefetchLookahead`), `:199-201` (constructor call)

**Interfaces:**
- Consumes: `BonsaiTrieLogToForestConverter`'s 4-arg constructor from Task 1/2.
- Produces: nothing consumed by later tasks — this is the final task.

- [ ] **Step 1: Add the CLI option field**

In `ConvertToForestSubCommand.java`, immediately after the `convertPrefetchLookahead` field
(after line 129, before the `@SuppressWarnings("unused") @ParentCommand` block), add:

```java
  @CommandLine.Option(
      names = {"--Xx-convert-warm-account-paths"},
      description =
          "EXPERIMENTAL: pre-warm each changed account's trie path from the exact current root before replay, in the same parallel batch as storage-trie rebuild; closes coverage gaps left by window-level prefetch in high-churn regions (default: ${DEFAULT-VALUE})",
      hidden = true)
  private boolean convertWarmAccountPaths = true;
```

- [ ] **Step 2: Pass the flag into the converter constructor**

Replace:

```java
      final BonsaiTrieLogToForestConverter converter =
          new BonsaiTrieLogToForestConverter(
              forest, convertCacheSizeMb * 1024L * 1024L, convertPrefetchThreads);
```

with:

```java
      final BonsaiTrieLogToForestConverter converter =
          new BonsaiTrieLogToForestConverter(
              forest,
              convertCacheSizeMb * 1024L * 1024L,
              convertPrefetchThreads,
              convertWarmAccountPaths);
```

- [ ] **Step 3: Format, then compile both modules and run the full converter test suite**

Run:
```bash
./gradlew :app:spotlessApply :ethereum:core:spotlessApply -q
./gradlew :app:compileJava :ethereum:core:test --tests "*BonsaiTrieLogToForestConverterTest*" 2>&1 | tail -40
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 4: Commit**

```bash
git add app/src/main/java/org/hyperledger/besu/cli/subcommands/storage/ConvertToForestSubCommand.java
git commit -m "feat: expose --Xx-convert-warm-account-paths CLI flag"
```

---

## After implementation

The fix is judged by the `applyAccountHits`/`applyAccountMisses` counters already in the
`convert.log` progress line (`applyAcct hits/misses X/Y`) — no new instrumentation needed.
Deploy to the node and watch apply-account hit% in a high-churn region: if it climbs toward
the ~99% seen in easier regions (instead of collapsing to 15-85% as observed), the fix is
working. If it regresses throughput instead, set `--Xx-convert-warm-account-paths=false` and
restart — this disables just this behavior, leaving the memory config, prefetch, and
storage-trie parallelization untouched.
