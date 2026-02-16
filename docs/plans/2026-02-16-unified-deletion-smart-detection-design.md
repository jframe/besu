# Unified Deletion with Smart Detection - Design Document

**Date:** 2026-02-16
**Status:** Approved
**Author:** Claude (via Brainstorming Session)

## Problem Statement

Bonsai archive mode currently has two deletion methods with different semantics:

1. **`removeFlatAccount()`** - Writes deletion markers (empty byte arrays)
2. **`deleteFlatAccountAtBlock()`** - Actually removes entries from storage

This dual approach adds complexity and requires callers to choose the correct method. However, unifying them is non-trivial because:

- **Deletion markers** act as hard barriers - they stop queries and return null
- **Actual deletion** reveals historical data from earlier blocks

### The Core Conflict

**SELFDESTRUCT requires markers:**
```
Block 10: Account exists (5 ETH)
Block 20: Account SELFDESTRUCTED
Query at block 25: Should return null (not 5 ETH from block 10)
→ Need marker at block 20 to hide historical data
```

**Reorg cleanup requires deletion:**
```
Block 10: Account exists (5 ETH) - legitimate historical data
Block 20 (Chain A): Account modified (10 ETH) - ORPHANED after reorg
Query at block 25 (Chain B): Should return 5 ETH (not null)
→ Need to DELETE orphaned entry at block 20 to reveal block 10's data
```

If we use markers for reorg cleanup, they incorrectly mask legitimate historical data.

## Solution: Smart Detection

Automatically detect the scenario based on database state and choose the correct operation.

### Key Insight

The two scenarios have different database states when `removeFlatAccount()` is called:

| Scenario | Data at Current Block? | Reason |
|----------|----------------------|--------|
| SELFDESTRUCT | ❌ Absent | Data not written yet (being deleted during commit) |
| Reorg cleanup | ✅ Present | Orphaned data still exists (rollback hasn't cleaned it) |

**This difference enables automatic detection.**

## Design

### Smart Detection Algorithm

```java
@Override
public void removeFlatAccount(
    final SegmentedKeyValueStorage storage,
    final SegmentedKeyValueStorageTransaction transaction,
    final Hash accountHash,
    final Supplier<Optional<BonsaiContext>> writeContextSupplier) {

  BonsaiContext writeContext = writeContextSupplier.get().orElse(new BonsaiContext(0L));
  byte[] keySuffixed = calculateArchiveKeyWithMinSuffix(
      Optional.of(writeContext), accountHash.getBytes().toArrayUnsafe());

  // SMART DETECTION: Check if data exists at this exact block
  Optional<byte[]> currentBlockData = storage.get(ACCOUNT_INFO_STATE, keySuffixed);

  if (currentBlockData.isPresent()
      && !Arrays.areEqual(DELETED_ACCOUNT_VALUE, currentBlockData.get())) {

    // CASE: Data exists at current block = ORPHANED DATA from reorg
    // This is reorg cleanup - check for historical data

    if (hasHistoricalAccountDataBefore(storage, accountHash, writeContext.getBlockNumber())) {
      // Historical data exists - DELETE orphaned entry to reveal it
      transaction.remove(ACCOUNT_INFO_STATE, keySuffixed);
    } else {
      // No historical data - OVERWRITE orphaned data with marker
      transaction.put(ACCOUNT_INFO_STATE, keySuffixed, DELETED_ACCOUNT_VALUE);
    }

  } else {
    // CASE: No data at current block = SELFDESTRUCT scenario
    // Account is being destroyed during block commit (data not written yet)
    // Always write marker to hide historical data
    transaction.put(ACCOUNT_INFO_STATE, keySuffixed, DELETED_ACCOUNT_VALUE);
  }
}
```

### Historical Data Check

Reuses existing `getFlatAccount()` logic to avoid duplication:

```java
private boolean hasHistoricalAccountDataBefore(
    final SegmentedKeyValueStorage storage,
    final Hash accountHash,
    final long blockNumber) {

  if (blockNumber == 0) {
    return false; // No blocks before genesis
  }

  // Reuse getFlatAccount with readContext = blockNumber - 1
  Supplier<Optional<BonsaiContext>> readContext =
      () -> Optional.of(new BonsaiContext(blockNumber - 1));

  Optional<Bytes> historicalData = getFlatAccount(
      () -> Optional.empty(), // worldStateRootHash not needed
      null,                    // nodeLoader not needed
      accountHash,
      storage,
      readContext);

  return historicalData.isPresent();
}
```

**Same pattern for storage slots** using `getFlatAccountStorageValueByStorageSlotKey()`.

## Scenario Walkthroughs

### Scenario 1: SELFDESTRUCT

```
Block 10: Account created (balance: 5 ETH)
Block 15: Account modified (balance: 10 ETH)
Block 20: Account SELFDESTRUCTED
```

**At block 20 commit:**
1. `storage.get(account+20)` → Empty (no data written yet)
2. Detection: **NO current data = SELFDESTRUCT**
3. Action: `transaction.put(account+20, MARKER)`

**Query at block 25:**
- Finds marker at block 20 → Returns null ✓

---

### Scenario 2: Reorg Cleanup WITH Historical Data

```
Block 10: Account exists (balance: 5 ETH) [legitimate]
Block 20 (Chain A): Account modified (balance: 10 ETH) [ORPHANED]
Reorg to Chain B: Never touches this account
```

**During `writeRollbackDeletionMarkers()`:**
1. `storage.get(account+20)` → Returns orphaned data (10 ETH)
2. Detection: **Current data exists = Reorg cleanup**
3. `hasHistoricalDataBefore(account, 20)` → TRUE
4. Action: `transaction.remove(account+20)` - DELETE orphaned entry

**Query at block 25:**
- Finds legitimate data at block 10 → Returns 5 ETH ✓

---

### Scenario 3: Reorg Cleanup WITHOUT Historical Data

```
Block 20 (Chain A): Account created (balance: 10 ETH) [ORPHANED]
Reorg to Chain B: Never touches this account
```

**During `writeRollbackDeletionMarkers()`:**
1. `storage.get(account+20)` → Returns orphaned data (10 ETH)
2. Detection: **Current data exists = Reorg cleanup**
3. `hasHistoricalDataBefore(account, 20)` → FALSE
4. Action: `transaction.put(account+20, MARKER)` - Overwrite with marker

**Query at block 25:**
- Finds marker at block 20 → Returns null ✓

## Methods Removed

After unification, these methods become obsolete:

**BonsaiArchiveFlatDbStrategy.java:**
- ❌ `deleteFlatAccountAtBlock()`
- ❌ `deleteFlatStorageAtBlock()`

**BonsaiWorldStateKeyValueStorage.java (Updater):**
- ❌ `deleteAccountInfoStateAtBlock()`
- ❌ `deleteStorageValueBySlotHashAtBlock()`

**BonsaiWorldState.java:**
- Update `writeRollbackDeletionMarkers()` to call `removeAccountInfoStateAtBlock()` (already does)

## Edge Cases

**1. Account recreation after deletion:**
- Block 15: SELFDESTRUCT (marker)
- Block 20: Account re-created (new data)
- Query at 18: null ✓, Query at 22: account exists ✓

**2. Multiple reorgs at same block:**
- Smart detection handles each reorg based on current database state

**3. Genesis block:**
- `hasHistoricalDataBefore(0)` returns false (guard clause)

**4. Concurrent processing:**
- Reads committed state (`storage.get()`) - deterministic and race-free

## Performance Impact

**SELFDESTRUCT (common case):**
- Added overhead: 1 `storage.get()` read (~microseconds)
- Minimal impact on normal operation

**Reorg cleanup (rare):**
- Added overhead: 1 `storage.get()` + 1 `getFlatAccount()` call
- Only during reorgs (infrequent events)

**Overall:** Negligible performance impact for significantly simpler code.

## Testing Strategy

**Unit Tests (BonsaiArchiveFlatDbStrategyTest):**
1. SELFDESTRUCT with/without historical data → always writes marker
2. Reorg cleanup with historical data → deletes to reveal
3. Reorg cleanup without historical data → writes marker
4. `hasHistoricalDataBefore()` correctness
5. Idempotency of multiple deletions

**Integration Tests (BonsaiArchiveReorgTest):**
1. `shouldMaintainCorrectStateForMultipleBlocksAfterReorg()` - the failing test
2. All existing reorg tests
3. Account recreation scenarios
4. Storage slot equivalents

## Migration Path

1. Implement smart detection in removal methods
2. Run full test suite (especially `BonsaiArchiveReorgTest`)
3. Remove deprecated `delete*()` methods
4. Run mainnet/testnet sync verification

**Rollback:** Old methods can be temporarily restored if issues arise.

## Benefits

1. **Unified API** - Single deletion method handles all scenarios
2. **Automatic** - No caller decision required
3. **Correct** - Handles both SELFDESTRUCT and reorg cleanup properly
4. **Simple** - ~60 lines of smart detection vs dual code paths
5. **Minimal overhead** - One extra read per deletion
6. **No API changes** - Existing callers work unchanged

## Conclusion

Smart detection provides a clean unification by exploiting the natural difference in database state between SELFDESTRUCT (no current data) and reorg cleanup (orphaned data present). The solution is automatic, correct, and maintains the existing API.
