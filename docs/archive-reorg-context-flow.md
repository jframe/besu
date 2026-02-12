# Archive Reorg Context Flow: The Complete Picture

## The Critical Issue

**When accounts are rolled back to non-existence, they're REMOVED from the accumulator (line 716), not marked as deleted. Therefore `persist()` never sees them and can't write deletion markers.**

## Code Path

```
PathBasedWorldStateUpdateAccumulator.rollAccountChange():
  if (replacementValue == null && accountValue.getPrior() == null) {
    accountsToUpdate.remove(address);  // ← Account disappears!
    return;
  }
```

Result: Rolled-back accounts vanish from tracking → no deletion markers written.

## Option 1: Persist After Each Rollback - Context Flow

### Rollback 4→3→2→1 with Intermediate Persists

```
┌─────────────────────────────────────────────────────────────────┐
│ INITIAL STATE: At Block 4                                       │
│ readContext = 4                                                  │
│ Archive DB: ACCOUNT_B+suffix(4) = 2 ETH (from Block 4A)        │
│             ACCOUNT_A+suffix(3) = 1 ETH (from Block 3A)         │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 1: Rollback Block 4                                        │
│                                                                  │
│ rollBack(TrieLog(block4)):                                      │
│   - ACCOUNT_B created in block 4 (prior=null)                  │
│   - rollAccountChange(ACCOUNT_B, value, null)                  │
│   - Line 716: accountsToUpdate.remove(ACCOUNT_B) ← REMOVED!    │
│                                                                  │
│ ❌ Problem: ACCOUNT_B no longer in accumulator                  │
│                                                                  │
│ persist(block3Header):                                          │
│   - prePersist(): writeContext = 3                             │
│   - updateTheAccounts(): iterates accountsToUpdate             │
│   - ACCOUNT_B not found → no deletion marker written! ❌       │
│   - postPersistSuccess(): readContext = 3                      │
│                                                                  │
│ Archive DB: ACCOUNT_B+suffix(4) = 2 ETH (STILL THERE!)        │
│             ACCOUNT_A+suffix(3) = 1 ETH                         │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 2: Rollback Block 3                                        │
│                                                                  │
│ rollBack(TrieLog(block3)):                                      │
│   - ACCOUNT_A created in block 3 (prior=null)                  │
│   - Line 716: accountsToUpdate.remove(ACCOUNT_A) ← REMOVED!    │
│                                                                  │
│ persist(block2Header):                                          │
│   - prePersist(): writeContext = 2                             │
│   - ACCOUNT_A not in accumulator → no deletion marker! ❌      │
│   - postPersistSuccess(): readContext = 2                      │
│                                                                  │
│ Archive DB: ACCOUNT_B+suffix(4) = 2 ETH                        │
│             ACCOUNT_A+suffix(3) = 1 ETH (STILL THERE!)         │
└─────────────────────────────────────────────────────────────────┘
```

## Why Option 1 Alone Doesn't Work

Even with persist after each rollback:
- ✅ Correct writeContext is used (3, 2, 1)
- ❌ But accountsToUpdate is empty for rolled-back accounts
- ❌ persist() has nothing to write
- ❌ No deletion markers created

## The Complete Fix: Track Deletions + Persist Each Step

### Change 1: Track Rolled-Back Deletions

**File:** `PathBasedWorldStateUpdateAccumulator.java`

```java
private Set<Address> accountsDeletedDuringRollback = new HashSet<>();
private Map<Address, Set<StorageSlotKey>> storageDeletedDuringRollback = new HashMap<>();

private void rollAccountChange(Address address, AccountValue expectedValue, AccountValue replacementValue) {
  // ... existing code ...
  
  if (replacementValue == null) {
    if (accountValue.getPrior() == null) {
      accountsToUpdate.remove(address);
      accountsDeletedDuringRollback.add(address);  // ← TRACK IT!
      return;
    }
    // ... rest of method
  }
}

public Set<Address> getAndClearDeletedAccounts() {
  Set<Address> deleted = new HashSet<>(accountsDeletedDuringRollback);
  accountsDeletedDuringRollback.clear();
  return deleted;
}
```

### Change 2: Write Deletion Markers in persist()

**File:** `BonsaiWorldState.java` (or override in BonsaiArchiveWorldState)

```java
private void updateTheAccounts(...) {
  // Existing code for accountsToUpdate
  for (final Map.Entry<Address, PathBasedValue<BonsaiAccount>> accountUpdate : ...) {
    // ... existing logic ...
  }
  
  // NEW: Write deletion markers for rolled-back accounts (archive mode only)
  if (worldStateUpdater instanceof BonsaiWorldStateUpdateAccumulator) {
    BonsaiWorldStateUpdateAccumulator bonsaiUpdater = 
        (BonsaiWorldStateUpdateAccumulator) worldStateUpdater;
    for (Address deletedAddress : bonsaiUpdater.getAndClearDeletedAccounts()) {
      maybeStateUpdater.ifPresent(
          stateUpdater -> stateUpdater.removeAccountInfoState(deletedAddress.addressHash()));
    }
  }
}
```

### Change 3: Persist After Each Rollback

**File:** `PathBasedWorldStateProvider.java`

```java
BlockHeader currentHeader = blockchain.getBlockHeader(mutableState.blockHash()).get();

for (final TrieLog rollBack : rollBacks) {
  pathBasedUpdater.rollBack(rollBack);
  
  if (isArchiveMode(mutableState)) {
    pathBasedUpdater.commit();
    BlockHeader parentHeader = blockchain.getBlockHeader(currentHeader.getParentHash()).get();
    mutableState.persist(parentHeader);  // Writes deletion markers with writeContext=parent
    pathBasedUpdater = (PathBasedWorldStateUpdateAccumulator<?>) mutableState.updater();
    currentHeader = parentHeader;
  }
}
```

## Complete Flow With Fix

```
┌─────────────────────────────────────────────────────────────────┐
│ Rollback Block 4 with Deletion Tracking                         │
│                                                                  │
│ rollBack(TrieLog(4)):                                           │
│   - accountsToUpdate.remove(ACCOUNT_B)                          │
│   - accountsDeletedDuringRollback.add(ACCOUNT_B) ← TRACKED!    │
│                                                                  │
│ persist(block3Header):                                          │
│   - prePersist(): writeContext = 3                             │
│   - updateTheAccounts():                                        │
│       * Regular accounts in accountsToUpdate                    │
│       * getAndClearDeletedAccounts() → {ACCOUNT_B}             │
│       * removeAccountInfoState(ACCOUNT_B)                       │
│       * Writes: ACCOUNT_B+suffix(3) = DELETED ✓                │
│   - postPersistSuccess(): readContext = 3                      │
│                                                                  │
│ Archive DB: ACCOUNT_B+suffix(4) = 2 ETH (orphaned)            │
│             ACCOUNT_B+suffix(3) = DELETED ← OVERWRITES!        │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Later: Block 4B Reads ACCOUNT_B                                 │
│                                                                  │
│ readContext = 3                                                  │
│ getNearestBefore(ACCOUNT_B+suffix(3))                           │
│   → Finds: ACCOUNT_B+suffix(3) = DELETED                       │
│   → Returns: Optional.empty() ✓                                │
│   → Transfer creates fresh account with correct balance ✓      │
└─────────────────────────────────────────────────────────────────┘
```

## Summary

**Q: Why doesn't rollback write deletion markers?**

**A: Because rolled-back accounts are removed from the accumulator (not marked as deleted), so persist() never sees them.**

The fix requires **two changes:**
1. **Track** accounts deleted during rollback (instead of just removing them)
2. **Persist** after each rollback step with the correct writeContext
3. **Write** deletion markers for tracked deletions during each persist

The correct context (writeContext = parent block number) is automatically set by `prePersist()` when `persist(parentHeader)` is called, but only if we actually have the deleted accounts to write!
