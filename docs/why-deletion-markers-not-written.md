# Why Deletion Markers Aren't Written During Rollback

## The Answer: Rolled-Back Accounts Are Removed from the Accumulator

When an account created in a rolled-back block is removed, it's **completely deleted** from the accumulator, not marked as "deleted". Therefore, `persist()` never sees it and can't write a deletion marker.

## Code Flow Analysis

### Step 1: Account Created in Block 4A

```java
Block 4A creates ACCOUNT_B with balance = 2 ETH
→ accountsToUpdate.put(ACCOUNT_B, PathBasedValue(prior=null, updated=account))
→ persist(block4): writes ACCOUNT_B+suffix(4) = 2 ETH
```

### Step 2: Rollback Block 4A

**TrieLog for Block 4A:**
```java
accountChanges.put(ACCOUNT_B, Change(prior=null, updated=accountValue))
```

**rollBack() calls rollAccountChange():**
```java
rollAccountChange(
  address = ACCOUNT_B,
  expectedValue = accountValue,  // What existed in block 4
  replacementValue = null         // What existed before block 4 (nothing)
)
```

**Critical Code (PathBasedWorldStateUpdateAccumulator.java:712-716):**
```java
if (replacementValue == null) {
  if (accountValue.getPrior() == null) {
    // Account was created in this block, didn't exist before
    // Remove it entirely from the accumulator!
    accountsToUpdate.remove(address);  // ← THIS IS THE ISSUE!
    return;
  } else {
    // Account existed before, restore prior value
    accountsToUpdate.put(
        address,
        new PathBasedValue<>(accountValue.getPrior(), null));
  }
}
```

### Step 3: Persist After Rollback

```java
mutableState.persist(block3Header)
→ prePersist(): writeContext = 3
→ updateTheAccounts() iterates accountsToUpdate:
    - ACCOUNT_B is NOT in accountsToUpdate (was removed at line 716!)
    - No deletion marker written for ACCOUNT_B
```

**Result:**
```
Archive DB still has: ACCOUNT_B+suffix(4) = 2 ETH (orphaned!)
Archive DB should have: ACCOUNT_B+suffix(3) = DELETED (but doesn't!)
```

## Why This Design Exists

The `accountsToUpdate.remove(address)` at line 716 makes sense for **non-archive modes**:
- If an account was created in block 4 and we roll back, the account never existed
- No need to track it or write anything
- Removing it from the map is correct

But for **archive mode with block suffixes**, this causes problems:
- The orphaned entry `ACCOUNT_B+suffix(4)` remains in the DB
- We need to write `ACCOUNT_B+suffix(3) = DELETED` to prevent future reads from finding it
- But since ACCOUNT_B was removed from accountsToUpdate, persist() doesn't know to write a deletion marker

## The Context Problem in Option 1

When implementing "persist after each rollback", here's what happens:

### Scenario: Rolling Back Block 4 → Block 3

**Before Rollback:**
```
readContext = 4 (from postPersistSuccess after block 4)
writeContext = empty
Accumulator: { ACCOUNT_B: PathBasedValue(prior=null, updated=accountB) }
```

**After rollBack(TrieLog(block4)):**
```
readContext = 4 (unchanged)
writeContext = empty (unchanged)
Accumulator: { }  ← ACCOUNT_B was removed!
```

**When We Call persist(block3Header):**
```java
prePersist(block3Header):
  → writeContext = 3

persist() calls updateTheAccounts():
  → Iterates accountsToUpdate (which is empty!)
  → Nothing written for ACCOUNT_B
  → ACCOUNT_B+suffix(4) remains in DB (orphaned!)
```

**Problem:** ACCOUNT_B was removed from accumulator, so `persist()` doesn't know it existed and can't write a deletion marker.

## The Real Fix Needed

### Option A: Track Rolled-Back Account Deletions

Modify rollback to track accounts that were deleted:

```java
// In PathBasedWorldStateUpdateAccumulator
private Set<Address> accountsDeletedDuringRollback = new HashSet<>();

private void rollAccountChange(...) {
  if (replacementValue == null) {
    if (accountValue.getPrior() == null) {
      accountsToUpdate.remove(address);
      accountsDeletedDuringRollback.add(address);  // ← Track this!
      return;
    }
    ...
  }
}
```

Then during persist:
```java
// Write deletion markers for rolled-back accounts
for (Address deletedAddress : accountsDeletedDuringRollback) {
  bonsaiUpdater.removeAccountInfoState(deletedAddress.addressHash());
}
accountsDeletedDuringRollback.clear();
```

### Option B: Mark as Deleted Instead of Removing

Change line 716 to mark as deleted instead of removing:

```java
// Instead of:
accountsToUpdate.remove(address);

// Do:
accountsToUpdate.put(
    address,
    new PathBasedValue<>(null, null));  // Both prior and updated = null = deleted
```

Then in persist, check for this pattern and write deletion markers.

### Option C: Scan Archive DB for Orphaned Data

After rollback to block N, scan the archive DB for all entries with suffix > N for accounts that don't exist in the accumulator, and write deletion markers. This is expensive and complex.

## Recommended Approach

**Combination of Option A + Persist After Each Rollback:**

1. Track deletions during rollback in a separate set
2. Persist after each rollback step with correct writeContext
3. Write deletion markers for tracked deletions
4. Clear the tracking set after each persist

This ensures:
- Correct writeContext for each intermediate block
- Deletion markers written even for removed accounts
- Clean separation of concerns

## Implementation Changes Required

1. **PathBasedWorldStateUpdateAccumulator.java:**
   - Add `accountsDeletedDuringRollback` set
   - Modify `rollAccountChange()` line 716 to track deletions
   - Add method to get and clear deleted accounts

2. **BonsaiWorldState.java:**
   - In `updateTheAccounts()`, write deletion markers for rolled-back deletions
   - Or add separate method `writeRollbackDeletions()`

3. **PathBasedWorldStateProvider.java:**
   - Modify rollback loop to persist after each step when in archive mode
   - Ensure correct parent block header is used for each persist

## Context Flow With Fix

```
Initial: at block 4, readContext=4

Rollback block 4:
  → rollBack(TrieLog(4)): ACCOUNT_B deleted, tracked in accountsDeletedDuringRollback
  → persist(block3Header):
      - prePersist(): writeContext = 3  ← Correct context!
      - Write deletion markers for accountsDeletedDuringRollback
      - ACCOUNT_B+suffix(3) = DELETED  ← Overwrites position where orphaned data might exist
  → postPersistSuccess(): readContext = 3
  → Clear accountsDeletedDuringRollback

Rollback block 3:
  → rollBack(TrieLog(3)): ACCOUNT_A deleted, tracked
  → persist(block2Header):
      - prePersist(): writeContext = 2  ← Correct context!
      - ACCOUNT_A+suffix(2) = DELETED
  → postPersistSuccess(): readContext = 2

Rollback block 2:
  → rollBack(TrieLog(2)): Contract deleted, tracked
  → persist(block1Header):
      - prePersist(): writeContext = 1  ← Correct context!
      - Contract+suffix(1) = DELETED
  → postPersistSuccess(): readContext = 1
```

Each persist uses the correct writeContext (the parent block number after that rollback step), ensuring deletion markers are written at the right block numbers to overwrite orphaned data.
