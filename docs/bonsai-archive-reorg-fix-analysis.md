# Analysis: Fixing Bonsai Archive Reorg Tests

## Current Failures

3 tests fail due to orphaned block data remaining in archive flat DB after reorgs:
- `shouldHandleMultiBlockReorgWithCombinedAccountAndStorageConflicts`
- `shouldHandleDeepMultiBlockReorgWithConflictsAtEveryLevel`
- `shouldHandleReorgWithAlternatingAccountCreationDeletion`

## Problem Statement

**Example Scenario:**
```
Chain A: Block 3A creates ACCOUNT_A with 1 ETH (writes ACCOUNT_A+suffix(3) = 1 ETH)
Reorg:   Roll back to block 1 (TrieLog updates in-memory, archive DB unchanged)
Chain B: Block 3B sends to ACCOUNT_C (doesn't touch ACCOUNT_A)
         Block 4B sends 5 ETH to ACCOUNT_A
         - Reads with readContext=3: finds ACCOUNT_A+suffix(3) = 1 ETH (from Chain A!)
         - Adds 5 ETH → balance = 6 ETH
         - Writes ACCOUNT_A+suffix(4) = 6 ETH

Expected: ACCOUNT_A = 5 ETH
Actual:   ACCOUNT_A = 6 ETH (includes orphaned 1 ETH from Chain A)
```

## Required Changes

### Option 1: Persist After Each Rollback (Recommended)

**File:** `PathBasedWorldStateProvider.java`
**Method:** `rollFullWorldStateToBlockHash()`
**Location:** Lines 297-300

**Change:**
```java
// Current code:
for (final TrieLog rollBack : rollBacks) {
  LOG.debug("Attempting Rollback of {}", rollBack.getBlockHash());
  pathBasedUpdater.rollBack(rollBack);
}

// Proposed fix:
BlockHeader currentHeader = blockchain.getBlockHeader(mutableState.blockHash()).get();
for (final TrieLog rollBack : rollBacks) {
  LOG.debug("Attempting Rollback of {}", rollBack.getBlockHash());
  pathBasedUpdater.rollBack(rollBack);

  // For archive mode, persist after each rollback to overwrite orphaned data
  if (isArchiveMode(mutableState)) {
    pathBasedUpdater.commit();
    BlockHeader parentHeader = blockchain.getBlockHeader(currentHeader.getParentHash()).get();
    mutableState.persist(parentHeader); // Writes state at parent block with parent's block number
    pathBasedUpdater = (PathBasedWorldStateUpdateAccumulator<?>) mutableState.updater();
    currentHeader = parentHeader;
  }
}
```

**Helper Method:**
```java
private boolean isArchiveMode(PathBasedWorldState worldState) {
  return worldState.getWorldStateStorage() instanceof BonsaiWorldStateKeyValueStorage &&
         ((BonsaiWorldStateKeyValueStorage) worldState.getWorldStateStorage())
             .getFlatDbMode() == FlatDbMode.ARCHIVE;
}
```

**Impact:**
- Overwrites orphaned data at each block number during rollback
- When block 4→3→2→1 rollback happens, persists at blocks 3, 2, 1
- Each persist writes current state with that block number suffix, overwriting orphaned entries
- Performance: Additional persist calls during reorg (acceptable for rare reorgs)

### Option 2: Write Deletion Markers During Rollback

**File:** `PathBasedWorldStateUpdateAccumulator.java`
**Method:** Add new method `rollBackWithDeletionMarkers()`

**Approach:**
```java
public void rollBackWithDeletionMarkers(final TrieLog layer, long blockNumber) {
  layer.getAccountChanges().forEach((address, change) -> {
    rollAccountChange(address, change.getUpdated(), change.getPrior());

    // If account was created in this block (prior == null), mark as deleted
    if (change.getPrior() == null && change.getUpdated() != null) {
      // This account didn't exist before this block, so delete it at this block number
      deleteAccount(address); // Will write deletion marker with current writeContext
    }
  });
  // Similar for storage changes
}
```

**Integration:**
- Call this method instead of `rollBack()` when in archive mode
- Requires passing block number to know which block is being rolled back
- Writes `DELETED_ACCOUNT_VALUE` markers for accounts created in rolled-back blocks

**Impact:**
- More surgical approach - only writes deletion markers for affected accounts
- Requires changes to rollback interface to pass block number
- Complex to handle storage slots correctly

### Option 3: Persist Complete State at Target Block

**File:** `PathBasedWorldStateProvider.java`
**Method:** `rollFullWorldStateToBlockHash()`
**Location:** After line 308

**Change:**
```java
// After: mutableState.persist(blockchain.getBlockHeader(blockHash).get());

// For archive mode, also persist at intermediate block numbers to clear orphaned data
if (isArchiveMode(mutableState) && !rollBacks.isEmpty()) {
  BlockHeader targetHeader = blockchain.getBlockHeader(blockHash).get();
  long highestRolledBackBlock = /* get from rollBacks list */;

  // Persist current state at each intermediate block number
  for (long blockNum = targetHeader.getNumber() + 1; blockNum <= highestRolledBackBlock; blockNum++) {
    // Create temporary context for this block number
    persistStateAtBlockNumber(mutableState, blockNum);
  }
}
```

**Impact:**
- Writes the rolled-back state at multiple block numbers
- Overwrites all orphaned data comprehensively
- Performance: Multiple persist operations, but simple logic
- Challenge: Need to handle persist with non-existent block headers

## Recommended Implementation

**Option 1** is recommended because:
1. ✅ Clean integration point in existing rollback loop
2. ✅ Persists actual historical state at each block (not just deletions)
3. ✅ Minimal code changes
4. ✅ Preserves correct historical state for each block number
5. ✅ Easy to test and verify

## Testing Strategy

After implementing Option 1:
1. Re-enable the 3 disabled tests
2. Verify they pass
3. Run full archive test suite
4. Performance test: measure reorg overhead (should be acceptable for rare reorgs)

## Code Locations

**Files to modify:**
- `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/common/provider/PathBasedWorldStateProvider.java` (primary)
- `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiWorldStateProvider.java` (helper method if needed)

**Files to update after fix:**
- `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiArchiveReorgTest.java` (re-enable tests)
- `docs/bonsai-archive-reorg-limitation.md` (update or remove)

## Estimated Complexity

- **Code changes:** ~30 lines
- **Testing:** ~15 minutes
- **Risk:** Low (only affects archive mode reorg path)
- **Performance impact:** Minimal (reorgs are rare)
