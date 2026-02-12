# Archive Mode Reorg Issue: Missing Deletion Markers at Intermediate Blocks

## Executive Summary

Bonsai archive mode writes deletion markers during rollback, but **only at the final target block**, not at intermediate block numbers. This leaves orphaned data from the old fork that gets incorrectly read when building the new fork.

## Detailed Explanation

### How Archive Keys Work

Archive mode stores data with block numbers as key suffixes:
```
Key format: naturalKey + blockNumberSuffix
Example: ACCOUNT_A + suffix(3) = account data at block 3
```

When reading with `readContext=N`, it uses `getNearestBefore(key + suffix(N))` to find the most recent entry ≤ block N.

### The Reorg Scenario

**Initial Chain A:**
```
Block 1: Genesis (no accounts)
Block 2A: Deploy contract
Block 3A: Create ACCOUNT_A with 1 ETH
  → Writes: ACCOUNT_A+suffix(3) = 1 ETH
Block 4A: Create ACCOUNT_B with 2 ETH
  → Writes: ACCOUNT_B+suffix(4) = 2 ETH

After block 4A: headWorldState.readContext = 4
```

**Reorg to Block 1:**
```
1. PathBasedWorldStateProvider.rollFullWorldStateToBlockHash(headWorldState, block1Hash)
2. Build rollBacks list: [TrieLog(block4A), TrieLog(block3A), TrieLog(block2A)]
3. Apply rollbacks to accumulator:
   - rollBack(TrieLog(block4A)): ACCOUNT_B deleted from accumulator
   - rollBack(TrieLog(block3A)): ACCOUNT_A deleted from accumulator
   - rollBack(TrieLog(block2A)): Contract deleted from accumulator
4. pathBasedUpdater.commit() - commits in-memory changes
5. mutableState.persist(block1Header):
   - prePersist(): sets writeContext = 1
   - Writes accounts in accumulator to flat DB with suffix=1
   - ACCOUNT_A (null in accumulator): removeAccountInfoState() → ACCOUNT_A+suffix(1) = DELETED
   - ACCOUNT_B (null in accumulator): removeAccountInfoState() → ACCOUNT_B+suffix(1) = DELETED
6. postPersistSuccess(): sets readContext = 1
7. resetWorldStateTo(block1Header): sets readContext = 1 (redundant but explicit)
```

**Archive Database After Rollback:**
```
✅ ACCOUNT_A+suffix(1) = DELETED   (written during persist(block1))
❌ ACCOUNT_A+suffix(3) = 1 ETH     (orphaned from Chain A, never overwritten!)
✅ ACCOUNT_B+suffix(1) = DELETED   (written during persist(block1))
❌ ACCOUNT_B+suffix(4) = 2 ETH     (orphaned from Chain A, never overwritten!)
```

**Building New Chain B:**
```
Block 2B: Deploy contract (readContext=1, writeContext=2)
  → No entries for ACCOUNT_A or ACCOUNT_B written

Block 3B: Transfer 3 ETH to ACCOUNT_C (readContext=2, writeContext=3)
  → Creates: ACCOUNT_C+suffix(3) = 3 ETH
  → ACCOUNT_A not touched, so ACCOUNT_A+suffix(3) from Chain A remains!

Block 4B: Transfer 5 ETH to ACCOUNT_A (readContext=3, writeContext=4)
  → Read ACCOUNT_A with readContext=3:
     getNearestBefore(ACCOUNT_A+suffix(3))
     → Finds: ACCOUNT_A+suffix(3) = 1 ETH ⚠️ (from Chain A!)
  → Current balance: 1 ETH (wrong!)
  → Add transfer: 1 + 5 = 6 ETH
  → Write: ACCOUNT_A+suffix(4) = 6 ETH

Expected: ACCOUNT_A = 5 ETH (starting from 0)
Actual:   ACCOUNT_A = 6 ETH (includes 1 ETH from orphaned Chain A)
```

## Why Deletion Markers Are Only Written at Block 1

The current `rollFullWorldStateToBlockHash()` logic:
1. Collects all TrieLogs to roll back
2. Applies all rollbacks to the accumulator (in-memory)
3. **Calls persist() exactly once** at the target block
4. This single persist() writes deletion markers with `writeContext = targetBlockNumber`

**The gap:** No deletion markers are written for intermediate blocks 2, 3, 4.

## Why This Happens

The persist-once design works fine for **FULL** and **PARTIAL** modes because they:
- Overwrite data in-place (no block suffixes)
- Don't keep historical versions
- Single persist is sufficient

But **ARCHIVE** mode:
- Uses block number suffixes
- Keeps all historical versions
- Needs deletion markers at EVERY block number to prevent stale reads

## The Fix

### What Needs to Change

**Persist after each rollback step**, not just once at the end:

```java
// Current (PathBasedWorldStateProvider.java, lines 297-308):
for (final TrieLog rollBack : rollBacks) {
  pathBasedUpdater.rollBack(rollBack);
}
pathBasedUpdater.commit();
mutableState.persist(blockchain.getBlockHeader(blockHash).get());

// Fixed (for archive mode):
BlockHeader currentHeader = blockchain.getBlockHeader(mutableState.blockHash()).get();

for (final TrieLog rollBack : rollBacks) {
  pathBasedUpdater.rollBack(rollBack);

  // Archive mode: persist after EACH rollback
  if (isArchiveMode(mutableState)) {
    pathBasedUpdater.commit();
    BlockHeader parentHeader = blockchain.getBlockHeader(currentHeader.getParentHash()).get();
    mutableState.persist(parentHeader); // Writes deletion markers at parent block number
    pathBasedUpdater = (PathBasedWorldStateUpdateAccumulator<?>) mutableState.updater();
    currentHeader = parentHeader;
  }
}

// Final persist for all modes
if (!isArchiveMode(mutableState)) {
  pathBasedUpdater.commit();
}
mutableState.persist(blockchain.getBlockHeader(blockHash).get());
```

### What This Achieves

**Rolling back 4→3→2→1 with intermediate persists:**
```
1. rollBack(block4)
   → persist(block3Header) with writeContext=3
   → Writes: ACCOUNT_B+suffix(3) = DELETED (overwrites any orphaned data at block 3)

2. rollBack(block3)
   → persist(block2Header) with writeContext=2
   → Writes: ACCOUNT_A+suffix(2) = DELETED

3. rollBack(block2)
   → persist(block1Header) with writeContext=1
   → Writes: ACCOUNT_A+suffix(1) = DELETED, contract+suffix(1) = DELETED
```

**Archive Database After Fixed Rollback:**
```
✅ ACCOUNT_A+suffix(1) = DELETED
✅ ACCOUNT_A+suffix(2) = DELETED
✅ ACCOUNT_A+suffix(3) = DELETED  ← This is the key fix!
✅ ACCOUNT_B+suffix(1) = DELETED
✅ ACCOUNT_B+suffix(3) = DELETED
✅ ACCOUNT_B+suffix(4) = DELETED
```

Now when Block 4B reads ACCOUNT_A with `readContext=3`:
```
getNearestBefore(ACCOUNT_A+suffix(3))
→ Finds: ACCOUNT_A+suffix(3) = DELETED
→ Returns: Optional.empty() (account doesn't exist)
→ Transfer creates new account: ACCOUNT_A = 5 ETH ✓
```

## Why Deletion Markers Exist But Don't Help

**Deletion markers ARE written**, but in the wrong place:
- ✅ Written at block 1 (the target)
- ❌ NOT written at blocks 2, 3, 4 (the intermediate blocks)

When reading at block 3 or 4, the `getNearestBefore` search doesn't look back far enough to find the deletion marker at block 1. It stops at the orphaned data at block 3.

## Summary

| Aspect | Current Behavior | Required Behavior |
|--------|-----------------|-------------------|
| **Rollback approach** | Apply all rollbacks, then persist once | Apply rollback, persist, repeat |
| **Deletion markers** | Written only at target block (e.g., block 1) | Written at each intermediate block (1, 2, 3, 4) |
| **Archive DB cleanup** | Orphaned data remains at blocks 2+ | Orphaned data overwritten with deletion markers |
| **Impact** | Reads find orphaned data | Reads find deletion markers |
| **Performance** | 1 persist per reorg | N persists per reorg (N = depth) |

## Files Requiring Changes

1. **PathBasedWorldStateProvider.java** - Modify rollback loop to persist after each step when in archive mode
2. **BonsaiArchiveReorgTest.java** - Re-enable the 3 disabled tests
3. **docs/bonsai-archive-reorg-limitation.md** - Update to reflect fix or remove

## Complexity Assessment

- **Code changes:** ~40 lines in PathBasedWorldStateProvider
- **Risk:** Low (only affects archive mode reorg path)
- **Performance:** Acceptable (reorgs are rare; deep reorgs even rarer)
- **Testing:** 3 existing tests will validate the fix
