# Account Deletion During Reorg - Root Cause Analysis

## The Problem

After reorging from chain A to chain B, accounts that existed ONLY on chain A are still visible when querying chain B's head state.

**Example Test Scenario:**
- Chain A: genesis → 1 → 2A (creates ACCOUNT_B) → 3A → 4A
- Reorg to Chain B: genesis → 1 → 2B → 3B → 4B (never creates ACCOUNT_B)
- Query chain B at block 4B for ACCOUNT_B
- **Expected**: null (account doesn't exist on chain B)
- **Actual**: AccountState with balance from chain A

## Root Cause

The flat DB stores accounts with keys like:
```
accountHash + blockNumberSuffix → accountValue
```

### During Chain A Execution:
- Block 2A creates ACCOUNT_B
- Flat DB write: `ACCOUNT_B + suffix 2 → {balance: 3 ETH}`
  (Write context uses WORLD_BLOCK_NUMBER_KEY + 1 as suffix)

### During Reorg to Chain B:
1. **Rollback** chain A blocks (4A → 3A → 2A → genesis)
   - In-memory state: ACCOUNT_B is removed from `accountsToUpdate`
   - Flat DB: **NO deletion marker written** (rollback just updates memory)

2. **Set context** to common ancestor (genesis, block 0)
   - WORLD_BLOCK_NUMBER_KEY = 0

3. **Rollforward** to block 2B
   - In-memory state: ACCOUNT_C is created
   - ACCOUNT_B is not in `accountsToUpdate` (not touched on chain B)

4. **Set context** for commit (block 1 for writes)
   - Write context = 1 + 1 = 2

5. **Commit** block 2B
   - Writes ACCOUNT_C with suffix 2
   - **Does NOT write deletion marker for ACCOUNT_B**
   - Flat DB still has: `ACCOUNT_B + suffix 2 → {balance: 3 ETH}` from chain A

### When Querying Chain B:
- At block 4B, WORLD_BLOCK_NUMBER_KEY = 4
- Query for ACCOUNT_B with max suffix 4
- `getNearestBefore(ACCOUNT_B + suffix 4)` finds `ACCOUNT_B + suffix 2` from chain A
- **Returns orphaned chain A data!**

## Why Deletion Markers Aren't Written

The commit process (BonsaiWorldState.updateTheAccounts lines 173-198) only writes deletion markers for accounts in the `accountsToUpdate` map:

```java
for (final Map.Entry<Address, PathBasedValue<BonsaiAccount>> accountUpdate :
     worldStateUpdater.getAccountsToUpdate().entrySet()) {
    if (updatedAccount == null) {
        // This account is being deleted
        bonsaiUpdater.removeAccountInfoState(addressHash); // Writes DELETED marker
    } else {
        // This account is being created/updated
        bonsaiUpdater.putAccountInfoState(addressHash, accountValue);
    }
}
```

**The problem**: Accounts deleted during rollback are removed from `accountsToUpdate`, not set to null!

Looking at PathBasedWorldStateUpdateAccumulator.rollAccountChange (line 712-719):
```java
if (replacementValue == null) {
    if (accountValue.getPrior() == null) {
        accountsToUpdate.remove(address);  // ← REMOVES from map
    } else {
        accountValue.setUpdated(null);     // ← Sets to null (this triggers deletion marker)
    }
}
```

If `accountValue.getPrior() == null` (account was created in a block being rolled back), the account is **removed** from the map entirely, not set to null. This means:
- It won't be processed during commit
- No deletion marker will be written
- The old flat DB entry persists

## The Solution

We need to ensure that accounts removed during rollback get deletion markers written at the commit block number. Two approaches:

### Option 1: Keep Track of Deleted Accounts
During rollback, track accounts that are removed from `accountsToUpdate` and ensure they get deletion markers written during commit.

### Option 2: Write Deletion Markers During Rollback
When rolling back an account creation, immediately write a deletion marker to the flat DB at the rollback target block number.

### Option 3: Filter by Block Hash (Not Just Block Number)
This would be a larger architectural change - include the block hash in the flat DB keys so chain A and chain B entries don't conflict.

## Recommended Fix: Option 1

Modify `PathBasedWorldStateUpdateAccumulator` to track deleted accounts separately and ensure deletion markers are written during commit, even for accounts removed from the main update map.
