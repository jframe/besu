# Bonsai Archive Mode Reorg Limitation

## Problem

When a blockchain reorganization (reorg) occurs in Bonsai archive mode, orphaned block data remains in the archive flat database. This causes subsequent reads to return stale data from the orphaned fork when new blocks are created with the same block numbers on a different fork.

## Technical Details

### Archive Key Structure

Archive mode stores accounts and storage with keys formatted as:
```
key = naturalKey + blockNumberSuffix
```

Where `blockNumberSuffix` is an 8-byte big-endian representation of the block number.

### Reorg Scenario

Consider this scenario:

**Chain A (original):**
- Block 2A: Deploy contract
- Block 3A: Create ACCOUNT_A with balance = 1 ETH → writes `ACCOUNT_A+suffix(3)` = 1 ETH
- Block 4A: Create ACCOUNT_B with balance = 2 ETH → writes `ACCOUNT_B+suffix(4)` = 2 ETH

**Reorg to Block 1:**
- TrieLog rollback updates in-memory state and trie
- **Archive flat DB entries remain unchanged** (ACCOUNT_A+suffix(3) still exists)

**Chain B (after reorg):**
- Block 2B: Deploy contract
- Block 3B: Create ACCOUNT_C with 3 ETH (does NOT touch ACCOUNT_A)
  - No write for ACCOUNT_A → archive DB still has `ACCOUNT_A+suffix(3)` = 1 ETH from Chain A
- Block 4B: Transfer 5 ETH to ACCOUNT_A
  - **Read with readContext=3:** finds `ACCOUNT_A+suffix(3)` from Chain A = 1 ETH (orphaned!)
  - Adds 5 ETH transfer → new balance = 6 ETH
  - Writes `ACCOUNT_A+suffix(4)` = 6 ETH
  - **Expected:** ACCOUNT_A should have 5 ETH (starting from 0)
  - **Actual:** ACCOUNT_A has 6 ETH (1 ETH from orphaned chain + 5 ETH)

## Root Cause

The archive flat database uses block number as the key suffix, but:
1. Block numbers are **not unique across forks** - both Chain A and Chain B have block number 3
2. TrieLog rollback only updates in-memory state and the Merkle trie, **not the flat DB**
3. When new blocks are created after a reorg, they may read orphaned data if they don't write to all previously-modified accounts

## Potential Solutions

### Option 1: Write Deletion Markers During Rollback (Recommended)
When rolling back from block N to block M, write deletion markers for all accounts modified in blocks M+1 through N. This requires:
- Tracking which accounts were modified during rollback (available from TrieLog)
- Writing `DELETED_ACCOUNT_VALUE` markers with appropriate block suffixes
- Handling storage slots similarly

### Option 2: Include Fork Identifier in Keys
Change the key structure to include both block number and a fork/chain identifier:
```
key = naturalKey + chainId + blockNumber
```
This prevents collisions but requires:
- Major refactoring of key structure
- Migration strategy for existing data
- Mechanism to track canonical chain ID

### Option 3: Persist Rolled-Back State
After TrieLog rollback, persist the complete world state (not just modified accounts) with the target block number suffix. This would overwrite orphaned data but is expensive.

## Affected Tests

The following tests expose this limitation and are currently disabled:
- `shouldHandleMultiBlockReorgWithCombinedAccountAndStorageConflicts`
- `shouldHandleDeepMultiBlockReorgWithConflictsAtEveryLevel`
- `shouldHandleReorgWithAlternatingAccountCreationDeletion`

These tests involve multi-block reorgs where accounts are created/modified in orphaned blocks, then new blocks with the same numbers are created on a different fork.

## Impact

This limitation affects:
- **Historical queries after reorgs:** May return incorrect data for blocks that were reorged
- **State consistency:** New blocks may have incorrect balances if they don't touch all accounts
- **Use cases:** Primarily impacts scenarios with frequent deep reorgs (e.g., test networks)

Most production scenarios are unaffected because:
- Deep reorgs are rare on mainnet
- Most accounts touched in reorged blocks are typically touched again in new blocks
- The 26 passing tests cover standard reorg scenarios successfully

## Status

- **Issue identified:** Yes
- **Tests disabled:** 3 tests with `@Disabled` annotation
- **Workaround:** None currently implemented
- **Fix complexity:** Medium to High (requires changes to rollback mechanism)
- **Priority:** Low to Medium (affects edge cases in reorg scenarios)
