# Bonsai Archiver Performance Optimization Design

**Date:** 2026-02-19
**Branch:** bonsai_archive_archiver
**Status:** Approved

## Problem Statement

The current Bonsai archiver implementation takes an extremely long time to catch up after syncing mainnet. When millions of blocks need archiving, the current approach processes blocks too slowly due to:

1. Individual DB transactions per entry (each `moveDBEntry()` creates/commits a transaction)
2. Sequential processing with no batching
3. Repeated lookups for block headers and TrieLogs
4. Low `CATCHUP_LIMIT` of 1,000 blocks per invocation

## Goals

- Achieve 20-100x improvement in archiving throughput
- Reduce catch-up time from days/weeks to hours
- Acceptable trade-offs: higher memory and I/O during archiving

## Design Overview

Combine **batched transactions** with **data access optimization** to dramatically improve throughput.

## Component 1: Batched Transaction Architecture

### Current Flow (Slow)
```
For each block:
  For each account change:
    openTx → remove → put → commit  // SLOW: individual commits
  For each storage change:
    openTx → remove → put → commit  // SLOW: individual commits
```

### Proposed Flow (Fast)
```
openTx
For batch of blocks (up to CATCHUP_LIMIT=50,000):
  For each account change:
    accumulate remove+put in transaction
  For each storage change:
    accumulate remove+put in transaction
  Every BATCH_SIZE=10,000 entries:
    commit transaction
    openTx (new transaction for next batch)
  Every 1,000 blocks:
    update latestArchivedBlock marker
Final commit
```

### Key Parameters

| Parameter | Current | Proposed | Rationale |
|-----------|---------|----------|-----------|
| CATCHUP_LIMIT | 1,000 | 50,000 | Process more blocks per invocation |
| BATCH_SIZE | 1 | 10,000 | Batch entries before commit |
| Progress log interval | 100 | 1,000 | Reduce log noise |

### API Changes

New methods in `PathBasedWorldStateKeyValueStorage`:

```java
/**
 * Archive previous account state using an existing transaction (batched).
 * Does NOT commit - caller manages transaction lifecycle.
 */
public int archivePreviousAccountStateBatched(
    SegmentedKeyValueStorageTransaction tx,
    BlockHeader previousBlockHeader,
    Hash accountHash);

/**
 * Archive previous storage state using an existing transaction (batched).
 * Does NOT commit - caller manages transaction lifecycle.
 */
public int archivePreviousStorageStateBatched(
    SegmentedKeyValueStorageTransaction tx,
    BlockHeader previousBlockHeader,
    Bytes storageSlotKey);
```

Existing methods remain for backward compatibility.

## Component 2: Data Access Optimization

### Header Caching

Cache block headers for the current batch to avoid repeated DB lookups:

```java
Map<Hash, BlockHeader> headerCache = new HashMap<>();

// Pre-populate before processing batch
blocksToArchive.forEach((blockNum, blockHash) -> {
    BlockHeader header = blockchain.getBlockHeader(blockHash).get();
    headerCache.put(blockHash, header);
    // Also cache parent header
    blockchain.getBlockHeader(header.getParentHash())
              .ifPresent(parent -> headerCache.put(header.getParentHash(), parent));
});
```

### TrieLog Pre-fetching

Pre-fetch TrieLogs for all blocks in the current batch:

```java
Map<Hash, TrieLog> trieLogCache = new HashMap<>();

blocksToArchive.forEach((blockNum, blockHash) -> {
    trieLogManager.getTrieLogLayer(blockHash)
                  .ifPresent(log -> trieLogCache.put(blockHash, log));
});
```

### Optimized getNearestBefore Pattern

Current approach repeatedly calls `getNearestBefore` in a while loop. Optimize by collecting all matching entries in fewer DB operations.

## Error Handling

### Transaction Safety

- Catch exceptions during batch commits and log last successfully archived block
- Progress marker (`latestArchivedBlock`) updated every 1,000 blocks within a batch
- On failure, re-processing starts from last committed progress marker

### Memory Bounds

- Header cache bounded to current batch size (cleared between invocations)
- TrieLog cache bounded to current batch size (cleared between invocations)
- Intermediate commits every BATCH_SIZE entries prevent unbounded growth

### Graceful Interruption

- Maintain existing `archiveMutex.tryLock()` pattern
- Check for thread interruption between blocks
- Commit partial progress on shutdown signal

## Metrics

### Existing Metrics (unchanged)
- `archived_blocks_state` - gauge of total blocks archived

### New Metrics
- `archive_batch_duration_seconds` - histogram of batch processing time
- `archive_entries_per_second` - gauge of archiving throughput

## Testing Strategy

### Unit Tests

1. **BonsaiArchiverTest**
   - Verify entries are batched, not committed individually
   - Test CATCHUP_LIMIT and BATCH_SIZE boundary conditions
   - Test cache population and clearing

2. **PathBasedWorldStateKeyValueStorageTest**
   - Test `archivePreviousAccountStateBatched` accumulates in transaction
   - Test `archivePreviousStorageStateBatched` accumulates in transaction
   - Test batch commit moves correct entries

### Integration Tests

1. Archive 1000+ blocks with synthetic TrieLogs
2. Verify measurable performance improvement (at least 10x)
3. Test recovery after simulated mid-batch failure

### Manual Testing

1. Run against actual mainnet synced node
2. Measure blocks/second before and after
3. Monitor memory usage during archiving

## Implementation Order

1. Add batched archive methods to `PathBasedWorldStateKeyValueStorage`
2. Add header and TrieLog caching in `BonsaiArchiver`
3. Refactor `moveBlockStateToArchive()` to use batched approach
4. Update CATCHUP_LIMIT and add BATCH_SIZE constant
5. Add new metrics
6. Update progress logging
7. Add unit tests for batched methods
8. Integration testing
9. Manual performance validation

## Expected Results

| Metric | Before | After (Expected) |
|--------|--------|------------------|
| Blocks/second | ~10-50 | ~1,000-5,000 |
| Catch-up time (20M blocks) | Days/weeks | Hours |
| Memory during archiving | Low | Moderate (bounded) |
| I/O during archiving | Low | Higher (acceptable) |

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Memory pressure from caching | Bounded cache sizes, cleared between batches |
| Long-running transactions | Intermediate commits every BATCH_SIZE entries |
| Data loss on failure | Progress marker updated every 1,000 blocks |
| RocksDB "Busy" errors | Existing retry logic maintained |
