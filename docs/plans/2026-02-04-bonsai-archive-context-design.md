# Bonsai Archive Context Design

## Problem

The Bonsai archive flat DB strategy reads `WORLD_BLOCK_NUMBER_KEY` from the database to determine context for reads and writes. This global state causes issues during reorgs:

1. Block 1A persisted: `WORLD_BLOCK_NUMBER_KEY = 1`, account stored with suffix `1`
2. Reorg to block 1B: `WORLD_BLOCK_NUMBER_KEY` still `1` in DB
3. Block 1B writes use suffix `2` (from stale DB value + 1)
4. Reads find wrong value (suffix `1` from orphaned chain)

The root cause: context is global (in DB) rather than local to each world state instance.

## Solution

Pass context through method parameters using suppliers. Each `BonsaiArchiveWorldState` instance holds its own read and write contexts, set by `BonsaiArchiveWorldStateProvider`.

### Key Design Decisions

1. **Context lives on world state instance** - `BonsaiArchiveWorldState` holds `readContext` and `writeContext` fields
2. **Provider sets context** - `BonsaiArchiveWorldStateProvider` sets appropriate contexts based on operation (head update, historical query, reorg)
3. **Separate read/write contexts** - Different semantics require separate tracking
4. **Context always required** - Archive strategy throws if context not provided (no DB fallback)
5. **Non-archive strategies ignore context** - Parameter added but unused in Full/Partial modes

## Architecture

### Context Flow

```
BonsaiArchiveWorldStateProvider
    │
    │ creates and sets contexts
    ▼
BonsaiArchiveWorldState
    │ readContext: Optional<BonsaiContext>
    │ writeContext: Optional<BonsaiContext>
    │
    │ provides suppliers
    ▼
BonsaiWorldStateKeyValueStorage
    │
    │ passes suppliers through
    ▼
BonsaiArchiveFlatDbStrategy
    │
    │ uses context for key suffix calculation
    ▼
Database (no longer source of context)
```

### New Class: BonsaiArchiveWorldState

```java
public class BonsaiArchiveWorldState extends BonsaiWorldState {

    private Optional<BonsaiContext> readContext = Optional.empty();
    private Optional<BonsaiContext> writeContext = Optional.empty();

    public Supplier<Optional<BonsaiContext>> getReadContextSupplier() {
        return () -> readContext;
    }

    public Supplier<Optional<BonsaiContext>> getWriteContextSupplier() {
        return () -> writeContext;
    }

    public void setReadContext(final BonsaiContext context) {
        this.readContext = Optional.of(context);
    }

    public void setWriteContext(final BonsaiContext context) {
        this.writeContext = Optional.of(context);
    }
}
```

### Strategy Interface Changes

Add `Supplier<Optional<BonsaiContext>>` parameter to:

**Read methods:**
- `getFlatAccount(..., readContextSupplier)`
- `getFlatStorageValueByStorageSlotKey(..., readContextSupplier)`

**Write methods:**
- `putFlatAccount(..., writeContextSupplier)`
- `removeFlatAccount(..., writeContextSupplier)`
- `putFlatAccountStorageValueByStorageSlotHash(..., writeContextSupplier)`
- `removeFlatAccountStorageValueByStorageSlotHash(..., writeContextSupplier)`

### Provider Context Setting

**Normal block processing (head update):**
```java
// Read context = parent block (state we're reading from)
worldState.setReadContext(new BonsaiContext(parentHeader.getNumber()));

// Write context = target block (state we're writing to)
worldState.setWriteContext(new BonsaiContext(targetHeader.getNumber()));
```

**Historical query (read only):**
```java
// Read context = target block
worldState.setReadContext(new BonsaiContext(targetHeader.getNumber()));
// No write context needed
```

**Reorg rollback/rollforward:**
```java
// Read context = block we're rolling back from
worldState.setReadContext(new BonsaiContext(rollbackTargetBlock));

// Write context = block we're rolling forward to
worldState.setWriteContext(new BonsaiContext(newForkBlock));
```

### Archive Strategy Changes

Replace DB reads with passed context:

```java
@Override
public Optional<Bytes> getFlatAccount(
    Supplier<Optional<Bytes>> worldStateRootHashSupplier,
    NodeLoader nodeLoader,
    Hash accountHash,
    SegmentedKeyValueStorage storage,
    Supplier<Optional<BonsaiContext>> readContextSupplier) {

    // Context is required for archive mode
    BonsaiContext readContext = readContextSupplier.get()
        .orElseThrow(() -> new IllegalStateException(
            "Read context required for archive flat DB"));

    Bytes keyNearest = calculateArchiveKeyWithMaxSuffix(
        Optional.of(readContext),
        accountHash.getBytes().toArrayUnsafe());

    // ... rest of lookup logic
}
```

## Files to Change

| File | Changes |
|------|---------|
| **New: `BonsaiArchiveWorldState.java`** | New class extending `BonsaiWorldState`, holds `readContext` and `writeContext` fields with getters/setters |
| **`BonsaiArchiveWorldStateProvider.java`** | Create `BonsaiArchiveWorldState` instances, set contexts for each scenario |
| **`FlatDbStrategy.java`** | Add `Supplier<Optional<BonsaiContext>>` parameter to abstract methods |
| **`BonsaiFlatDbStrategy.java`** | Add context parameter to interface methods |
| **`BonsaiArchiveFlatDbStrategy.java`** | Use passed context instead of reading from DB, throw if context missing |
| **`BonsaiFullFlatDbStrategy.java`** | Add parameter to method signatures, ignore it |
| **`BonsaiPartialFlatDbStrategy.java`** | Add parameter to method signatures, ignore it |
| **`BonsaiWorldStateKeyValueStorage.java`** | Add context parameter to `getAccount`, `getStorageValueByStorageSlotKey`; update `Updater` constructor and methods |
| **`BonsaiSnapshotWorldStateKeyValueStorage.java`** | Pass through context parameters |
| **`BonsaiWorldStateLayerStorage.java`** | Pass through context parameters |

## Testing

The existing `BonsaiArchiveReorgIntegrationTest` should pass after implementation. Key scenarios:

1. Normal block processing with correct context
2. Historical queries return correct state
3. Reorg correctly uses new chain's context
4. Orphaned chain values not returned after reorg
