# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Besu is an Apache 2.0 licensed, MainNet compatible Ethereum client written in Java. It supports public and private networks, multiple consensus mechanisms (PoW, PoA via Clique/IBFT/QBFT, and PoS via merge), and provides JSON-RPC APIs for interaction.

## Build Commands

**Requirements:** Java 21+

```bash
# Build the project (includes spotless check)
./gradlew build

# Build for development (applies formatting, builds, generates javadoc)
./gradlew dev

# Apply code formatting (Spotless with Google Java Format)
./gradlew spotlessApply

# Check formatting without applying
./gradlew spotlessCheck

# Generate distribution
./gradlew installDist
```

## Testing Commands

```bash
# Run unit tests
./gradlew test

# Run a single test class
./gradlew :ethereum:core:test --tests "BonsaiWorldStateTest"

# Run a single test method
./gradlew :ethereum:core:test --tests "BonsaiWorldStateTest.methodName"

# Run integration tests
./gradlew integrationTest

# Run acceptance tests (requires installDist first)
./gradlew acceptanceTest

# Run Ethereum reference tests
./gradlew referenceTests

# Run reference tests filtered by hardfork or EIP
./gradlew referenceTests --tests "*ExecutionSpec*_prague_*"
./gradlew referenceTests --tests "*eip7702*"

# Run devnet/pre-release reference tests
./gradlew referenceTestsDevnet
```

## Architecture Overview

### Module Structure

- **app/** - Main application entry point, CLI (`BesuCommand`), and runner
- **besu/** - Dagger dependency injection components and service implementations
- **ethereum/core** - Core Ethereum protocol implementation (blocks, transactions, state)
- **ethereum/api** - JSON-RPC API implementation
- **ethereum/eth** - Ethereum wire protocol (P2P networking)
- **ethereum/p2p** - P2P networking layer (devp2p)
- **ethereum/trie** - Merkle Patricia Trie implementations
- **ethereum/rlp** - RLP encoding/decoding
- **evm/** - Ethereum Virtual Machine implementation
- **consensus/** - Consensus mechanism implementations (clique, ibft, qbft, merge)
- **plugin-api/** - Public API for Besu plugins
- **services/** - Internal services (kvstore, pipeline, tasks)

### World State Storage

Two world state implementations exist in `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/`:

- **forest/** - Legacy "forest" mode using full Merkle Patricia Tries
- **pathbased/bonsai/** - Bonsai Tries (default) - flat database with path-based storage
- **pathbased/common/** - Shared path-based trie infrastructure

Key Bonsai classes:
- `BonsaiWorldStateProvider` - Creates world state instances
- `BonsaiWorldStateKeyValueStorage` - Database storage layer
- `BonsaiArchiveFlatDbStrategy` - Historical state access for archive nodes

### Consensus Mechanisms

Located in `consensus/`:
- **merge/** - Proof of Stake (The Merge)
- **clique/** - Proof of Authority for Ethereum testnets
- **qbft/** - QBFT Byzantine fault-tolerant consensus
- **ibft/** - IBFT legacy support

### EVM Implementation

The EVM is in `evm/src/main/java/org/hyperledger/besu/evm/`:
- `EVM.java` - Main EVM execution engine
- `MainnetEVMs.java` - EVM configurations per hardfork
- `operation/` - Individual opcode implementations
- `precompile/` - Precompiled contract implementations
- `gascalculator/` - Gas cost calculations per hardfork

## Code Style

- Formatting: Google Java Format (enforced via Spotless)
- Import order: `org.hyperledger`, `java`, then others
- License headers are automatically managed
- Run `./gradlew spotlessApply` before committing

## JMH Benchmarks

```bash
# Run benchmarks in a module
./gradlew :ethereum:core:jmh

# Filter specific benchmarks
./gradlew :ethereum:core:jmh -Pincludes=SomeBenchmark

# With async profiler
./gradlew :ethereum:core:jmh -PasyncProfiler=/path/to/libasyncProfiler.so
```

## Useful Resources

- [Besu Documentation](https://besu.hyperledger.org)
- [REFERENCE_TESTS.md](REFERENCE_TESTS.md) - Running Ethereum reference tests with JSON tracing
- [docs/PROFILING.md](docs/PROFILING.md) - Profiling guide with Async Profiler
