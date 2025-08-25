# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# AI Dev Tasks
Use these files when I request structured feature development using PRDs:
/ai-dev-tasks/create-prd.md
/ai-dev-tasks/generate-tasks.md
/ai-dev-tasks/process-task-list.md

## Build and Development Commands

### Build
```bash
./gradlew build
```

### Test Commands
```bash
# Run all tests
./gradlew test

# Run integration tests
./gradlew integrationTest

# Run acceptance tests (all)
./gradlew acceptanceTest

# Run specific acceptance test suites
./gradlew acceptanceTestNotPrivacy      # Runs mainnet tests only
./gradlew acceptanceTestCliqueBft       # Runs Clique and BFT tests
./gradlew acceptanceTestPermissioning   # Runs permissioning tests

# Run a specific test class
./gradlew test --tests <TestClassName>

# Run tests with specific pattern
./gradlew test --tests "*.<TestClassName>.*"
```

### Code Quality
```bash
# Check code formatting
./gradlew spotlessCheck

# Apply code formatting
./gradlew spotlessApply

# Run all checks (includes spotlessCheck)
./gradlew check

# Generate code coverage report
./gradlew jacocoTestReport
```

### Default Tasks
Running `./gradlew` without arguments executes: `build`, `checkLicense`, `javadoc`

## High-Level Architecture

### Core Modules

**besu/** - Main entry point and CLI
- `Besu.java` - Bootstrap class and main entry point
- `BesuCommand.java` - CLI command handling
- `BesuController.java` - Core controller that orchestrates the node
- Different controller builders for consensus mechanisms (Mainnet, Clique, IBFT, QBFT, Merge)

**ethereum/** - Core Ethereum implementation
- `ethereum/core/` - Core blockchain data structures and processing
  - `Blockchain.java` - Main blockchain interface
  - `DefaultBlockchain.java` - Default blockchain implementation
  - Transaction processing and validation
- `ethereum/eth/` - Ethereum wire protocol and synchronization
  - `sync/` - Synchronization logic including headers-first sync
  - `manager/` - Peer management
  - `transactions/` - Transaction pool management
- `ethereum/api/` - JSON-RPC API implementation

**consensus/** - Consensus algorithm implementations
- `clique/` - Clique (PoA) consensus
- `ibft/` - IBFT consensus
- `qbft/` - QBFT consensus  
- `merge/` - Post-merge (PoS) consensus

**evm/** - Ethereum Virtual Machine implementation

**services/** - Shared service components
- `kvstore/` - Key-value storage abstraction
- `pipeline/` - Pipeline processing framework
- `tasks/` - Asynchronous task management

### Key Architectural Patterns

1. **Dependency Injection**: Uses Dagger for dependency injection (see `BesuComponent`)

2. **Pipeline Processing**: Many operations use the pipeline framework for efficient parallel processing

3. **Plugin System**: Extensible plugin architecture via `plugin-api/`

4. **Storage Abstraction**: Database operations abstracted through key-value store interfaces

5. **Consensus Abstraction**: Different consensus mechanisms implement common interfaces allowing runtime selection

### Synchronization Flow

The sync process (relevant to current branch `sync_headers_first`):
1. `DefaultSynchronizer` orchestrates the sync process
2. `ChainDownloader` manages the download pipeline
3. `DownloadHeadersStep` downloads headers first
4. `DownloadBodiesStep` downloads block bodies
5. Headers are validated before bodies are fetched for efficiency