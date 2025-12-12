/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.cli.subcommands.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.cli.subcommands.storage.Inconsistency.InconsistencyType;
import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage.NearestKeyValue;

import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.bouncycastle.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Checks flat DB consistency against trielog data. */
public class FlatDbConsistencyChecker {
  private static final Logger LOG = LoggerFactory.getLogger(FlatDbConsistencyChecker.class);

  private static final byte[] DELETED_ACCOUNT_VALUE = new byte[0];
  private static final byte[] DELETED_STORAGE_VALUE = new byte[0];

  private final Blockchain blockchain;
  private final SegmentedKeyValueStorage composedStorage;
  private final KeyValueStorage trieLogStorage;
  private final InconsistencyReporter reporter;
  private final TrieLogFactoryImpl trieLogFactory;

  /**
   * Creates a new flat DB consistency checker.
   *
   * @param blockchain the blockchain
   * @param composedStorage the world state storage
   * @param trieLogStorage the trielog storage (KeyValueStorage, not segmented)
   * @param reporter the inconsistency reporter
   */
  public FlatDbConsistencyChecker(
      final Blockchain blockchain,
      final SegmentedKeyValueStorage composedStorage,
      final KeyValueStorage trieLogStorage,
      final InconsistencyReporter reporter) {
    this.blockchain = blockchain;
    this.composedStorage = composedStorage;
    this.trieLogStorage = trieLogStorage;
    this.reporter = reporter;
    this.trieLogFactory = new TrieLogFactoryImpl();
  }

  /**
   * Performs the consistency check from startBlock to endBlock.
   *
   * @param startBlock the starting block number (inclusive)
   * @param endBlock the ending block number (inclusive)
   */
  public void check(final long startBlock, final long endBlock) {
    LOG.info("Starting flat DB consistency check from block {} to {}", startBlock, endBlock);

    for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
      // Get block hash
      Optional<Hash> blockHash = blockchain.getBlockHashByNumber(blockNum);
      if (blockHash.isEmpty()) {
        LOG.warn("Block {} not found in blockchain, skipping", blockNum);
        continue;
      }

      // Get trielog
      Optional<TrieLogLayer> trieLog = getTrieLog(blockHash.get());
      if (trieLog.isEmpty()) {
        LOG.warn("Trielog not found for block {} ({}), skipping", blockNum, blockHash.get());
        continue;
      }

      // Verify this block
      verifyBlock(blockNum, blockHash.get(), trieLog.get());

      // Progress reporting
      if (blockNum % 1000 == 0) {
        LOG.info(
            "Progress: {}/{} blocks checked, {} inconsistencies found",
            blockNum,
            endBlock,
            reporter.getTotalInconsistencies());
      }
    }

    LOG.info("Consistency check complete. Checked {} blocks", (endBlock - startBlock + 1));
    reporter.printSummary();
  }

  /**
   * Retrieves the trielog for a given block hash.
   *
   * @param blockHash the block hash
   * @return the trielog layer, or empty if not found
   */
  private Optional<TrieLogLayer> getTrieLog(final Hash blockHash) {
    return trieLogStorage
        .get(blockHash.toArrayUnsafe())
        .flatMap(
            bytes -> {
              try {
                return Optional.of(trieLogFactory.deserialize(bytes));
              } catch (Exception e) {
                LOG.error("Failed to deserialize trielog for block {}", blockHash, e);
                return Optional.empty();
              }
            });
  }

  /**
   * Verifies a single block by comparing trielog changes against flat DB.
   *
   * @param blockNum the block number
   * @param blockHash the block hash
   * @param trieLog the trielog layer
   */
  private void verifyBlock(
      final long blockNum, final Hash blockHash, final TrieLogLayer trieLog) {

    // Verify accounts
    for (Map.Entry<Address, PathBasedValue<AccountValue>> entry :
        trieLog.getAccountChanges().entrySet()) {
      verifyAccount(blockNum, blockHash, entry.getKey(), entry.getValue());
    }

    // Verify storage
    for (Map.Entry<Address, Map<StorageSlotKey, PathBasedValue<UInt256>>> entry :
        trieLog.getStorageChanges().entrySet()) {
      verifyStorage(blockNum, blockHash, entry.getKey(), entry.getValue());
    }

    // Verify code
    for (Map.Entry<Address, PathBasedValue<Bytes>> entry : trieLog.getCodeChanges().entrySet()) {
      verifyCode(blockNum, blockHash, entry.getKey(), entry.getValue());
    }
  }

  /**
   * Verifies an account change against flat DB.
   *
   * @param blockNum the block number
   * @param blockHash the block hash
   * @param address the account address
   * @param change the account change from trielog
   */
  private void verifyAccount(
      final long blockNum,
      final Hash blockHash,
      final Address address,
      final PathBasedValue<AccountValue> change) {

    AccountValue expected = change.getUpdated();
    Hash accountHash = Hash.hash(address);

    // Read from flat DB
    Optional<AccountValue> actual = getAccountAtBlock(accountHash, blockNum);

    // Compare
    if (expected == null) {
      // Account should be deleted
      if (actual.isPresent()) {
        reporter.report(
            new Inconsistency(
                blockNum,
                blockHash,
                InconsistencyType.ACCOUNT_UNEXPECTED,
                address,
                Optional.empty(),
                "account",
                "null",
                accountValueToString(actual.get()),
                "Account should be deleted but exists in flat DB"));
      }
    } else {
      // Account should exist
      if (actual.isEmpty()) {
        reporter.report(
            new Inconsistency(
                blockNum,
                blockHash,
                InconsistencyType.ACCOUNT_MISSING,
                address,
                Optional.empty(),
                "account",
                accountValueToString(expected),
                "null",
                "Account exists in trielog but missing from flat DB"));
      } else {
        // Compare fields
        compareAccountFields(blockNum, blockHash, address, expected, actual.get());
      }
    }
  }

  /**
   * Compares individual account fields between expected and actual values.
   *
   * @param blockNum the block number
   * @param blockHash the block hash
   * @param address the account address
   * @param expected the expected account value from trielog
   * @param actual the actual account value from flat DB
   */
  private void compareAccountFields(
      final long blockNum,
      final Hash blockHash,
      final Address address,
      final AccountValue expected,
      final AccountValue actual) {

    if (expected.getNonce() != actual.getNonce()) {
      reporter.report(
          new Inconsistency(
              blockNum,
              blockHash,
              InconsistencyType.ACCOUNT_FIELD_MISMATCH,
              address,
              Optional.empty(),
              "nonce",
              String.valueOf(expected.getNonce()),
              String.valueOf(actual.getNonce()),
              "Nonce mismatch"));
    }

    if (!expected.getBalance().equals(actual.getBalance())) {
      reporter.report(
          new Inconsistency(
              blockNum,
              blockHash,
              InconsistencyType.ACCOUNT_FIELD_MISMATCH,
              address,
              Optional.empty(),
              "balance",
              expected.getBalance().toString(),
              actual.getBalance().toString(),
              "Balance mismatch"));
    }

    if (!expected.getStorageRoot().equals(actual.getStorageRoot())) {
      reporter.report(
          new Inconsistency(
              blockNum,
              blockHash,
              InconsistencyType.ACCOUNT_FIELD_MISMATCH,
              address,
              Optional.empty(),
              "storageRoot",
              expected.getStorageRoot().toHexString(),
              actual.getStorageRoot().toHexString(),
              "Storage root mismatch"));
    }

    if (!expected.getCodeHash().equals(actual.getCodeHash())) {
      reporter.report(
          new Inconsistency(
              blockNum,
              blockHash,
              InconsistencyType.ACCOUNT_FIELD_MISMATCH,
              address,
              Optional.empty(),
              "codeHash",
              expected.getCodeHash().toHexString(),
              actual.getCodeHash().toHexString(),
              "Code hash mismatch"));
    }
  }

  /**
   * Verifies storage changes for an account against flat DB.
   *
   * @param blockNum the block number
   * @param blockHash the block hash
   * @param address the account address
   * @param storageChanges the storage changes from trielog
   */
  private void verifyStorage(
      final long blockNum,
      final Hash blockHash,
      final Address address,
      final Map<StorageSlotKey, PathBasedValue<UInt256>> storageChanges) {

    Hash accountHash = Hash.hash(address);

    for (Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> entry : storageChanges.entrySet()) {
      StorageSlotKey slotKey = entry.getKey();
      UInt256 expected = entry.getValue().getUpdated();

      // Read from flat DB
      Optional<UInt256> actual = getStorageAtBlock(accountHash, slotKey, blockNum);

      // Compare
      if (expected == null || expected.isZero()) {
        // Storage should be deleted/zero
        if (actual.isPresent() && !actual.get().isZero()) {
          reporter.report(
              new Inconsistency(
                  blockNum,
                  blockHash,
                  InconsistencyType.STORAGE_UNEXPECTED,
                  address,
                  Optional.of(slotKey),
                  "value",
                  "0",
                  actual.get().toHexString(),
                  "Storage slot should be zero/deleted but has value in flat DB"));
        }
      } else {
        // Storage should have specific value
        if (actual.isEmpty() || actual.get().isZero()) {
          reporter.report(
              new Inconsistency(
                  blockNum,
                  blockHash,
                  InconsistencyType.STORAGE_MISSING,
                  address,
                  Optional.of(slotKey),
                  "value",
                  expected.toHexString(),
                  actual.map(UInt256::toHexString).orElse("0"),
                  "Storage slot exists in trielog but missing/zero in flat DB"));
        } else if (!expected.equals(actual.get())) {
          reporter.report(
              new Inconsistency(
                  blockNum,
                  blockHash,
                  InconsistencyType.STORAGE_VALUE_MISMATCH,
                  address,
                  Optional.of(slotKey),
                  "value",
                  expected.toHexString(),
                  actual.get().toHexString(),
                  "Storage value mismatch"));
        }
      }
    }
  }

  /**
   * Verifies code changes for an account against flat DB.
   *
   * @param blockNum the block number
   * @param blockHash the block hash
   * @param address the account address
   * @param change the code change from trielog
   */
  private void verifyCode(
      final long blockNum,
      final Hash blockHash,
      final Address address,
      final PathBasedValue<Bytes> change) {

    Bytes expected = change.getUpdated();

    // Determine the expected code hash
    Hash expectedCodeHash = (expected == null || expected.isEmpty()) ? Hash.EMPTY : Hash.hash(expected);

    // Read from flat DB
    Optional<Bytes> actual = getCode(expectedCodeHash);

    // Compare
    if (expected == null || expected.isEmpty()) {
      // Code should be empty
      if (actual.isPresent() && !actual.get().isEmpty()) {
        reporter.report(
            new Inconsistency(
                blockNum,
                blockHash,
                InconsistencyType.CODE_MISMATCH,
                address,
                Optional.empty(),
                "code",
                "EMPTY",
                truncateHex(actual.get().toHexString()),
                "Code should be empty but exists in flat DB"));
      }
    } else {
      // Code should exist
      if (actual.isEmpty()) {
        reporter.report(
            new Inconsistency(
                blockNum,
                blockHash,
                InconsistencyType.CODE_MISSING,
                address,
                Optional.empty(),
                "code",
                truncateHex(expected.toHexString()),
                "null",
                "Code exists in trielog but missing from flat DB"));
      } else if (!expected.equals(actual.get())) {
        reporter.report(
            new Inconsistency(
                blockNum,
                blockHash,
                InconsistencyType.CODE_MISMATCH,
                address,
                Optional.empty(),
                "code",
                Hash.hash(expected).toHexString(),
                Hash.hash(actual.get()).toHexString(),
                "Code hash mismatch"));
      }
    }
  }

  /**
   * Reads an account from flat DB at a specific block number. Follows the same pattern as
   * BonsaiArchiveFlatDbStrategy.getFlatAccount().
   *
   * @param accountHash the account hash
   * @param blockNumber the block number
   * @return the account value, or empty if not found or deleted
   */
  private Optional<AccountValue> getAccountAtBlock(
      final Hash accountHash, final long blockNumber) {

    // Calculate archive key with block number suffix (same as BonsaiArchiveFlatDbStrategy)
    Bytes keyNearest = Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(blockNumber));

    // Try ACCOUNT_INFO_STATE first, then ACCOUNT_INFO_STATE_ARCHIVE
    Optional<NearestKeyValue> result =
        composedStorage
            .getNearestBefore(ACCOUNT_INFO_STATE, keyNearest)
            .filter(kv -> accountHash.commonPrefixLength(kv.key()) >= accountHash.size());

    if (result.isEmpty()) {
      result =
          composedStorage
              .getNearestBefore(ACCOUNT_INFO_STATE_ARCHIVE, keyNearest)
              .filter(kv -> accountHash.commonPrefixLength(kv.key()) >= accountHash.size());
    }

    // Filter out DELETED values (empty byte arrays)
    return result
        .filter(kv -> kv.value().isPresent() && kv.value().get().length > 0)
        .filter(kv -> !Arrays.areEqual(DELETED_ACCOUNT_VALUE, kv.value().get()))
        .flatMap(NearestKeyValue::wrapBytes)
        .map(bytes -> PmtStateTrieAccountValue.readFrom(RLP.input(bytes)));
  }

  /**
   * Reads storage from flat DB at a specific block number. Follows the same pattern as
   * BonsaiArchiveFlatDbStrategy.getFlatStorageValueByStorageSlotKey().
   *
   * @param accountHash the account hash
   * @param storageSlotKey the storage slot key
   * @param blockNumber the block number
   * @return the storage value, or empty if not found or deleted
   */
  private Optional<UInt256> getStorageAtBlock(
      final Hash accountHash, final StorageSlotKey storageSlotKey, final long blockNumber) {

    // Calculate natural key (accountHash + slotHash)
    Hash slotHash = storageSlotKey.getSlotHash();
    Bytes naturalKey = Bytes.concatenate(accountHash, slotHash);

    // Calculate archive key with block number suffix
    Bytes keyNearest = Bytes.concatenate(naturalKey, Bytes.ofUnsignedLong(blockNumber));

    // Try ACCOUNT_STORAGE_STORAGE first, then ACCOUNT_STORAGE_ARCHIVE
    Optional<NearestKeyValue> result =
        composedStorage
            .getNearestBefore(ACCOUNT_STORAGE_STORAGE, keyNearest)
            .filter(kv -> naturalKey.commonPrefixLength(kv.key()) >= naturalKey.size());

    if (result.isEmpty()) {
      result =
          composedStorage
              .getNearestBefore(ACCOUNT_STORAGE_ARCHIVE, keyNearest)
              .filter(kv -> naturalKey.commonPrefixLength(kv.key()) >= naturalKey.size());
    }

    // Filter out DELETED values (empty byte arrays)
    return result
        .filter(kv -> kv.value().isPresent() && kv.value().get().length > 0)
        .filter(kv -> !Arrays.areEqual(DELETED_STORAGE_VALUE, kv.value().get()))
        .flatMap(NearestKeyValue::wrapBytes)
        .map(UInt256::fromBytes);
  }

  /**
   * Reads code from flat DB by code hash.
   *
   * @param codeHash the code hash
   * @return the code bytes, or empty if not found
   */
  private Optional<Bytes> getCode(final Hash codeHash) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    }

    return composedStorage.get(CODE_STORAGE, codeHash.toArrayUnsafe()).map(Bytes::wrap);
  }

  /**
   * Converts an AccountValue to a string representation.
   *
   * @param accountValue the account value
   * @return string representation
   */
  private String accountValueToString(final AccountValue accountValue) {
    return String.format(
        "AccountValue{nonce=%d, balance=%s, storageRoot=%s, codeHash=%s}",
        accountValue.getNonce(),
        accountValue.getBalance(),
        accountValue.getStorageRoot().toHexString(),
        accountValue.getCodeHash().toHexString());
  }

  /**
   * Truncates a hex string for display.
   *
   * @param hex the hex string
   * @return truncated hex string
   */
  private String truncateHex(final String hex) {
    if (hex.length() > 20) {
      return hex.substring(0, 20) + "...";
    }
    return hex;
  }
}
