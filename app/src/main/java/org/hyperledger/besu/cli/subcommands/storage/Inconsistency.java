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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;

import java.util.Optional;

/**
 * Represents an inconsistency found between flat DB and trielog data.
 *
 * @param blockNumber the block number where the inconsistency was found
 * @param blockHash the hash of the block
 * @param type the type of inconsistency
 * @param address the account address affected
 * @param storageKey the storage slot key (for storage inconsistencies)
 * @param fieldName the field name that differs (e.g., "nonce", "balance", "value")
 * @param expectedValue the expected value from trielog
 * @param actualValue the actual value from flat DB
 * @param description human-readable description of the inconsistency
 */
record Inconsistency(
    long blockNumber,
    Hash blockHash,
    InconsistencyType type,
    Address address,
    Optional<StorageSlotKey> storageKey,
    String fieldName,
    String expectedValue,
    String actualValue,
    String description) {

  /** Types of inconsistencies that can be detected. */
  enum InconsistencyType {
    /** Account exists in trielog but missing from flat DB. */
    ACCOUNT_MISSING,
    /** Account exists in flat DB but should be deleted according to trielog. */
    ACCOUNT_UNEXPECTED,
    /** Account field value differs between trielog and flat DB. */
    ACCOUNT_FIELD_MISMATCH,
    /** Storage slot exists in trielog but missing from flat DB. */
    STORAGE_MISSING,
    /** Storage slot exists in flat DB but should be deleted according to trielog. */
    STORAGE_UNEXPECTED,
    /** Storage slot value differs between trielog and flat DB. */
    STORAGE_VALUE_MISMATCH,
    /** Code exists in trielog but missing from flat DB. */
    CODE_MISSING,
    /** Code content differs between trielog and flat DB. */
    CODE_MISMATCH
  }
}
