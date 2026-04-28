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
package org.hyperledger.besu.cli.subcommands.storage.archivestats;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;

/** The two archive column families this tool scans. */
public enum ArchiveCf {
  /** Account-level archive entries. Key = accountHash(32) || blockNumber(8). */
  ACCOUNT(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE, 32, "account"),
  /** Storage-slot archive entries. Key = accountHash(32) || slotHash(32) || blockNumber(8). */
  STORAGE(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE, 64, "storage");

  /** Width of the trailing big-endian block-number suffix in archive keys. */
  public static final int BLOCK_NUMBER_SUFFIX_BYTES = 8;

  private final KeyValueSegmentIdentifier segment;
  private final int prefixBytes;
  private final String cliLabel;

  ArchiveCf(final KeyValueSegmentIdentifier segment, final int prefixBytes, final String cliLabel) {
    this.segment = segment;
    this.prefixBytes = prefixBytes;
    this.cliLabel = cliLabel;
  }

  /**
   * Returns the Besu segment identifier for this CF.
   *
   * @return the Besu segment identifier for this CF
   */
  public KeyValueSegmentIdentifier segment() {
    return segment;
  }

  /**
   * Returns the natural-key (prefix) byte length for this CF — 32 for account, 64 for storage.
   *
   * @return the natural-key (prefix) byte length for this CF — 32 for account, 64 for storage
   */
  public int prefixBytes() {
    return prefixBytes;
  }

  /**
   * Returns the total expected key length in bytes (prefix + 8-byte block number).
   *
   * @return total expected key length in bytes (prefix + 8-byte block number)
   */
  public int expectedKeyLength() {
    return prefixBytes + BLOCK_NUMBER_SUFFIX_BYTES;
  }

  /**
   * Returns the CLI label used in {@code --cf} ("account" or "storage").
   *
   * @return CLI label used in {@code --cf} ("account" or "storage")
   */
  public String cliLabel() {
    return cliLabel;
  }
}
