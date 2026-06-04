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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex;

import org.apache.tuweni.bytes.Bytes;

/**
 * Pure static utility class providing the canonical key-construction helpers for Design 5's
 * trie-node differential index.
 *
 * <p>Key shapes (all big-endian):
 *
 * <ul>
 *   <li>Account natural key = {@code location}
 *   <li>Storage natural key = {@code accountHash(32) ‖ location}
 *   <li>History CF key = {@code naturalKey ‖ block(8 bytes BE)}
 *   <li>Index/range-marker CF key = {@code naturalKey ‖ rangeId(8 bytes BE)}
 *   <li>Bloom CF key = {@code rangeId(8 bytes BE)}
 * </ul>
 */
public final class ArchiveNodeKey {

  /** Number of blocks per index range. Canonical constant for Design 5. */
  public static final long RANGE_SIZE = 1_000_000L;

  private ArchiveNodeKey() {
    // pure static utility — no instances
  }

  /**
   * Returns the natural key for an account-trie node: the compact path {@code location} bytes.
   *
   * <p>This method exists for call-site clarity and symmetry with {@link #storage}.
   *
   * @param location the compact path nibbles for the trie node
   * @return {@code location} unchanged
   */
  public static Bytes account(final Bytes location) {
    return location;
  }

  /**
   * Returns the natural key for a storage-trie node: {@code accountHash(32) ‖ location}.
   *
   * @param accountHash the 32-byte account hash that owns the storage trie
   * @param location the compact path nibbles within the storage trie
   * @return concatenation of {@code accountHash} and {@code location}
   * @throws IllegalArgumentException if {@code accountHash.size() != 32}
   */
  public static Bytes storage(final Bytes accountHash, final Bytes location) {
    if (accountHash.size() != 32) {
      throw new IllegalArgumentException(
          "accountHash must be exactly 32 bytes, got " + accountHash.size());
    }
    return Bytes.concatenate(accountHash, location);
  }

  /**
   * Returns the range identifier for a given block number: {@code block / RANGE_SIZE}.
   *
   * @param block the block number (must be &gt;= 0)
   * @return the range identifier
   * @throws IllegalArgumentException if {@code block < 0}
   */
  public static long rangeId(final long block) {
    if (block < 0) {
      throw new IllegalArgumentException("block must be >= 0, got " + block);
    }
    return block / RANGE_SIZE;
  }

  /**
   * Constructs a history-CF key: {@code naturalKey ‖ block(8 bytes BE)}.
   *
   * @param naturalKey the account or storage natural key
   * @param block the block number
   * @return the history key
   */
  public static Bytes historyKey(final Bytes naturalKey, final long block) {
    return Bytes.concatenate(naturalKey, Bytes.ofUnsignedLong(block));
  }

  /**
   * Extracts the block number from a history key (the trailing 8 big-endian bytes).
   *
   * @param historyKey a key built by {@link #historyKey}
   * @return the block number
   * @throws IllegalArgumentException if {@code historyKey.size() < 8}
   */
  public static long blockFromHistoryKey(final Bytes historyKey) {
    if (historyKey.size() < 8) {
      throw new IllegalArgumentException(
          "historyKey too short: expected >= 8 bytes, got " + historyKey.size());
    }
    return historyKey.getLong(historyKey.size() - 8);
  }

  /**
   * Extracts the natural key prefix from a history key (all bytes except the trailing 8).
   *
   * @param historyKey a key built by {@link #historyKey}
   * @return the natural key
   * @throws IllegalArgumentException if {@code historyKey.size() < 8}
   */
  public static Bytes naturalKeyFromHistoryKey(final Bytes historyKey) {
    if (historyKey.size() < 8) {
      throw new IllegalArgumentException(
          "historyKey too short: expected >= 8 bytes, got " + historyKey.size());
    }
    return historyKey.slice(0, historyKey.size() - 8);
  }

  /**
   * Constructs an index/range-marker CF key: {@code naturalKey ‖ rangeId(8 bytes BE)}.
   *
   * @param naturalKey the account or storage natural key
   * @param rangeId the range identifier
   * @return the range key
   */
  public static Bytes rangeKey(final Bytes naturalKey, final long rangeId) {
    return Bytes.concatenate(naturalKey, Bytes.ofUnsignedLong(rangeId));
  }

  /**
   * Constructs a bloom-CF key: {@code rangeId(8 bytes BE)}.
   *
   * @param rangeId the range identifier
   * @return the bloom key
   */
  public static Bytes bloomKey(final long rangeId) {
    return Bytes.ofUnsignedLong(rangeId);
  }
}
