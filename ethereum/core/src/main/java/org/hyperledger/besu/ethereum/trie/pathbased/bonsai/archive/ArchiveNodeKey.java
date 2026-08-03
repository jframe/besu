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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import org.apache.tuweni.bytes.Bytes;

/**
 * Pure static key-construction helpers for trie-node history storage. No range, sub-block, or bloom
 * key concepts — this design has no change-block index (see the design spec's Non-goal #2).
 */
public final class ArchiveNodeKey {

  private ArchiveNodeKey() {}

  /**
   * Returns the natural key for an account-trie node: the compact path {@code location} bytes,
   * unchanged. Exists for call-site clarity and symmetry with {@link #storage}.
   */
  public static Bytes account(final Bytes location) {
    return location;
  }

  /** Returns the natural key for a storage-trie node: {@code accountHash(32) ‖ location}. */
  public static Bytes storage(final Bytes accountHash, final Bytes location) {
    if (accountHash.size() != 32) {
      throw new IllegalArgumentException(
          "accountHash must be exactly 32 bytes, got " + accountHash.size());
    }
    return Bytes.concatenate(accountHash, location);
  }

  /** Constructs a history-CF key: {@code naturalKey ‖ block(8 bytes BE)}. */
  public static Bytes historyKey(final Bytes naturalKey, final long block) {
    return Bytes.concatenate(naturalKey, Bytes.ofUnsignedLong(block));
  }

  /** Extracts the block number from a history key (the trailing 8 big-endian bytes). */
  public static long blockFromHistoryKey(final Bytes historyKey) {
    if (historyKey.size() < 8) {
      throw new IllegalArgumentException(
          "historyKey too short: expected >= 8 bytes, got " + historyKey.size());
    }
    return historyKey.getLong(historyKey.size() - 8);
  }

  /** Extracts the natural key prefix from a history key (all bytes except the trailing 8). */
  public static Bytes naturalKeyFromHistoryKey(final Bytes historyKey) {
    if (historyKey.size() < 8) {
      throw new IllegalArgumentException(
          "historyKey too short: expected >= 8 bytes, got " + historyKey.size());
    }
    return historyKey.slice(0, historyKey.size() - 8);
  }
}
