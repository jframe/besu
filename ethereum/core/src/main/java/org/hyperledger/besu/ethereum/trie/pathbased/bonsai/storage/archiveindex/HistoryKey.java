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
import org.apache.tuweni.bytes.Bytes32;

/**
 * Fixed-width key codec for {@code TRIE_NODE_HISTORY_ARCHIVE_V2}: {@code domain(1B) || keyLen(1B)
 * || naturalKey(keyLen bytes) || block(8B BE)}.
 *
 * <p>Within one (domain, keyLen) group every key has identical total width {@code 10 + keyLen}, so
 * {@link
 * org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage#getNearestBeforeMatchLength}
 * can never return a key belonging to a different natural key of the same length: two fixed-width
 * byte strings that share a common prefix P bound every fixed-width string lexicographically
 * between them to also start with P. Cross-group interleaving cannot happen either, because the
 * {@code keyLen} byte (position 1) orders before any {@code naturalKey} byte. {@link #matchesNode}
 * is still required as a defensive check for the "no entry exists" case, where the nearest-before
 * hit belongs to a different node entirely.
 */
public final class HistoryKey {

  public static final byte DOMAIN_ACCOUNT = 0x00;
  public static final byte DOMAIN_STORAGE = 0x01;

  private static final int BLOCK_BYTES = 8;

  private HistoryKey() {}

  public static Bytes accountNaturalKey(final Bytes location) {
    return location;
  }

  public static Bytes storageNaturalKey(final Bytes32 accountHash, final Bytes location) {
    return Bytes.concatenate(accountHash, location);
  }

  public static Bytes encode(final byte domain, final Bytes naturalKey, final long block) {
    if (block < 0) {
      throw new IllegalArgumentException("block must be >= 0, got " + block);
    }
    return Bytes.concatenate(prefix(domain, naturalKey), Bytes.ofUnsignedLong(block));
  }

  public static Bytes prefix(final byte domain, final Bytes naturalKey) {
    if (naturalKey.size() > 0xFF) {
      throw new IllegalArgumentException("naturalKey too long: " + naturalKey.size());
    }
    return Bytes.concatenate(Bytes.of(domain), Bytes.of((byte) naturalKey.size()), naturalKey);
  }

  public static long blockOf(final Bytes key) {
    return key.slice(key.size() - BLOCK_BYTES, BLOCK_BYTES).toLong();
  }

  public static byte domainOf(final Bytes key) {
    return key.get(0);
  }

  public static int keyLenOf(final Bytes key) {
    return key.get(1) & 0xFF;
  }

  public static Bytes naturalKeyOf(final Bytes key) {
    return key.slice(2, keyLenOf(key));
  }

  /** True iff {@code key} is a history entry for exactly the node {@code (domain, naturalKey)}. */
  public static boolean matchesNode(final Bytes key, final byte domain, final Bytes naturalKey) {
    final int expectedTotalLength = 2 + naturalKey.size() + BLOCK_BYTES;
    if (key.size() != expectedTotalLength) {
      return false;
    }
    return key.get(0) == domain
        && (key.get(1) & 0xFF) == naturalKey.size()
        && key.slice(2, naturalKey.size()).equals(naturalKey);
  }
}
