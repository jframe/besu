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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Pure helpers that turn a {@link TrieLog}'s changed keys into the set of trie-node storage keys
 * the migrator's {@code persist()} walk will read, so those reads can be prefetched. Account
 * trie-node keys are the nibble {@code location}; storage trie-node keys are {@code accountHash ‖
 * location} (matching {@code BonsaiTrieNodeStrategy}).
 */
public final class TrieNodePathEnumerator {

  private TrieNodePathEnumerator() {}

  /** Expands each byte of {@code hash} into two nibble bytes (high, low), values 0..15. */
  public static Bytes toNibbles(final Bytes hash) {
    final int size = hash.size();
    final MutableBytes nibbles = MutableBytes.create(size * 2);
    for (int i = 0; i < size; i++) {
      final int b = hash.get(i) & 0xFF;
      nibbles.set(i * 2, (byte) (b >>> 4));
      nibbles.set(i * 2 + 1, (byte) (b & 0x0F));
    }
    return nibbles;
  }

  /**
   * Adds trie-node keys for depths {@code 0..min(maxDepth, nibbles.size())} inclusive. Each key is
   * {@code prepend ‖ nibbles[0..d]} (or {@code nibbles[0..d]} when {@code prepend} is null).
   */
  public static void addLocationPrefixes(
      final Bytes nibbles, final int maxDepth, final Bytes prepend, final Set<Bytes> out) {
    final int limit = Math.min(maxDepth, nibbles.size());
    for (int d = 0; d <= limit; d++) {
      final Bytes location = nibbles.slice(0, d);
      out.add(prepend == null ? location : Bytes.concatenate(prepend, location));
    }
  }

  /**
   * Deduped trie-node prefetch keys for every changed account and storage slot in {@code trieLog}.
   */
  public static List<byte[]> trieNodePrefetchKeys(final TrieLog trieLog, final int maxDepth) {
    final Set<Bytes> keys = new LinkedHashSet<>();
    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) ->
                addLocationPrefixes(
                    toNibbles(address.addressHash().getBytes()), maxDepth, null, keys));
    trieLog
        .getStorageChanges()
        .forEach(
            (address, slotMap) -> {
              final Bytes accountHash = address.addressHash().getBytes();
              slotMap.forEach(
                  (slotKey, change) ->
                      addLocationPrefixes(
                          toNibbles(slotKey.getSlotHash().getBytes()),
                          maxDepth,
                          accountHash,
                          keys));
            });
    final List<byte[]> out = new ArrayList<>(keys.size());
    for (final Bytes k : keys) {
      out.add(k.toArrayUnsafe());
    }
    return out;
  }
}
