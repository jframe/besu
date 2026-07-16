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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * {@link StorageTrieLeafCountProvider} backed by the live Bonsai flat database. Counts a contract's
 * occupied storage slots by range-scanning {@code ACCOUNT_STORAGE_STORAGE} (keyed {@code
 * accountHash ‖ slotHash}) over the account-hash prefix, capping the scan and caching the result
 * per account so each contract is probed at most once across the whole (multi-threaded) estimate
 * scan.
 *
 * <p>The count reflects <em>head</em> state, not the state at each historical block; a contract's
 * slot set grows over its lifetime, so this is an upper-bound-ish proxy for early blocks and can
 * miss slots for accounts self-destructed before head. That is an accepted approximation: the count
 * feeds only a logarithmic depth model, and it is a vast improvement over the previous behaviour of
 * pricing every storage slot against the global account-trie leaf count.
 *
 * <p>The cap bounds worst-case scan cost for a handful of very large contracts. Beyond the cap the
 * storage trie's expected depth barely grows (depth ≈ log16(N)) and the extra nodes are cheap DIFF
 * entries, so capping has negligible effect on the size estimate.
 */
public final class FlatDbStorageLeafCountProvider implements StorageTrieLeafCountProvider {

  private final SegmentedKeyValueStorage storage;
  private final int cap;
  private final Map<Bytes, Long> cache = new ConcurrentHashMap<>();

  /**
   * @param storage a storage handle that includes the {@code ACCOUNT_STORAGE_STORAGE} segment
   * @param cap maximum number of slots to count per contract (scan stops once reached)
   */
  public FlatDbStorageLeafCountProvider(final SegmentedKeyValueStorage storage, final int cap) {
    this.storage = storage;
    this.cap = cap;
  }

  @Override
  public long leafCount(final Bytes accountHash) {
    return cache.computeIfAbsent(accountHash, this::countSlots);
  }

  private long countSlots(final Bytes accountHash) {
    // Full-length (64-byte) start key accountHash‖0…0, matching BonsaiFlatDbStrategy's storage
    // stream. A bare 32-byte prefix would be length-normalised by some Bytes.compareTo-based range
    // filters and wrongly admit other accounts' entries before takeWhile can reject them.
    final byte[] startKey = Bytes.concatenate(accountHash, Bytes32.ZERO).toArrayUnsafe();
    try (Stream<Pair<byte[], byte[]>> slots =
        storage.streamFromKey(ACCOUNT_STORAGE_STORAGE, startKey)) {
      return slots
          .takeWhile(pair -> Bytes.wrap(pair.getKey()).slice(0, Bytes32.SIZE).equals(accountHash))
          .limit(cap)
          .count();
    }
  }
}
