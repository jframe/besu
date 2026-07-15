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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.tuweni.bytes.Bytes;

/**
 * Random (domain, naturalKey, block) triples, written into a plain sorted map using the same
 * bytewise ordering RocksDB uses, then checked against a brute-force "latest entry <= T for exactly
 * this node" oracle via a hand-rolled getNearestBeforeMatchLength emulation. This is the executable
 * form of design doc section 3.2's soundness argument.
 */
class HistoryKeyPropertyBasedTest {

  @Provide
  Arbitrary<Bytes> naturalKeys() {
    // Shapes chosen to exercise boundary lengths: empty (trie root), 1 byte, 32 (accountHash-only
    // storage key with empty location), 33 (the historical account/storage collision length), 40.
    return Arbitraries.of(0, 1, 5, 32, 33, 40)
        .flatMap(len -> Arbitraries.bytes().array(byte[].class).ofSize(len).map(Bytes::wrap));
  }

  @Provide
  Arbitrary<Byte> domains() {
    return Arbitraries.of(HistoryKey.DOMAIN_ACCOUNT, HistoryKey.DOMAIN_STORAGE);
  }

  @Property
  void nearestBeforeMatchLengthEmulationMatchesBruteForceOracle(
      @ForAll("domains") final byte queryDomain,
      @ForAll("naturalKeys") final Bytes queryNaturalKey,
      @ForAll("randomEntries") final List<Entry> entries,
      @ForAll("blocks") final long targetBlock) {

    final TreeMap<Bytes, Bytes> sorted =
        new TreeMap<>(Comparator.comparing(Bytes::toArrayUnsafe, Arrays::compareUnsigned));
    for (final Entry e : entries) {
      sorted.put(HistoryKey.encode(e.domain(), e.naturalKey(), e.block()), Bytes.of((byte) 1));
    }
    // Also insert an entry for the query node itself at a few candidate blocks so the "found" case
    // is exercised, not just misses.
    for (final long b : List.of(0L, targetBlock, targetBlock + 1)) {
      sorted.put(HistoryKey.encode(queryDomain, queryNaturalKey, b), Bytes.of((byte) 1));
    }

    final Optional<Bytes> emulated =
        simulateGetNearestBeforeMatchLength(
            sorted, HistoryKey.encode(queryDomain, queryNaturalKey, targetBlock));
    final Optional<Long> emulatedBlock =
        emulated
            .filter(k -> HistoryKey.matchesNode(k, queryDomain, queryNaturalKey))
            .map(HistoryKey::blockOf);

    final Optional<Long> oracle =
        sorted.keySet().stream()
            .filter(k -> HistoryKey.matchesNode(k, queryDomain, queryNaturalKey))
            .map(HistoryKey::blockOf)
            .filter(b -> b <= targetBlock)
            .max(Long::compareTo);

    assertThat(emulatedBlock).isEqualTo(oracle);
  }

  /**
   * Mirrors RocksDBColumnarKeyValueStorage.getNearestBeforeMatchLength: seekForPrev, then walk
   * backward while the candidate key's length differs from the query key's length.
   */
  private static Optional<Bytes> simulateGetNearestBeforeMatchLength(
      final NavigableMap<Bytes, Bytes> sorted, final Bytes queryKey) {
    Bytes candidate = sorted.floorKey(queryKey);
    while (candidate != null && candidate.size() != queryKey.size()) {
      candidate = sorted.lowerKey(candidate);
    }
    return Optional.ofNullable(candidate);
  }

  record Entry(byte domain, Bytes naturalKey, long block) {}

  @Provide
  Arbitrary<List<Entry>> randomEntries() {
    return Arbitraries.of(HistoryKey.DOMAIN_ACCOUNT, HistoryKey.DOMAIN_STORAGE)
        .flatMap(
            domain ->
                Arbitraries.integers()
                    .between(0, 40)
                    .flatMap(
                        len ->
                            Arbitraries.bytes()
                                .array(byte[].class)
                                .ofSize(len)
                                .map(Bytes::wrap)
                                .flatMap(
                                    nk ->
                                        Arbitraries.longs()
                                            .between(0, 1000)
                                            .map(block -> new Entry(domain, nk, block)))))
        .list()
        .ofMinSize(0)
        .ofMaxSize(30);
  }

  @Provide
  Arbitrary<Long> blocks() {
    return Arbitraries.longs().between(0, 1000);
  }
}
