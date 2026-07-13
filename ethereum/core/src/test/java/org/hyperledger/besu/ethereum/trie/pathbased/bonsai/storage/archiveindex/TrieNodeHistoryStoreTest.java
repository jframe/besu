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

import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrieNodeHistoryStoreTest {

  // A plausible account-trie naturalKey (compact nibble path, variable length)
  private static final Bytes ACCOUNT_KEY = Bytes.fromHexString("0xdeadbeef");

  // A storage-trie naturalKey: 32-byte accountHash ‖ location
  private static final Bytes STORAGE_KEY =
      Bytes.concatenate(
          Bytes.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
          Bytes.fromHexString("0xbb"));

  // A different natural key for isolation checks
  private static final Bytes OTHER_KEY =
      Bytes.fromHexString("0x1111111111111111111111111111111111111111111111111111111111111111");

  // Minimal node RLP for encoding
  private static final Bytes NODE_RLP = Bytes.fromHexString("0xc0"); // empty RLP list

  private SegmentedInMemoryKeyValueStorage kv;
  private TrieNodeHistoryStore store;

  @BeforeEach
  void setUp() {
    kv = new SegmentedInMemoryKeyValueStorage();
    store = new TrieNodeHistoryStore(kv);
  }

  // -------------------------------------------------------------------------
  // Plan-required test (verbatim from task spec)
  // -------------------------------------------------------------------------

  @Test
  void putGetDelete() {
    var tx = kv.startTransaction();
    store.put(tx, ACCOUNT_KEY, 100L, TrieNodeDiffCodec.encodeFull(NODE_RLP));
    tx.commit();
    assertThat(store.get(ACCOUNT_KEY, 100L))
        .hasValueSatisfying(e -> assertThat(TrieNodeDiffCodec.decode(e).isFull()).isTrue());
    var tx2 = kv.startTransaction();
    store.delete(tx2, ACCOUNT_KEY, 100L);
    tx2.commit();
    assertThat(store.get(ACCOUNT_KEY, 100L)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Two different blocks for the same key — no cross-contamination
  // -------------------------------------------------------------------------

  @Test
  void twoBlocksSameKeyNoContamination() {
    Bytes entry100 = TrieNodeDiffCodec.encodeFull(NODE_RLP);
    Bytes entry200 = TrieNodeDiffCodec.encodeFull(Bytes.fromHexString("0xc1"));

    var tx = kv.startTransaction();
    store.put(tx, ACCOUNT_KEY, 100L, entry100);
    store.put(tx, ACCOUNT_KEY, 200L, entry200);
    tx.commit();

    assertThat(store.get(ACCOUNT_KEY, 100L)).hasValue(entry100);
    assertThat(store.get(ACCOUNT_KEY, 200L)).hasValue(entry200);
  }

  // -------------------------------------------------------------------------
  // Different natural keys do not see each other's entries
  // -------------------------------------------------------------------------

  @Test
  void differentNaturalKeysAreIsolated() {
    Bytes entryA = TrieNodeDiffCodec.encodeFull(NODE_RLP);
    Bytes entryB = TrieNodeDiffCodec.encodeFull(Bytes.fromHexString("0xc2"));

    var tx = kv.startTransaction();
    store.put(tx, ACCOUNT_KEY, 50L, entryA);
    store.put(tx, OTHER_KEY, 50L, entryB);
    tx.commit();

    assertThat(store.get(ACCOUNT_KEY, 50L)).hasValue(entryA);
    assertThat(store.get(OTHER_KEY, 50L)).hasValue(entryB);
  }

  // -------------------------------------------------------------------------
  // Storage-trie naturalKey (32 + N bytes) works correctly
  // -------------------------------------------------------------------------

  @Test
  void storageTrieNaturalKeyWorks() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(NODE_RLP);
    var tx = kv.startTransaction();
    store.put(tx, STORAGE_KEY, 42L, entry);
    tx.commit();

    assertThat(store.get(STORAGE_KEY, 42L)).hasValue(entry);
    // Account key at same block should be empty
    assertThat(store.get(ACCOUNT_KEY, 42L)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Delete a non-existent entry is a no-op (no exception)
  // -------------------------------------------------------------------------

  @Test
  void deleteNonExistentIsNoOp() {
    var tx = kv.startTransaction();
    store.delete(tx, ACCOUNT_KEY, 999L); // nothing stored, must not throw
    tx.commit();

    assertThat(store.get(ACCOUNT_KEY, 999L)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // get on empty store returns empty
  // -------------------------------------------------------------------------

  @Test
  void getOnEmptyStoreReturnsEmpty() {
    assertThat(store.get(ACCOUNT_KEY, 1L)).isEmpty();
    assertThat(store.get(OTHER_KEY, 0L)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Block 0 is a valid key (no off-by-one at zero)
  // -------------------------------------------------------------------------

  @Test
  void blockZeroIsValidKey() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(NODE_RLP);
    var tx = kv.startTransaction();
    store.put(tx, ACCOUNT_KEY, 0L, entry);
    tx.commit();

    assertThat(store.get(ACCOUNT_KEY, 0L)).hasValue(entry);
  }

  // -------------------------------------------------------------------------
  // Deletion removes only the targeted (key, block) — sibling entry is preserved
  // -------------------------------------------------------------------------

  @Test
  void deleteRemovesOnlyTargetedBlock() {
    Bytes entry100 = TrieNodeDiffCodec.encodeFull(NODE_RLP);
    Bytes entry200 = TrieNodeDiffCodec.encodeFull(Bytes.fromHexString("0xc3"));

    var tx = kv.startTransaction();
    store.put(tx, ACCOUNT_KEY, 100L, entry100);
    store.put(tx, ACCOUNT_KEY, 200L, entry200);
    tx.commit();

    var tx2 = kv.startTransaction();
    store.delete(tx2, ACCOUNT_KEY, 100L);
    tx2.commit();

    assertThat(store.get(ACCOUNT_KEY, 100L)).isEmpty();
    assertThat(store.get(ACCOUNT_KEY, 200L)).hasValue(entry200);
  }

  // -------------------------------------------------------------------------
  // Verify tombstone (DELETION codec entry) round-trips correctly
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  // getAll — batched multi-block read, same order as requested, gaps -> empty
  // -------------------------------------------------------------------------

  @Test
  void getAllReturnsEntriesInRequestedOrderWithGaps() {
    Bytes entry100 = TrieNodeDiffCodec.encodeFull(NODE_RLP);
    Bytes entry300 = TrieNodeDiffCodec.encodeFull(Bytes.fromHexString("0xc4"));

    var tx = kv.startTransaction();
    store.put(tx, ACCOUNT_KEY, 100L, entry100);
    store.put(tx, ACCOUNT_KEY, 300L, entry300);
    tx.commit();

    // Request 100 (present), 200 (missing), 300 (present) — result must align by index.
    List<Optional<Bytes>> results = store.getAll(ACCOUNT_KEY, new long[] {100L, 200L, 300L});

    assertThat(results).hasSize(3);
    assertThat(results.get(0)).hasValue(entry100);
    assertThat(results.get(1)).isEmpty();
    assertThat(results.get(2)).hasValue(entry300);
  }

  @Test
  void getAllOnEmptyKeyListReturnsEmptyList() {
    assertThat(store.getAll(ACCOUNT_KEY, new long[] {})).isEmpty();
  }

  @Test
  void getAllIsolatesByNaturalKey() {
    Bytes entryA = TrieNodeDiffCodec.encodeFull(NODE_RLP);
    var tx = kv.startTransaction();
    store.put(tx, ACCOUNT_KEY, 5L, entryA);
    tx.commit();

    // Same block number under a different natural key must not be returned.
    assertThat(store.getAll(OTHER_KEY, new long[] {5L})).containsExactly(Optional.empty());
    assertThat(store.getAll(ACCOUNT_KEY, new long[] {5L})).containsExactly(Optional.of(entryA));
  }

  // -------------------------------------------------------------------------
  // Verify tombstone (DELETION codec entry) round-trips correctly
  // -------------------------------------------------------------------------

  @Test
  void tombstoneEntryRoundTrips() {
    // A deletion tombstone is a codec entry (metadata-only DELETION byte), not a storage delete.
    Bytes tombstone = TrieNodeDiffCodec.encodeDiff(NODE_RLP, null); // produces DELETION tombstone
    var tx = kv.startTransaction();
    store.put(tx, ACCOUNT_KEY, 77L, tombstone);
    tx.commit();

    assertThat(store.get(ACCOUNT_KEY, 77L))
        .hasValueSatisfying(e -> assertThat(TrieNodeDiffCodec.decode(e).isDeletion()).isTrue());
  }
}
