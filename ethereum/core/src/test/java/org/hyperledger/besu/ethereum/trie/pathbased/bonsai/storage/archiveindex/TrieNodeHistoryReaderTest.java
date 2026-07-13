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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TrieNodeHistoryReader#nodeAt(Bytes, long)}.
 *
 * <p>Each test seeds both the {@link TrieNodeHistoryStore} (codec entries) and the {@link
 * TrieNodeChangeIndex} (block-change list) before querying the reader. Writes use separate
 * committed transactions because the bloom same-tx hazard in {@link TrieNodeChangeIndex} would drop
 * bits when multiple nodes share one transaction.
 */
class TrieNodeHistoryReaderTest {

  // A plausible account-trie natural key (compact nibble path)
  private static final Bytes KEY = Bytes.fromHexString("0xdeadbeef");

  // A second key used for multi-key isolation tests
  private static final Bytes OTHER_KEY = Bytes.fromHexString("0xcafebabe");

  private SegmentedInMemoryKeyValueStorage kv;
  private TrieNodeHistoryStore store;
  private TrieNodeChangeIndex index;
  private TrieNodeHistoryReader reader;

  @BeforeEach
  void setUp() {
    kv = new SegmentedInMemoryKeyValueStorage();
    store = new TrieNodeHistoryStore(kv);
    // Use a small rangeSize so all test blocks fit in one range without multi-range walks.
    index = new TrieNodeChangeIndex(kv, 1_000_000);
    reader = new TrieNodeHistoryReader(store, index);
  }

  // -------------------------------------------------------------------------
  // Helper: build a branch-node RLP where child slot [slotIndex] carries a
  // 32-byte "hash" derived from [markerByte], all other slots empty.
  // -------------------------------------------------------------------------

  /** Build a branch-node RLP with a single occupied child slot. */
  private static Bytes branchWith(final int slotIndex, final int markerByte) {
    return RLP.encode(
        out -> {
          out.startList();
          for (int i = 0; i < 16; i++) {
            if (i == slotIndex) {
              // 32-byte child hash: left-pad marker byte
              out.writeBytes(Bytes32.leftPad(Bytes.of(markerByte)));
            } else {
              out.writeNull(); // empty slot
            }
          }
          out.writeNull(); // branch terminal value: empty
          out.endList();
        });
  }

  /** Write a single (key, block, entry) pair to the store and commit. */
  private void putEntry(final Bytes naturalKey, final long block, final Bytes entry) {
    var tx = kv.startTransaction();
    store.put(tx, naturalKey, block, entry);
    tx.commit();
  }

  /** Append a single (key, block) change record to the index and commit. */
  private void appendIndex(final Bytes naturalKey, final long block) {
    var tx = kv.startTransaction();
    index.append(tx, naturalKey, block);
    tx.commit();
  }

  // =========================================================================
  // Plan-required test (verbatim from task spec)
  // =========================================================================

  /**
   * Seed FULL@100, DIFF@101, DIFF@102, DIFF@103. Verify:
   *
   * <ul>
   *   <li>{@code nodeAt(key, 103)} reconstructs v103 (keccak matches)
   *   <li>{@code nodeAt(key, 102)} returns v102 exactly
   * </ul>
   */
  @Test
  void reconstructsVersionFromCheckpointPlusDiffs() {
    // Build four branch-node versions, each changing a different child slot.
    Bytes v100 = branchWith(3, 100); // child3 = hash(100)
    Bytes v101 = branchWith(5, 101); // child5 = hash(101)  [child3 gone, child5 added]
    Bytes v102 = branchWith(7, 102); // child7 = hash(102)  [child5 gone, child7 added]
    Bytes v103 = branchWith(9, 103); // child9 = hash(103)  [child7 gone, child9 added]

    // Seed store: FULL@100, DIFF@101, DIFF@102, DIFF@103
    putEntry(KEY, 100, TrieNodeDiffCodec.encodeFull(v100));
    putEntry(KEY, 101, TrieNodeDiffCodec.encodeDiff(v100, v101));
    putEntry(KEY, 102, TrieNodeDiffCodec.encodeDiff(v101, v102));
    putEntry(KEY, 103, TrieNodeDiffCodec.encodeDiff(v102, v103));

    // Seed index
    appendIndex(KEY, 100);
    appendIndex(KEY, 101);
    appendIndex(KEY, 102);
    appendIndex(KEY, 103);

    // nodeAt(103): walk back 103→102→101→100(FULL), apply [DIFF@101, DIFF@102, DIFF@103] → v103
    assertThat(reader.nodeAt(KEY, 103))
        .hasValueSatisfying(b -> assertThat(Hash.hash(b)).isEqualTo(Hash.hash(v103)));

    // nodeAt(102): walk back 102→101→100(FULL), apply [DIFF@101, DIFF@102] → v102
    assertThat(reader.nodeAt(KEY, 102)).hasValueSatisfying(b -> assertThat(b).isEqualTo(v102));
  }

  // =========================================================================
  // Additional tests
  // =========================================================================

  /**
   * When no entry has ever been written for a key before the target block, {@code nodeAt} returns
   * empty.
   */
  @Test
  void nodeAtForBlockWithNoPriorChangeReturnsEmpty() {
    // Nothing written for KEY at all.
    assertThat(reader.nodeAt(KEY, 50)).isEmpty();
  }

  /**
   * When the latest change at or before targetBlock is a deletion tombstone, {@code nodeAt} returns
   * empty (the node was deleted).
   */
  @Test
  void nodeAtAfterDeletionTombstoneReturnsEmpty() {
    Bytes v100 = branchWith(3, 100);

    // FULL at 100
    putEntry(KEY, 100, TrieNodeDiffCodec.encodeFull(v100));
    appendIndex(KEY, 100);

    // Deletion tombstone at 101
    putEntry(KEY, 101, TrieNodeDiffCodec.encodeDiff(v100, null)); // DELETION tombstone
    appendIndex(KEY, 101);

    // Query at block 105 — latest change is the tombstone at 101 → return empty
    assertThat(reader.nodeAt(KEY, 105)).isEmpty();

    // Query at exactly 101 — same tombstone
    assertThat(reader.nodeAt(KEY, 101)).isEmpty();
  }

  /**
   * When the target block is exactly the FULL checkpoint block, no diffs need to be applied; the
   * FULL node is returned directly.
   */
  @Test
  void nodeAtExactlyAtFullCheckpointReturnsFull() {
    Bytes v100 = branchWith(3, 100);
    putEntry(KEY, 100, TrieNodeDiffCodec.encodeFull(v100));
    appendIndex(KEY, 100);

    assertThat(reader.nodeAt(KEY, 100)).hasValue(v100);
  }

  /**
   * When the target block is between the FULL checkpoint and the next recorded change, the FULL
   * version is returned (no diffs exist yet for this key after the checkpoint).
   */
  @Test
  void nodeAtBetweenCheckpointAndNextChangeReturnsFull() {
    Bytes v100 = branchWith(3, 100);
    putEntry(KEY, 100, TrieNodeDiffCodec.encodeFull(v100));
    appendIndex(KEY, 100);

    // No entry at 150; latest change <= 150 is 100 (FULL) → return v100 directly
    assertThat(reader.nodeAt(KEY, 150)).hasValue(v100);
  }

  /** Querying one key does not affect the result for a different key. */
  @Test
  void multipleKeysAreIsolated() {
    Bytes vA = branchWith(1, 10);
    Bytes vB = branchWith(2, 20);

    putEntry(KEY, 100, TrieNodeDiffCodec.encodeFull(vA));
    appendIndex(KEY, 100);

    putEntry(OTHER_KEY, 200, TrieNodeDiffCodec.encodeFull(vB));
    appendIndex(OTHER_KEY, 200);

    assertThat(reader.nodeAt(KEY, 100)).hasValue(vA);
    assertThat(reader.nodeAt(OTHER_KEY, 200)).hasValue(vB);

    // KEY has no entry at block 200 except its FULL@100
    assertThat(reader.nodeAt(KEY, 200)).hasValue(vA);

    // OTHER_KEY has no entry before block 200
    assertThat(reader.nodeAt(OTHER_KEY, 100)).isEmpty();
  }

  /**
   * A tombstone mid-chain (FULL@100, DIFF@101, TOMBSTONE@102, query@102 and query@103) must not
   * crash reconstruct() — it should return empty for both queries without throwing.
   *
   * <p>The b* for both query@102 and query@103 is 102 (the tombstone), which is caught by the
   * top-level tombstone check (Step 3) and returns empty immediately — the backward walk is never
   * reached. This test also validates that a tombstone encountered during the backward walk (if it
   * were ever reached via query@101 walking back through a hypothetical DIFF@101) returns empty
   * without passing the tombstone to {@link TrieNodeDiffCodec#reconstruct}.
   *
   * <p>To exercise the backward-walk tombstone path: seed FULL@100, DIFF@101, TOMBSTONE@102,
   * DIFF@103 (pretending a re-creation happened without the FULL|CREATION entry — a corrupt but
   * possible state). Query at 103: b*=103 (DIFF), walk back to 102 (TOMBSTONE) — must return empty
   * without crashing.
   */
  @Test
  void tombstoneMidChainReturnsEmptyWithoutCrash() {
    Bytes v100 = branchWith(3, 100);
    Bytes v101 = branchWith(5, 101);
    Bytes v103 = branchWith(9, 103);

    // FULL@100, DIFF@101, TOMBSTONE@102
    putEntry(KEY, 100, TrieNodeDiffCodec.encodeFull(v100));
    appendIndex(KEY, 100);
    putEntry(KEY, 101, TrieNodeDiffCodec.encodeDiff(v100, v101));
    appendIndex(KEY, 101);
    putEntry(KEY, 102, TrieNodeDiffCodec.encodeDiff(v101, null)); // DELETION tombstone
    appendIndex(KEY, 102);

    // nodeAt(102) — b* is 102, top-level tombstone check → empty (no crash)
    assertThat(reader.nodeAt(KEY, 102)).isEmpty();

    // nodeAt(105) — b* is still 102 (tombstone), same result
    assertThat(reader.nodeAt(KEY, 105)).isEmpty();

    // Now add a DIFF@103 after the tombstone (corrupt/incomplete re-creation — no FULL|CREATION).
    // Query at 103: b*=103 (DIFF), backward walk hits tombstone@102 → must return empty, not throw.
    putEntry(KEY, 103, TrieNodeDiffCodec.encodeDiff(v100, v103)); // pretend re-creation as DIFF
    appendIndex(KEY, 103);

    assertThat(reader.nodeAt(KEY, 103)).isEmpty();
  }

  /**
   * Seeds FULL@10 and 16 consecutive DIFFs at blocks 11–26 (each changing one child slot), then
   * asserts {@code nodeAt(key, 26)} equals the expected v26 node. This exercises the full
   * steady-state backward walk depth of 16 steps.
   */
  @Test
  void sixteenStepBackwardWalkReconstructsCorrectly() {
    // Build 17 versions: v10 is the FULL base; v11–v26 each change slot (i % 16).
    final int VERSIONS = 17; // v10..v26
    final Bytes[] versions = new Bytes[VERSIONS];
    versions[0] = branchWith(0, 10); // v10: child0 = hash(10)
    for (int i = 1; i < VERSIONS; i++) {
      // Each version changes a different child slot (wrapping through 0–15).
      versions[i] = branchWith(i % 16, 10 + i);
    }

    // Seed store: FULL at block 10, DIFFs at blocks 11–26
    putEntry(KEY, 10, TrieNodeDiffCodec.encodeFull(versions[0]));
    appendIndex(KEY, 10);
    for (int i = 1; i < VERSIONS; i++) {
      putEntry(KEY, 10 + i, TrieNodeDiffCodec.encodeDiff(versions[i - 1], versions[i]));
      appendIndex(KEY, 10 + i);
    }

    // nodeAt(26): walk back 16 steps to FULL@10, apply 16 DIFFs → v26
    final Bytes expected = versions[VERSIONS - 1]; // v26
    assertThat(reader.nodeAt(KEY, 26))
        .hasValueSatisfying(
            b -> {
              assertThat(Hash.hash(b)).isEqualTo(Hash.hash(expected));
              assertThat(b).isEqualTo(expected);
            });

    // Spot-check an intermediate version to confirm partial reconstruction
    final Bytes expected11 = versions[1]; // v11
    assertThat(reader.nodeAt(KEY, 11)).hasValue(expected11);
  }

  /**
   * When the FULL checkpoint is NOT at the position the reader's global-mutation formula computes
   * (which is the common case on migrated data), reconstruction must still locate the nearest FULL
   * via batched reads over the change-block list — <em>not</em> degrade into a per-step sequential
   * backward walk that issues one index read per step.
   *
   * <p>Seeds FULL@100 followed by 20 DIFFs (101–120). The old {@code globalMut - globalMut%16}
   * checkpoint formula lands on list index 16 (block 116, a DIFF), so the old code fell back to a
   * 20-step backward walk — one {@code latestChangeBlock} index read per step. The scan-based
   * reconstruction reads the trailing change-block window in a single batch and finds FULL@100 with
   * no per-step index walk.
   */
  @Test
  void reconstructionAvoidsSequentialWalkWhenCheckpointNotAtFormulaPosition() {
    final SegmentedKeyValueStorage spyKv = spy(new SegmentedInMemoryKeyValueStorage());
    final TrieNodeHistoryStore spyStore = new TrieNodeHistoryStore(spyKv);
    final TrieNodeChangeIndex spyIndex = new TrieNodeChangeIndex(spyKv, 1_000_000);
    final TrieNodeHistoryReader spyReader = new TrieNodeHistoryReader(spyStore, spyIndex);

    final int n = 20;
    final Bytes[] v = new Bytes[n + 1];
    v[0] = branchWith(0, 100);
    commitPut(spyKv, spyStore, KEY, 100, TrieNodeDiffCodec.encodeFull(v[0]));
    commitAppend(spyKv, spyIndex, KEY, 100);
    for (int i = 1; i <= n; i++) {
      v[i] = branchWith(i % 16, 100 + i);
      commitPut(spyKv, spyStore, KEY, 100 + i, TrieNodeDiffCodec.encodeDiff(v[i - 1], v[i]));
      commitAppend(spyKv, spyIndex, KEY, 100 + i);
    }

    clearInvocations(spyKv); // ignore all setup reads/writes

    // Correctness: reconstructs v120 from FULL@100 + 20 diffs.
    assertThat(spyReader.nodeAt(KEY, 120)).hasValueSatisfying(b -> assertThat(b).isEqualTo(v[n]));

    // Behaviour: the index CF is read only a small bounded number of times (the initial
    // latest-change lookup + the change-list read), never once-per-diff. The old backward walk
    // issued ~20 index reads here.
    verify(spyKv, atMost(6))
        .get(eq(KeyValueSegmentIdentifier.TRIE_NODE_INDEX_ARCHIVE), any(byte[].class));
  }

  private static void commitPut(
      final SegmentedKeyValueStorage kv,
      final TrieNodeHistoryStore store,
      final Bytes key,
      final long block,
      final Bytes entry) {
    var tx = kv.startTransaction();
    store.put(tx, key, block, entry);
    tx.commit();
  }

  private static void commitAppend(
      final SegmentedKeyValueStorage kv,
      final TrieNodeChangeIndex index,
      final Bytes key,
      final long block) {
    var tx = kv.startTransaction();
    index.append(tx, key, block);
    tx.commit();
  }

  /**
   * A single DIFF with no prior FULL should not exist in well-formed data, but if the FULL is
   * exactly at the same block as the change, the FULL is returned directly.
   */
  @Test
  void singleDiffAfterFull() {
    Bytes v100 = branchWith(3, 100);
    Bytes v101 = branchWith(3, 101);

    putEntry(KEY, 100, TrieNodeDiffCodec.encodeFull(v100));
    appendIndex(KEY, 100);

    putEntry(KEY, 101, TrieNodeDiffCodec.encodeDiff(v100, v101));
    appendIndex(KEY, 101);

    assertThat(reader.nodeAt(KEY, 101)).hasValue(v101);
    assertThat(reader.nodeAt(KEY, 100)).hasValue(v100);
  }

  /**
   * Hot node changed every block for 50 blocks (blocks 0–49). With {@code CHECKPOINT_INTERVAL = 16}
   * this produces:
   *
   * <ul>
   *   <li>FULL at mutations 0, 16, 32, 48 (blocks 0, 16, 32, 48)
   *   <li>DIFFs at all other blocks
   * </ul>
   *
   * Verifies that {@code nodeAt} returns the correct result for:
   *
   * <ul>
   *   <li>The last block (49) — nearest FULL at block 48, one DIFF to apply
   *   <li>A mid-range block (35) — nearest FULL at block 32, 3 DIFFs to apply
   *   <li>A FULL checkpoint block (16) — returned directly with no reconstruction
   *   <li>The first block (0) — FULL, returned directly
   * </ul>
   *
   * <p>This exercises the optimised single-index-list-read path in {@link
   * TrieNodeHistoryReader#nodeAt} rather than the old backward-walk loop.
   */
  @Test
  void hotNodeChanged50TimesReconstructsCorrectlyForMidRangeBlock() {
    final int TOTAL_BLOCKS = 50;
    final int CHECKPOINT_INTERVAL = TrieNodeHistoryReader.CHECKPOINT_INTERVAL; // 16

    // Build TOTAL_BLOCKS versions; v[i] has child slot (i % 16) filled with hash(i).
    final Bytes[] versions = new Bytes[TOTAL_BLOCKS];
    for (int i = 0; i < TOTAL_BLOCKS; i++) {
      versions[i] = branchWith(i % 16, i);
    }

    // Determine which blocks get FULL entries (mutations 0, 16, 32, 48 in a 0-based scheme).
    // Mutation 0 is block 0; mutation count starts at 0.
    // The write path stores FULL when (mutationCount % CHECKPOINT_INTERVAL == 0).
    // For a node starting at block 0 with changes every block: mutation i → block i.
    // FULLs: blocks 0, 16, 32, 48.
    for (int block = 0; block < TOTAL_BLOCKS; block++) {
      final boolean isFull = (block % CHECKPOINT_INTERVAL == 0);
      final Bytes entry;
      if (isFull) {
        entry = TrieNodeDiffCodec.encodeFull(versions[block]);
      } else {
        entry = TrieNodeDiffCodec.encodeDiff(versions[block - 1], versions[block]);
      }
      putEntry(KEY, block, entry);
      appendIndex(KEY, block);
    }

    // 1. nodeAt(49): FULL@48, DIFF@49 → v49
    assertThat(reader.nodeAt(KEY, 49))
        .hasValueSatisfying(b -> assertThat(b).isEqualTo(versions[49]));

    // 2. nodeAt(35): FULL@32, DIFF@33, DIFF@34, DIFF@35 → v35
    assertThat(reader.nodeAt(KEY, 35))
        .hasValueSatisfying(b -> assertThat(b).isEqualTo(versions[35]));

    // 3. nodeAt(16): exact FULL checkpoint — returned directly, no reconstruction
    assertThat(reader.nodeAt(KEY, 16)).hasValue(versions[16]);

    // 4. nodeAt(0): first block, FULL — returned directly
    assertThat(reader.nodeAt(KEY, 0)).hasValue(versions[0]);

    // 5. nodeAt(47): FULL@32, DIFFs@33–47 → v47 (15 DIFFs, max steady-state walk depth)
    assertThat(reader.nodeAt(KEY, 47))
        .hasValueSatisfying(b -> assertThat(b).isEqualTo(versions[47]));

    // 6. nodeAt(50) beyond the recorded range — latest change is block 49 (DIFF), reconstruct v49
    assertThat(reader.nodeAt(KEY, 50))
        .hasValueSatisfying(b -> assertThat(b).isEqualTo(versions[49]));
  }
}
