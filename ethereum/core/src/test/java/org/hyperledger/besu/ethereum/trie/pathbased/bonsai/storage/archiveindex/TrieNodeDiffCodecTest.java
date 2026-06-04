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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.rlp.RLP;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class TrieNodeDiffCodecTest {

  // A realistic branch-node RLP (17-item list with one child hash + mostly empty slots)
  private static final Bytes BRANCH_NODE_RLP =
      Bytes.fromHexString(
          "0xf85180a0000000000000000000000000000000000000000000000000000000000000000180808080808080808080808080808080");

  // A minimal non-empty bytes for quick round-trip checks
  private static final Bytes SIMPLE_NODE_RLP = Bytes.fromHexString("0xdeadbeef");

  // -------------------------------------------------------------------------
  // Helper: build a branch-node RLP from 16 child refs (as raw Bytes — the decoded value,
  // NOT the RLP encoding) and a branch value.
  //
  // Each child slot: if childRefs[i] is null or Bytes.EMPTY => write RLP null (0x80).
  //                  otherwise write as a 32-byte RLP byte string (0xa0 + 32 bytes).
  // Branch value: if value is null or Bytes.EMPTY => write RLP null (0x80).
  //               otherwise write as RLP byte string.
  //
  // This mirrors the encoding used by BranchNode.getEncodedBytes() and the decoding used by
  // parseBranchChildren() in TrieNodeDiffCodec (which calls readAsRlp().raw() per child).
  // -------------------------------------------------------------------------
  private static Bytes branchWith(final Bytes[] childRefs, final Bytes value) {
    return RLP.encode(
        out -> {
          out.startList();
          for (int i = 0; i < 16; i++) {
            Bytes ref = (childRefs != null && i < childRefs.length) ? childRefs[i] : null;
            if (ref == null || ref.isEmpty()) {
              out.writeNull(); // empty slot: RLP null = 0x80
            } else {
              out.writeBytes(ref); // hash ref: 0xa0 + 32 bytes
            }
          }
          if (value == null || value.isEmpty()) {
            out.writeNull();
          } else {
            out.writeBytes(value);
          }
          out.endList();
        });
  }

  /** Convenience overload: no branch value. */
  private static Bytes branchWith(final Bytes[] childRefs) {
    return branchWith(childRefs, Bytes.EMPTY);
  }

  /** Build a 32-byte hash-like value for child slot {@code n}. */
  private static Bytes32 childHash(final int n) {
    return Bytes32.leftPad(Bytes.of(n));
  }

  // -------------------------------------------------------------------------
  // FULL entry round-trip (the primary goal of Task 1.1)
  // -------------------------------------------------------------------------

  @Test
  void fullEntryRoundTrips() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);
    assertThat(d.isFull()).isTrue();
    assertThat(d.fullNode()).isEqualTo(BRANCH_NODE_RLP);
  }

  @Test
  void fullEntryRoundTripsSimpleBytes() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(SIMPLE_NODE_RLP);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);
    assertThat(d.isFull()).isTrue();
    assertThat(d.fullNode()).isEqualTo(SIMPLE_NODE_RLP);
  }

  // -------------------------------------------------------------------------
  // Metadata byte: ENTRY_FULL sets bit0 (0x01)
  // -------------------------------------------------------------------------

  @Test
  void fullEntryHasMetadataBit0Set() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP);
    // First byte must have ENTRY_FULL (0x01) set
    assertThat(entry.get(0) & TrieNodeDiffCodec.ENTRY_FULL).isEqualTo(TrieNodeDiffCodec.ENTRY_FULL);
  }

  @Test
  void fullEntryLengthIsOneMoreThanNodeRlp() {
    Bytes entry = TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP);
    assertThat(entry.size()).isEqualTo(BRANCH_NODE_RLP.size() + 1);
  }

  // -------------------------------------------------------------------------
  // Decoded predicates
  // -------------------------------------------------------------------------

  @Test
  void fullDecodedIsNotDeletion() {
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP));
    assertThat(d.isDeletion()).isFalse();
  }

  @Test
  void fullDecodedIsNotCreation() {
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP));
    assertThat(d.isCreation()).isFalse();
  }

  // -------------------------------------------------------------------------
  // fullNode() throws on non-full entries (guard for later diff entries)
  // -------------------------------------------------------------------------

  @Test
  void fullNodeThrowsWhenCalledOnDiffEntry() {
    // Manually craft a diff entry: metadata byte with ENTRY_FULL bit CLEAR (0x00) + some body
    Bytes diffEntry = Bytes.concatenate(Bytes.of(0x00), SIMPLE_NODE_RLP);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(diffEntry);
    assertThat(d.isFull()).isFalse();
    assertThatThrownBy(d::fullNode).isInstanceOf(IllegalStateException.class);
  }

  // -------------------------------------------------------------------------
  // Metadata constants are defined with correct bit positions
  // -------------------------------------------------------------------------

  @Test
  void metadataConstantsHaveCorrectValues() {
    assertThat(TrieNodeDiffCodec.ENTRY_FULL).isEqualTo((byte) 0x01);
    assertThat(TrieNodeDiffCodec.NODE_IS_BRANCH).isEqualTo((byte) 0x02);
    assertThat(TrieNodeDiffCodec.KEY_CHANGED).isEqualTo((byte) 0x04);
    assertThat(TrieNodeDiffCodec.VALUE_CHANGED).isEqualTo((byte) 0x08);
    assertThat(TrieNodeDiffCodec.CREATION).isEqualTo((byte) 0x10);
    assertThat(TrieNodeDiffCodec.DELETION).isEqualTo((byte) 0x20);
  }

  // -------------------------------------------------------------------------
  // Task 1.2: Branch DIFF encode/decode
  // -------------------------------------------------------------------------

  @Test
  void branchDiffCapturesChangedChildren() {
    // child 3 = h3, child 7 = h7, rest empty
    Bytes[] oldChildren = new Bytes[16];
    oldChildren[3] = childHash(3);
    oldChildren[7] = childHash(7);
    Bytes oldBranch = branchWith(oldChildren);

    // child 3 = h3b (changed), child 7 = h7 (unchanged), child 9 = h9 (new)
    Bytes[] newChildren = new Bytes[16];
    newChildren[3] = childHash(30); // changed: different from h3
    newChildren[7] = childHash(7); // same
    newChildren[9] = childHash(9); // new
    Bytes newBranch = branchWith(newChildren);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(oldBranch, newBranch);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.isFull()).isFalse();
    assertThat(d.isBranchNode()).isTrue();
    // children 3 (changed) and 9 (newly set) differ; child 7 is unchanged
    assertThat(d.changedChildIndices()).containsExactly(3, 9);
  }

  @Test
  void branchDiffNoOpProducesEmptyMask() {
    // Both old and new branch are identical
    Bytes[] children = new Bytes[16];
    children[5] = childHash(5);
    children[11] = childHash(11);
    Bytes branch = branchWith(children);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(branch, branch);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.isFull()).isFalse();
    assertThat(d.isBranchNode()).isTrue();
    assertThat(d.changedChildIndices()).isEmpty();
    assertThat(d.changedChildRefs()).isEmpty();
  }

  @Test
  void branchDiffValueChangedFlagAndAccessor() {
    // Same children, only value changes
    Bytes[] children = new Bytes[16];
    children[2] = childHash(2);
    Bytes branchValue = Bytes.fromHexString("0xaabbcc");
    Bytes oldBranch = branchWith(children, Bytes.EMPTY);
    Bytes newBranch = branchWith(children, branchValue);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(oldBranch, newBranch);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.isFull()).isFalse();
    assertThat(d.isBranchNode()).isTrue();
    assertThat((d.metadata() & TrieNodeDiffCodec.VALUE_CHANGED)).isNotEqualTo(0);
    assertThat(d.changedChildIndices()).isEmpty();
    assertThat(d.changedValue()).isPresent();
    assertThat(d.changedValue()).hasValue(branchValue);
  }

  @Test
  void branchDiffChildRefsMapContainsNewValues() {
    Bytes[] oldChildren = new Bytes[16];
    oldChildren[1] = childHash(1);
    Bytes[] newChildren = new Bytes[16];
    newChildren[1] = childHash(10); // changed
    newChildren[4] = childHash(4); // added

    Bytes oldBranch = branchWith(oldChildren);
    Bytes newBranch = branchWith(newChildren);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(oldBranch, newBranch);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.changedChildIndices()).containsExactly(1, 4);

    // The stored value for each changed child is the raw RLP of the new child item.
    // For a 32-byte hash ref, that's 0xa0 followed by 32 bytes (33 bytes total).
    var refs = d.changedChildRefs();
    assertThat(refs).containsKey(1);
    assertThat(refs).containsKey(4);
    // raw RLP of a 32-byte hash ref = 0xa0 + hash bytes (33 bytes)
    Bytes expectedRef1 = RLP.encodeOne(childHash(10));
    Bytes expectedRef4 = RLP.encodeOne(childHash(4));
    assertThat(refs.get(1)).isEqualTo(expectedRef1);
    assertThat(refs.get(4)).isEqualTo(expectedRef4);
  }

  @Test
  void branchDiffChildBecomingEmptyIsDetected() {
    // Child slot 6 goes from some hash to empty
    Bytes[] oldChildren = new Bytes[16];
    oldChildren[6] = childHash(6);
    Bytes[] newChildren = new Bytes[16]; // all empty

    Bytes oldBranch = branchWith(oldChildren);
    Bytes newBranch = branchWith(newChildren);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(oldBranch, newBranch);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.changedChildIndices()).containsExactly(6);
    // new value of slot 6 is the raw RLP null (0x80 = empty byte string)
    assertThat(d.changedChildRefs().get(6)).isEqualTo(Bytes.of((byte) 0x80));
  }

  @Test
  void changedChildIndicesThrowsOnFullEntry() {
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP));
    assertThatThrownBy(d::changedChildIndices).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void changedChildRefsThrowsOnFullEntry() {
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP));
    assertThatThrownBy(d::changedChildRefs).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void changedValueEmptyWhenValueChangedFlagNotSet() {
    // Only a child changes; the branch value is unchanged so VALUE_CHANGED must be clear.
    Bytes[] oldChildren = new Bytes[16];
    oldChildren[3] = childHash(3);
    Bytes[] newChildren = new Bytes[16];
    newChildren[3] = childHash(30);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(branchWith(oldChildren), branchWith(newChildren));
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat((d.metadata() & TrieNodeDiffCodec.VALUE_CHANGED)).isEqualTo(0);
    assertThat(d.changedValue()).isEmpty();
  }

  @Test
  void encodeDiffThrowsWhenBranchValueExceeds255Bytes() {
    // Same children, value grows from empty to 256 bytes — too large for the 1-byte length prefix.
    Bytes[] children = new Bytes[16];
    children[0] = childHash(0);
    Bytes oldBranch = branchWith(children, Bytes.EMPTY);
    Bytes bigValue = Bytes.repeat((byte) 0x11, 256);
    Bytes newBranch = branchWith(children, bigValue);

    assertThatThrownBy(() -> TrieNodeDiffCodec.encodeDiff(oldBranch, newBranch))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void branchAccessorsThrowOnNonBranchDiffEntry() {
    // Hand-craft a DIFF entry whose metadata clears ENTRY_FULL and NODE_IS_BRANCH (a short-node
    // DIFF). Branch accessors must reject it rather than mis-parse.
    Bytes nonBranchDiff = Bytes.concatenate(Bytes.of(0x00), SIMPLE_NODE_RLP);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(nonBranchDiff);
    assertThat(d.isFull()).isFalse();
    assertThat(d.isBranchNode()).isFalse();
    assertThatThrownBy(d::changedChildIndices).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(d::changedChildRefs).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(d::changedValue).isInstanceOf(IllegalStateException.class);
  }

  // -------------------------------------------------------------------------
  // Task 1.3: Short-node DIFF + creation / deletion / type-change
  // -------------------------------------------------------------------------

  /**
   * Builds a 2-item short-node RLP.
   *
   * <p>Item 0 (path) is written as a byte-string via {@code writeBytes(path)}. Item 1 (value) is
   * written as raw RLP via {@code writeRaw(valueRlp)}. This mirrors the way ExtensionNode uses
   * {@code writeRaw(child.getEncodedBytesRef())} and LeafNode uses {@code
   * writeBytes(valueSerializer.apply(value))} — both result in raw RLP that can be round-tripped
   * via {@code readAsRlp().raw()}.
   *
   * <p>For leaf nodes: pass {@code RLP.encodeOne(value)} as {@code valueRlp} so item 1 is an RLP
   * byte-string. For extension nodes: pass the child's encoded bytes ref (hash or inline list).
   */
  private static Bytes shortNodeWith(final Bytes path, final Bytes valueRlp) {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(path); // item 0: compact-encoded path (byte string)
          out.writeRaw(valueRlp); // item 1: raw RLP of value/child-ref
          out.endList();
        });
  }

  /** Convenience: build a leaf-style short node with a byte-string value. */
  private static Bytes leafNode(final Bytes path, final Bytes value) {
    // value is a byte string → raw RLP is RLP.encodeOne(value)
    return shortNodeWith(path, RLP.encodeOne(value));
  }

  /** A compact-encoded leaf path (just some non-zero bytes). */
  private static final Bytes LEAF_PATH = Bytes.fromHexString("0x20ab"); // compact leaf prefix

  /** A typical leaf value (account RLP ~72 bytes). */
  private static final Bytes LEAF_VALUE =
      Bytes.fromHexString(
          "0xf84e0185012a05f2008252089400000000000000000000000000000000000000018203e880");

  /** An alternative leaf value (different content). */
  private static final Bytes LEAF_VALUE_2 =
      Bytes.fromHexString(
          "0xf84e0285012a05f2008252089400000000000000000000000000000000000000028203e880");

  // --- 1.3a: short-node VALUE change ---

  @Test
  void shortNodeValueDiff() {
    // Leaf value changes → VALUE_CHANGED diff decodes to new value; KEY_CHANGED clear.
    Bytes oldLeaf = leafNode(LEAF_PATH, LEAF_VALUE);
    Bytes newLeaf = leafNode(LEAF_PATH, LEAF_VALUE_2);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(oldLeaf, newLeaf);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.isFull()).isFalse();
    assertThat(d.isBranchNode()).isFalse();
    assertThat(d.metadata() & TrieNodeDiffCodec.VALUE_CHANGED).isNotEqualTo(0);
    assertThat(d.metadata() & TrieNodeDiffCodec.KEY_CHANGED).isEqualTo(0);
    assertThat(d.changedShortNodeValue()).isPresent();
    // changedShortNodeValue() returns the raw RLP of item 1 (same unit as encoded)
    assertThat(d.changedShortNodeValue()).hasValue(RLP.encodeOne(LEAF_VALUE_2));
    assertThat(d.changedKey()).isEmpty();
  }

  // --- 1.3b: short-node KEY change ---

  @Test
  void shortNodeKeyDiff() {
    // Path changes → KEY_CHANGED; value unchanged → VALUE_CHANGED clear.
    Bytes path2 = Bytes.fromHexString("0x20ef");
    Bytes oldLeaf = leafNode(LEAF_PATH, LEAF_VALUE);
    Bytes newLeaf = leafNode(path2, LEAF_VALUE);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(oldLeaf, newLeaf);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.isFull()).isFalse();
    assertThat(d.isBranchNode()).isFalse();
    assertThat(d.metadata() & TrieNodeDiffCodec.KEY_CHANGED).isNotEqualTo(0);
    assertThat(d.metadata() & TrieNodeDiffCodec.VALUE_CHANGED).isEqualTo(0);
    assertThat(d.changedKey()).isPresent();
    assertThat(d.changedKey()).hasValue(path2); // new path bytes (decoded item 0)
    assertThat(d.changedShortNodeValue()).isEmpty();
  }

  // --- 1.3c: short-node BOTH key + value change ---

  @Test
  void shortNodeBothKeyAndValueDiff() {
    Bytes path2 = Bytes.fromHexString("0x20ef");
    Bytes oldLeaf = leafNode(LEAF_PATH, LEAF_VALUE);
    Bytes newLeaf = leafNode(path2, LEAF_VALUE_2);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(oldLeaf, newLeaf);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.isFull()).isFalse();
    assertThat(d.isBranchNode()).isFalse();
    assertThat(d.metadata() & TrieNodeDiffCodec.KEY_CHANGED).isNotEqualTo(0);
    assertThat(d.metadata() & TrieNodeDiffCodec.VALUE_CHANGED).isNotEqualTo(0);
    assertThat(d.changedKey()).hasValue(path2);
    assertThat(d.changedShortNodeValue()).hasValue(RLP.encodeOne(LEAF_VALUE_2));
  }

  // --- 1.3d: short-node NO-OP diff (identical nodes) ---

  @Test
  void shortNodeNoOpDiff() {
    Bytes leaf = leafNode(LEAF_PATH, LEAF_VALUE);
    Bytes entry = TrieNodeDiffCodec.encodeDiff(leaf, leaf);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.isFull()).isFalse();
    assertThat(d.isBranchNode()).isFalse();
    assertThat(d.metadata() & TrieNodeDiffCodec.KEY_CHANGED).isEqualTo(0);
    assertThat(d.metadata() & TrieNodeDiffCodec.VALUE_CHANGED).isEqualTo(0);
    assertThat(d.changedKey()).isEmpty();
    assertThat(d.changedShortNodeValue()).isEmpty();
  }

  // --- 1.3e: CREATION (old == null) → FULL entry ---

  @Test
  void creationIsFull() {
    Bytes leaf = leafNode(LEAF_PATH, LEAF_VALUE);
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeDiff(null, leaf));
    assertThat(d.isFull()).isTrue();
    assertThat(d.isCreation()).isTrue();
    assertThat(d.fullNode()).isEqualTo(leaf);
  }

  // --- 1.3f: DELETION (new == null) → tombstone ---

  @Test
  void deletionTombstone() {
    Bytes leaf = leafNode(LEAF_PATH, LEAF_VALUE);
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeDiff(leaf, null));
    assertThat(d.isDeletion()).isTrue();
    assertThat(d.isFull()).isFalse();
  }

  @Test
  void deletionTombstoneIsNotShortNodeDiff() {
    // A tombstone has ENTRY_FULL=0 and NODE_IS_BRANCH=0 (same as a short-node diff), but it carries
    // no short-node fields. isShortNodeDiff() must reject it and short-node accessors must throw so
    // Task 1.4's reconstruct never treats a deletion as a short-node diff.
    Bytes leaf = leafNode(LEAF_PATH, LEAF_VALUE);
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeDiff(leaf, null));
    assertThat(d.isDeletion()).isTrue();
    assertThat(d.isShortNodeDiff()).isFalse();
    assertThatThrownBy(d::changedKey).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(d::changedShortNodeValue).isInstanceOf(IllegalStateException.class);
  }

  // --- 1.3g: TYPE CHANGE (branch ↔ short) → FULL entry ---

  @Test
  void typeChangeIsFull() {
    // Use branchWith() to get a properly-formed 17-item branch RLP for RLP parsing.
    Bytes[] children = new Bytes[16];
    children[1] = childHash(1);
    Bytes branch = branchWith(children);
    Bytes leaf = leafNode(LEAF_PATH, LEAF_VALUE);

    // branch → leaf
    TrieNodeDiffCodec.Decoded d1 =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeDiff(branch, leaf));
    assertThat(d1.isFull()).isTrue();
    assertThat(d1.fullNode()).isEqualTo(leaf);

    // leaf → branch
    TrieNodeDiffCodec.Decoded d2 =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeDiff(leaf, branch));
    assertThat(d2.isFull()).isTrue();
    assertThat(d2.fullNode()).isEqualTo(branch);
  }

  // --- 1.3h: encodeDiff(null, null) throws ---

  @Test
  void encodeDiffBothNullThrows() {
    assertThatThrownBy(() -> TrieNodeDiffCodec.encodeDiff(null, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- 1.3i: short-node accessors throw on FULL/branch entries ---

  @Test
  void shortNodeAccessorsThrowOnFullEntry() {
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeFull(BRANCH_NODE_RLP));
    assertThatThrownBy(d::changedKey).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(d::changedShortNodeValue).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shortNodeAccessorsThrowOnBranchDiffEntry() {
    // Build a branch diff entry
    Bytes[] children = new Bytes[16];
    children[0] = childHash(0);
    children[1] = childHash(1);
    Bytes oldBranch = branchWith(new Bytes[16]);
    Bytes newBranch = branchWith(children);
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeDiff(oldBranch, newBranch));
    assertThat(d.isBranchNode()).isTrue();
    assertThatThrownBy(d::changedKey).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(d::changedShortNodeValue).isInstanceOf(IllegalStateException.class);
  }

  // --- 1.3j: branch changedValue() still throws on short-node diff (regression guard) ---

  @Test
  void branchChangedValueThrowsOnShortNodeDiff() {
    Bytes oldLeaf = leafNode(LEAF_PATH, LEAF_VALUE);
    Bytes newLeaf = leafNode(LEAF_PATH, LEAF_VALUE_2);
    TrieNodeDiffCodec.Decoded d =
        TrieNodeDiffCodec.decode(TrieNodeDiffCodec.encodeDiff(oldLeaf, newLeaf));
    assertThat(d.isBranchNode()).isFalse();
    assertThatThrownBy(d::changedValue).isInstanceOf(IllegalStateException.class);
  }

  // --- 1.3k: large value (>255 bytes) round-trips with 2-byte length prefix ---

  @Test
  void shortNodeLargeValueRoundTrips() {
    // A value of 260 bytes exceeds 1-byte length prefix capacity (255 max).
    // The codec must use a 2-byte length prefix for short-node fields.
    Bytes bigValue = Bytes.repeat((byte) 0xab, 260);
    Bytes bigValueRlp = RLP.encodeOne(bigValue); // RLP byte string of 260 bytes
    Bytes oldLeaf = shortNodeWith(LEAF_PATH, RLP.encodeOne(LEAF_VALUE));
    Bytes newLeaf = shortNodeWith(LEAF_PATH, bigValueRlp);

    Bytes entry = TrieNodeDiffCodec.encodeDiff(oldLeaf, newLeaf);
    TrieNodeDiffCodec.Decoded d = TrieNodeDiffCodec.decode(entry);

    assertThat(d.isFull()).isFalse();
    assertThat(d.isBranchNode()).isFalse();
    assertThat(d.changedShortNodeValue()).hasValue(bigValueRlp);
  }
}
