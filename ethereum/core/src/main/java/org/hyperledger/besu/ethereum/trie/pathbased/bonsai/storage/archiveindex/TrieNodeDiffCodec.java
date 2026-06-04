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

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Encodes and decodes per-node history entries stored in the trie-node differential index.
 *
 * <h2>Wire format</h2>
 *
 * <p>Every entry begins with a single <em>metadata byte</em> whose bits are defined by the {@code
 * ENTRY_*} / {@code NODE_*} constants below (design §5.3). The remainder of the entry depends on
 * the entry type:
 *
 * <ul>
 *   <li><b>FULL</b> ({@link #ENTRY_FULL} set): {@code [metadata(0x01)] ‖ [full node RLP bytes]}.
 *       The full trie node is stored verbatim — no delta compression.
 *   <li><b>DIFF — branch node</b> ({@link #ENTRY_FULL} clear, {@link #NODE_IS_BRANCH} set): {@code
 *       [metadata] ‖ [childMask: 2 bytes big-endian] ‖ (for each set bit i ascending: [1-byte
 *       length][raw RLP of new child item]) ‖ (if VALUE_CHANGED: [1-byte length][new value
 *       bytes])}. See §5.3 branch layout and the "child-ref unit" note below.
 *   <li><b>DIFF — short node</b> ({@link #ENTRY_FULL} clear, {@link #NODE_IS_BRANCH} clear):
 *       delta-compressed representation. Parsing is implemented in Task 1.3; calling {@link
 *       Decoded#changedChildIndices()} on a non-branch diff throws {@link IllegalStateException}.
 * </ul>
 *
 * <h2>Metadata byte bit assignments (§5.3)</h2>
 *
 * <pre>
 * bit0  ENTRY_FULL    (0x01)  1 = full node RLP follows; 0 = diff entry
 * bit1  NODE_IS_BRANCH (0x02) 1 = branch (17-item list); 0 = extension/leaf (2-item short node)
 * bit2  KEY_CHANGED   (0x04)  (short node) path segment changed
 * bit3  VALUE_CHANGED (0x08)  (short/branch) embedded value changed
 * bit4  CREATION      (0x10)  node created at this block (no prior)
 * bit5  DELETION      (0x20)  node deleted at this block (tombstone)
 * bit6-7 reserved
 * </pre>
 *
 * <h2>Child-ref unit (branch DIFF entries)</h2>
 *
 * <p>Each changed child slot is stored as the <em>raw RLP bytes of that child item</em> as it
 * appears in the branch node's 17-item RLP list — i.e. whatever {@link RLPInput#readAsRlp()} {@code
 * .raw()} returns for that slot. This handles all three cases uniformly:
 *
 * <ul>
 *   <li><b>Empty slot</b>: raw RLP is {@code 0x80} (RLP null / empty byte-string), 1 byte.
 *   <li><b>32-byte hash ref</b>: raw RLP is {@code 0xa0} + 32 bytes = 33 bytes.
 *   <li><b>Inline node</b>: raw RLP is the RLP list encoding. A node is only inlined when its
 *       encoding is shorter than a 32-byte hash ref, so the raw RLP of an inline child is always
 *       fewer than 33 bytes.
 * </ul>
 *
 * <p>Each field is framed with a <b>1-byte unsigned length prefix</b> (0–255). In practice raw RLP
 * of a branch child is at most 33 bytes (hash ref), well within 255. The length prefix allows
 * empty→non-empty and non-empty→empty transitions to round-trip correctly. The branch terminal
 * value (item 16) follows the same length-prefix framing when VALUE_CHANGED is set.
 */
public final class TrieNodeDiffCodec {

  // -------------------------------------------------------------------------
  // Metadata byte constants (§5.3) — ALL defined here even if not all used yet.
  // -------------------------------------------------------------------------

  /** bit0: full node RLP follows; if clear this is a diff entry. */
  public static final byte ENTRY_FULL = 0x01;

  /** bit1: the node is a branch (17-item RLP list); clear = extension or leaf (2-item). */
  public static final byte NODE_IS_BRANCH = 0x02;

  /** bit2: (short node diff) the path segment (key) changed relative to the previous version. */
  public static final byte KEY_CHANGED = 0x04;

  /** bit3: (short/branch node diff) the embedded value changed relative to the previous version. */
  public static final byte VALUE_CHANGED = 0x08;

  /** bit4: this node was created at this block — there is no prior version to diff against. */
  public static final byte CREATION = 0x10;

  /** bit5: this node was deleted at this block (tombstone entry). */
  public static final byte DELETION = 0x20;

  /** Number of child slots in a branch node. */
  private static final int BRANCH_CHILDREN = 16;

  private TrieNodeDiffCodec() {}

  // -------------------------------------------------------------------------
  // Encoding
  // -------------------------------------------------------------------------

  /**
   * Encodes a full trie-node RLP as a FULL entry.
   *
   * <p>Layout: {@code [metadata byte = ENTRY_FULL]} ‖ {@code nodeRlp}.
   *
   * @param nodeRlp the raw RLP bytes of the trie node; must not be {@code null}
   * @return the encoded entry (1 + nodeRlp.size() bytes)
   * @throws NullPointerException if {@code nodeRlp} is {@code null}
   */
  public static Bytes encodeFull(final Bytes nodeRlp) {
    Objects.requireNonNull(nodeRlp, "nodeRlp must not be null");
    return Bytes.concatenate(Bytes.of(ENTRY_FULL), nodeRlp);
  }

  /**
   * Encodes the diff between two branch-node RLPs as a DIFF entry.
   *
   * <p>Both {@code oldNodeRlp} and {@code newNodeRlp} must be 17-item RLP lists (branch nodes). The
   * resulting entry captures only the child slots that changed (old raw RLP != new raw RLP) and the
   * branch terminal value if it changed.
   *
   * <h3>Wire layout (§5.3 branch DIFF)</h3>
   *
   * <pre>
   * [metadata byte: NODE_IS_BRANCH set; VALUE_CHANGED set if value differs; ENTRY_FULL clear]
   * [childMask: 2 bytes big-endian]   — bit i set iff child slot i changed
   * for each set bit i (ascending 0..15):
   *     [1-byte length][raw RLP bytes of new child i]
   * if VALUE_CHANGED:
   *     [1-byte length][new branch value bytes]
   * </pre>
   *
   * <p>Each child ref is the raw RLP of the child item as it appears in the branch list (see
   * class-level Javadoc for the child-ref unit). A 1-byte length prefix frames each variable-length
   * field; this supports empty (length 0), 32-byte hash refs (raw RLP = 33 bytes), and inline
   * nodes.
   *
   * @param oldNodeRlp the prior branch node RLP; must not be {@code null}
   * @param newNodeRlp the updated branch node RLP; must not be {@code null}
   * @return encoded DIFF entry
   * @throws NullPointerException if either argument is {@code null}
   * @throws IllegalArgumentException if either argument cannot be decoded as a 17-item RLP list
   */
  public static Bytes encodeDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
    Objects.requireNonNull(oldNodeRlp, "oldNodeRlp must not be null");
    Objects.requireNonNull(newNodeRlp, "newNodeRlp must not be null");

    // Parse both branch nodes into their raw child/value RLP items.
    final BranchFields oldFields = parseBranchFields(oldNodeRlp);
    final BranchFields newFields = parseBranchFields(newNodeRlp);

    // Compute 16-bit changed-child mask.
    int childMask = 0;
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if (!oldFields.children[i].equals(newFields.children[i])) {
        childMask |= (1 << i);
      }
    }

    // Determine whether the branch terminal value changed.
    final boolean valueChanged = !oldFields.value.equals(newFields.value);

    // Build metadata byte.
    byte metadata = NODE_IS_BRANCH;
    if (valueChanged) {
      metadata |= VALUE_CHANGED;
    }

    // Assemble output: metadata + 2-byte mask + changed child refs [+ new value].
    final List<Bytes> parts = new ArrayList<>();
    parts.add(Bytes.of(metadata));
    // childMask: 2 bytes big-endian
    parts.add(Bytes.of((byte) ((childMask >> 8) & 0xFF), (byte) (childMask & 0xFF)));

    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if ((childMask & (1 << i)) != 0) {
        final Bytes ref = newFields.children[i];
        if (ref.size() > 255) {
          throw new IllegalArgumentException(
              "Child ref raw RLP too large for 1-byte length prefix: " + ref.size());
        }
        parts.add(Bytes.of((byte) ref.size()));
        parts.add(ref);
      }
    }

    if (valueChanged) {
      final Bytes val = newFields.value;
      if (val.size() > 255) {
        throw new IllegalArgumentException(
            "Branch value too large for 1-byte length prefix: " + val.size());
      }
      parts.add(Bytes.of((byte) val.size()));
      parts.add(val);
    }

    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  // -------------------------------------------------------------------------
  // Branch parsing helper
  // -------------------------------------------------------------------------

  /**
   * Parses a branch node RLP into raw child-item bytes and value bytes.
   *
   * <p>Each {@code children[i]} is the <em>raw RLP</em> of child item {@code i} as read by {@link
   * RLPInput#readAsRlp()}{@code .raw()} — this handles empty slots (0x80), 32-byte hash refs (0xa0
   * + 32 bytes), and inline nodes uniformly. The {@code value} field is the decoded byte payload of
   * item 16 ({@code Bytes.EMPTY} for absent/null).
   */
  private static BranchFields parseBranchFields(final Bytes nodeRlp) {
    final RLPInput in = RLP.input(nodeRlp);
    final int count = in.enterList();
    if (count != 17) {
      throw new IllegalArgumentException(
          "Expected 17-item branch node RLP list, got " + count + " items");
    }

    final Bytes[] children = new Bytes[BRANCH_CHILDREN];
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      // Capture the raw RLP of this child item (handles byte-string, list, and null uniformly).
      children[i] = in.readAsRlp().raw();
    }

    // Item 16: branch terminal value. Decoded as the raw byte payload (not raw RLP) so that it
    // round-trips through RLPOutput.writeBytes() when reconstructing the node in later tasks.
    final Bytes value = in.nextIsNull() ? readNullAsEmpty(in) : in.readBytes();
    in.leaveList();

    return new BranchFields(children, value);
  }

  /** Consumes the RLP null item and returns {@link Bytes#EMPTY}. */
  private static Bytes readNullAsEmpty(final RLPInput in) {
    in.skipNext();
    return Bytes.EMPTY;
  }

  private static final class BranchFields {
    final Bytes[] children;
    final Bytes value;

    BranchFields(final Bytes[] children, final Bytes value) {
      this.children = children;
      this.value = value;
    }
  }

  // -------------------------------------------------------------------------
  // Decoding
  // -------------------------------------------------------------------------

  /**
   * Decodes an entry produced by {@link #encodeFull} or {@link #encodeDiff}.
   *
   * @param entry the raw entry bytes; must not be {@code null} and must be at least 1 byte long
   * @return a {@link Decoded} value exposing the metadata and entry-type-specific accessors
   * @throws NullPointerException if {@code entry} is {@code null}
   * @throws IllegalArgumentException if {@code entry} is empty
   */
  public static Decoded decode(final Bytes entry) {
    Objects.requireNonNull(entry, "entry must not be null");
    if (entry.isEmpty()) {
      throw new IllegalArgumentException("Entry must be at least 1 byte (metadata byte)");
    }
    byte metadata = entry.get(0);
    Bytes body = entry.slice(1);
    return new Decoded(metadata, body);
  }

  // -------------------------------------------------------------------------
  // Decoded value type
  // -------------------------------------------------------------------------

  /**
   * Immutable value type holding the result of {@link #decode(Bytes)}.
   *
   * <p>The metadata byte is always accessible via {@link #metadata()} or the boolean predicates.
   * For FULL entries, {@link #fullNode()} returns the node RLP. For branch DIFF entries (Task 1.2),
   * {@link #changedChildIndices()}, {@link #changedChildRefs()}, and {@link #changedValue()} are
   * available. Short-node diff accessors ({@code changedKey}) are added in Task 1.3.
   */
  public static final class Decoded {

    private final byte metadata;
    private final Bytes body;

    private Decoded(final byte metadata, final Bytes body) {
      this.metadata = metadata;
      this.body = body;
    }

    // ------------------------------------------------------------------
    // Raw metadata access
    // ------------------------------------------------------------------

    /**
     * Returns the raw metadata byte for this entry. Callers can test individual bits using the
     * public constants defined on {@link TrieNodeDiffCodec} (e.g. {@link
     * TrieNodeDiffCodec#NODE_IS_BRANCH}).
     */
    public byte metadata() {
      return metadata;
    }

    // ------------------------------------------------------------------
    // Entry-type predicates
    // ------------------------------------------------------------------

    /** Returns {@code true} iff this is a FULL entry ({@link TrieNodeDiffCodec#ENTRY_FULL} set). */
    public boolean isFull() {
      return (metadata & ENTRY_FULL) != 0;
    }

    /**
     * Returns {@code true} iff the node was created at this block ({@link
     * TrieNodeDiffCodec#CREATION} set in metadata).
     */
    public boolean isCreation() {
      return (metadata & CREATION) != 0;
    }

    /**
     * Returns {@code true} iff the node was deleted at this block ({@link
     * TrieNodeDiffCodec#DELETION} set in metadata).
     */
    public boolean isDeletion() {
      return (metadata & DELETION) != 0;
    }

    /**
     * Returns {@code true} iff the node is a branch node ({@link TrieNodeDiffCodec#NODE_IS_BRANCH}
     * set in metadata).
     */
    public boolean isBranchNode() {
      return (metadata & NODE_IS_BRANCH) != 0;
    }

    // ------------------------------------------------------------------
    // FULL-entry accessor
    // ------------------------------------------------------------------

    /**
     * Returns the full node RLP for a FULL entry.
     *
     * @return raw RLP bytes of the trie node
     * @throws IllegalStateException if this is a diff entry (i.e. {@link #isFull()} is {@code
     *     false})
     */
    public Bytes fullNode() {
      if (!isFull()) {
        throw new IllegalStateException(
            "fullNode() called on a diff entry; use diff accessors (Task 1.2–1.4)");
      }
      return body;
    }

    // ------------------------------------------------------------------
    // Branch DIFF accessors (Task 1.2)
    // ------------------------------------------------------------------

    /**
     * Returns the ascending list of child slot indices (0–15) that changed in this branch DIFF
     * entry.
     *
     * <p>The list is derived from the 2-byte big-endian {@code childMask} embedded in the entry:
     * bit {@code i} set means child slot {@code i} changed.
     *
     * @return unmodifiable list of changed child indices in ascending order
     * @throws IllegalStateException if called on a FULL entry or a non-branch DIFF entry
     */
    public List<Integer> changedChildIndices() {
      requireBranchDiff("changedChildIndices()");
      final int mask = readChildMask();
      final List<Integer> indices = new ArrayList<>();
      for (int i = 0; i < BRANCH_CHILDREN; i++) {
        if ((mask & (1 << i)) != 0) {
          indices.add(i);
        }
      }
      return Collections.unmodifiableList(indices);
    }

    /**
     * Returns a map from changed child slot index to the <em>raw RLP bytes of the new child
     * item</em> for each changed slot in this branch DIFF entry.
     *
     * <p>The raw RLP of each child item is the same unit stored by {@link
     * TrieNodeDiffCodec#encodeDiff}: for an empty slot it is {@code 0x80} (RLP null, 1 byte); for a
     * 32-byte hash ref it is {@code 0xa0} + 32 bytes (33 bytes); for an inline node it is the
     * node's RLP list encoding (always &lt; 32 bytes by trie rules).
     *
     * @return unmodifiable map: child index → new child raw RLP, in ascending index order
     * @throws IllegalStateException if called on a FULL entry or a non-branch DIFF entry
     */
    public Map<Integer, Bytes> changedChildRefs() {
      requireBranchDiff("changedChildRefs()");
      final int mask = readChildMask();
      // Walk the body past the 2-byte mask, reading length-prefixed refs for each set bit.
      int offset = 2; // skip childMask bytes
      final Map<Integer, Bytes> result = new LinkedHashMap<>();
      for (int i = 0; i < BRANCH_CHILDREN; i++) {
        if ((mask & (1 << i)) != 0) {
          final int len = Byte.toUnsignedInt(body.get(offset));
          offset += 1;
          final Bytes ref = body.slice(offset, len);
          offset += len;
          result.put(i, ref);
        }
      }
      return Collections.unmodifiableMap(result);
    }

    /**
     * Returns the new branch terminal value if {@link TrieNodeDiffCodec#VALUE_CHANGED} is set,
     * otherwise {@link Optional#empty()}.
     *
     * @return optional new branch value bytes (the decoded payload, not RLP-framed)
     * @throws IllegalStateException if called on a FULL entry or a non-branch DIFF entry
     */
    public Optional<Bytes> changedValue() {
      requireBranchDiff("changedValue()");
      if ((metadata & VALUE_CHANGED) == 0) {
        return Optional.empty();
      }
      // The value follows all length-prefixed child refs; its own 1-byte length prefix is at that
      // offset.
      int offset = offsetAfterChildRefs(readChildMask());
      final int len = Byte.toUnsignedInt(body.get(offset));
      offset += 1;
      final Bytes val = body.slice(offset, len);
      return Optional.of(val);
    }

    // ------------------------------------------------------------------
    // Diff accessors — TODO(Task 1.3): changedKey() — new path segment for short-node key changes
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    private void requireBranchDiff(final String methodName) {
      if (isFull() || !isBranchNode()) {
        throw new IllegalStateException(
            methodName + " called on a non-branch DIFF entry; only valid for branch DIFF entries");
      }
    }

    /**
     * Reads the 2-byte big-endian child mask from the start of the body (bytes 0–1). Returns an int
     * with bits 0–15 representing changed child slots 0–15.
     */
    private int readChildMask() {
      final int hi = Byte.toUnsignedInt(body.get(0));
      final int lo = Byte.toUnsignedInt(body.get(1));
      return (hi << 8) | lo;
    }

    /**
     * Walks the body past the 2-byte child mask and every length-prefixed child ref named by {@code
     * mask}, returning the offset of the first byte after the last child ref. This is the single
     * source of truth for the child-ref framing shared by {@link #changedChildRefs()} (where it is
     * inlined while reading each ref) and {@link #changedValue()} (which needs only the final
     * offset to locate the value). When VALUE_CHANGED is set, the returned offset points at the
     * value's 1-byte length prefix.
     */
    private int offsetAfterChildRefs(final int mask) {
      int offset = 2; // skip the 2-byte childMask
      for (int i = 0; i < BRANCH_CHILDREN; i++) {
        if ((mask & (1 << i)) != 0) {
          final int len = Byte.toUnsignedInt(body.get(offset));
          offset += 1 + len;
        }
      }
      return offset;
    }
  }
}
