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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

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
 *       The full trie node is stored verbatim — no delta compression. CREATION uses {@code
 *       ENTRY_FULL | CREATION}; type-change uses plain {@code ENTRY_FULL}.
 *   <li><b>DELETION tombstone</b> ({@link #DELETION} set, {@link #ENTRY_FULL} clear): only the
 *       metadata byte — no node bytes follow.
 *   <li><b>DIFF — branch node</b> ({@link #ENTRY_FULL} clear, {@link #NODE_IS_BRANCH} set): {@code
 *       [metadata] ‖ [childMask: 2 bytes big-endian] ‖ (for each set bit i ascending: [1-byte
 *       length][raw RLP of new child item]) ‖ (if VALUE_CHANGED: [1-byte length][new value
 *       bytes])}. See §5.3 branch layout and the "child-ref unit" note below.
 *   <li><b>DIFF — short node</b> ({@link #ENTRY_FULL} clear, {@link #NODE_IS_BRANCH} clear): {@code
 *       [metadata] ‖ (if KEY_CHANGED: [2-byte length][new path bytes]) ‖ (if VALUE_CHANGED: [2-byte
 *       length][raw RLP of new value item])}. The 2-byte big-endian length prefix accommodates
 *       short-node values that can exceed 255 bytes (unlike branch child refs which are at most 33
 *       bytes). Path bytes are the decoded compact-encoded path payload (item 0 read via {@link
 *       RLPInput#readBytes()}). Value is the raw RLP of item 1 (read via {@link
 *       RLPInput#readAsRlp()}{@code .raw()}) so that both leaf values (byte strings) and extension
 *       child refs (hash byte-strings or inline lists) round-trip uniformly.
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
 * bit6  HASH_REF      (0x40)  (FULL only) body is a 32-byte keccak256 CAS reference, not inline RLP
 * bit7  reserved
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
 *
 * <h2>Short-node DIFF capture units (§5.3)</h2>
 *
 * <p>Short nodes (both leaf and extension) are 2-item RLP lists. The codec captures:
 *
 * <ul>
 *   <li><b>Path (item 0)</b>: decoded byte payload via {@link RLPInput#readBytes()} — the
 *       compact-encoded path nibbles. Reconstruct with {@code out.writeBytes(path)}.
 *   <li><b>Value (item 1)</b>: raw RLP via {@link RLPInput#readAsRlp()}{@code .raw()} — handles
 *       leaf values (RLP byte-strings) and extension child-refs (hash byte-strings or inline lists)
 *       uniformly. Reconstruct with {@code out.writeRaw(valueRlp)}.
 * </ul>
 *
 * <p>Short-node fields use a <b>2-byte big-endian length prefix</b> because leaf values can exceed
 * 255 bytes (a full account RLP with large storage or code is up to ~110 bytes currently, but the
 * format must be future-proof). Branch child refs are at most 33 bytes and keep the 1-byte prefix.
 * An {@link IllegalArgumentException} is thrown if a field exceeds 65535 bytes.
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

  /**
   * Set (together with {@link #ENTRY_FULL}) when the entry body is a 32-byte keccak256 reference
   * into the {@code TRIE_NODE_CAS_ARCHIVE} content-addressed body store instead of inline node RLP.
   * Composes with {@link #CREATION}.
   */
  public static final byte HASH_REF = 0x40;

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
   * Encodes a FULL entry whose body is a 32-byte content-hash reference into the CAS body store.
   *
   * <p>Layout: {@code [metadata = ENTRY_FULL | HASH_REF (| CREATION)]} ‖ {@code keccak256(node)}.
   *
   * @param nodeHash the keccak256 of the referenced node RLP; must not be {@code null}
   * @param creation whether this entry records the node's first appearance
   * @return the encoded entry (33 bytes)
   */
  public static Bytes encodeFullRef(final Bytes32 nodeHash, final boolean creation) {
    Objects.requireNonNull(nodeHash, "nodeHash must not be null");
    final byte metadata = (byte) (ENTRY_FULL | HASH_REF | (creation ? CREATION : 0));
    return Bytes.concatenate(Bytes.of(metadata), nodeHash);
  }

  /**
   * Encodes the diff between two trie-node RLPs as a DIFF (or FULL/tombstone) entry.
   *
   * <p>Both arguments are nullable. The dispatch logic (in order) is:
   *
   * <ol>
   *   <li><b>Both null</b>: throws {@link IllegalArgumentException} — nonsensical call.
   *   <li><b>CREATION</b> ({@code oldNodeRlp == null}): returns a FULL entry of {@code newNodeRlp}
   *       with metadata {@code ENTRY_FULL | CREATION}. {@code isFull()} and {@code isCreation()}
   *       are both true; {@code fullNode()} returns {@code newNodeRlp}.
   *   <li><b>DELETION</b> ({@code newNodeRlp == null}): returns a tombstone with metadata {@code
   *       DELETION} only. No node bytes follow. {@code isDeletion()} is true; {@code isFull()} is
   *       false.
   *   <li><b>TYPE CHANGE</b> (one is a 17-item branch, the other a 2-item short node): returns a
   *       plain FULL entry of {@code newNodeRlp} ({@code encodeFull(newNodeRlp)} semantics). {@code
   *       isFull()} is true; no CREATION or NODE_IS_BRANCH bits are set. This avoids encoding a
   *       structurally incompatible delta.
   *   <li><b>BOTH BRANCH</b>: encodes a branch DIFF (Task 1.2 path).
   *   <li><b>BOTH SHORT</b>: encodes a short-node DIFF (Task 1.3 path).
   * </ol>
   *
   * @param oldNodeRlp the prior node RLP; {@code null} signals creation
   * @param newNodeRlp the updated node RLP; {@code null} signals deletion
   * @return encoded entry
   * @throws IllegalArgumentException if both arguments are null, or if a node RLP has an unexpected
   *     list arity
   */
  public static Bytes encodeDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
    // Case 1: both null — nonsensical.
    if (oldNodeRlp == null && newNodeRlp == null) {
      throw new IllegalArgumentException("encodeDiff: both old and new node RLPs are null");
    }

    // Case 2: CREATION — old is null, new is present.
    if (oldNodeRlp == null) {
      return Bytes.concatenate(Bytes.of((byte) (ENTRY_FULL | CREATION)), newNodeRlp);
    }

    // Case 3: DELETION — new is null; emit tombstone (metadata byte only, no body).
    if (newNodeRlp == null) {
      return Bytes.of(DELETION);
    }

    // Both non-null: detect node types.
    final int oldArity = nodeArity(oldNodeRlp);
    final int newArity = nodeArity(newNodeRlp);

    // Case 4: TYPE CHANGE — different arities → plain FULL of new node.
    if (oldArity != newArity) {
      return encodeFull(newNodeRlp);
    }

    // Case 5: BOTH BRANCH (17-item).
    if (oldArity == 17) {
      return encodeBranchDiff(oldNodeRlp, newNodeRlp);
    }

    // Case 6: BOTH SHORT (2-item).
    return encodeShortDiff(oldNodeRlp, newNodeRlp);
  }

  /**
   * Reconstructs a trie node by applying a sequence of DIFF entries to a FULL checkpoint.
   *
   * <p>Starting from the base node embedded in {@code fullEntry}, each diff in {@code diffEntries}
   * is applied in order, patching the mutable working fields (child slots for branch nodes; path
   * and value for short nodes). The final working state is re-encoded to produce the reconstructed
   * node RLP.
   *
   * <p>The parse→re-encode round-trip is byte-exact: calling {@code reconstruct(encodeFull(node),
   * List.of())} returns bytes equal to {@code node} for both branch and short nodes. This is
   * because each field is captured and re-encoded with matching read/write units:
   *
   * <ul>
   *   <li>Branch child slots (items 0–15): read via {@code readAsRlp().raw()} (raw RLP), written
   *       via {@code writeRaw(childRawRlp)}.
   *   <li>Branch value (item 16): read via {@code readBytes()} (decoded payload), written via
   *       {@code writeBytes(value)}.
   *   <li>Short-node path (item 0): read via {@code readBytes()} (decoded payload), written via
   *       {@code writeBytes(path)}.
   *   <li>Short-node value (item 1): read via {@code readAsRlp().raw()} (raw RLP), written via
   *       {@code writeRaw(valueRlp)}.
   * </ul>
   *
   * <p><b>Caller contract</b>: each entry in {@code diffEntries} MUST be a DIFF entry of the same
   * node type as the base, in ascending block order. If a FULL entry or deletion tombstone appears
   * in {@code diffEntries}, the caller (e.g. the Task 3.2 history reader) is responsible for
   * slicing the chain at that checkpoint or tombstone before calling this method. Passing a FULL or
   * deletion entry as a diff, or a diff whose node type mismatches the base, throws {@link
   * IllegalArgumentException}.
   *
   * @param fullEntry a FULL codec entry ({@link #ENTRY_FULL} set); must not be {@code null}
   * @param diffEntries ordered list of DIFF entries to apply; must not be {@code null} and must not
   *     contain {@code null} elements; each must be a branch DIFF or short-node DIFF matching the
   *     base node type
   * @return the reconstructed node RLP (raw node bytes, not a codec entry)
   * @throws NullPointerException if {@code fullEntry}, {@code diffEntries}, or any element of
   *     {@code diffEntries} is {@code null} (a null element trips the null check in {@link
   *     #decode(Bytes)})
   * @throws IllegalArgumentException if {@code fullEntry} is not a FULL entry, if any diff entry is
   *     a FULL entry or deletion tombstone, or if any diff entry's node type mismatches the base
   */
  public static Bytes reconstruct(final Bytes fullEntry, final List<Bytes> diffEntries) {
    Objects.requireNonNull(fullEntry, "fullEntry must not be null");
    Objects.requireNonNull(diffEntries, "diffEntries must not be null");

    final Decoded base = decode(fullEntry);
    if (!base.isFull()) {
      throw new IllegalArgumentException(
          "reconstruct: fullEntry must be a FULL entry (ENTRY_FULL bit set); got metadata 0x"
              + Integer.toHexString(Byte.toUnsignedInt(base.metadata())));
    }

    final Bytes baseNode = base.fullNode(); // throws on HASH_REF — caller must resolve first
    return reconstructFromNode(baseNode, diffEntries);
  }

  /**
   * Reconstructs a trie node from an already-resolved base node body plus ordered DIFF entries.
   * Same caller contract as {@link #reconstruct(Bytes, List)}, but the base is raw node RLP (e.g.
   * fetched from the CAS store for a HASH_REF checkpoint) rather than a FULL codec entry.
   *
   * @param baseNode the base node RLP (raw node bytes, not a codec entry); must not be {@code null}
   * @param diffEntries ordered list of DIFF entries to apply; same contract as {@link #reconstruct}
   * @return the reconstructed node RLP
   */
  public static Bytes reconstructFromNode(final Bytes baseNode, final List<Bytes> diffEntries) {
    Objects.requireNonNull(baseNode, "baseNode must not be null");
    Objects.requireNonNull(diffEntries, "diffEntries must not be null");
    final int arity = nodeArity(baseNode);
    if (arity == 17) {
      return reconstructBranch(baseNode, diffEntries);
    } else {
      return reconstructShort(baseNode, diffEntries);
    }
  }

  /**
   * Applies branch DIFF entries to a branch base node and re-encodes to node RLP.
   *
   * <p>Working fields: {@code children[]} (raw RLP per slot, as captured by {@code
   * readAsRlp().raw()}) and {@code value} (decoded payload, as captured by {@code readBytes()}).
   * Re-encoded with {@code writeRaw(children[i])} and {@code writeBytes(value)}.
   */
  private static Bytes reconstructBranch(final Bytes baseNode, final List<Bytes> diffEntries) {
    final BranchFields base = parseBranchFields(baseNode);
    final Bytes[] children = Arrays.copyOf(base.children, BRANCH_CHILDREN);
    Bytes value = base.value;

    for (final Bytes diffEntry : diffEntries) {
      final Decoded d = decode(diffEntry);
      requireDiffEntry(d);
      if (!d.isBranchNode()) {
        throw new IllegalArgumentException(
            "reconstruct type mismatch: base is a branch node but diff entry is a short-node diff"
                + " (metadata 0x"
                + Integer.toHexString(Byte.toUnsignedInt(d.metadata()))
                + ")");
      }
      // Patch child slots (last-write-wins for slots touched by multiple diffs).
      for (final Map.Entry<Integer, Bytes> entry : d.changedChildRefs().entrySet()) {
        children[entry.getKey()] = entry.getValue();
      }
      // Patch value if VALUE_CHANGED.
      final Optional<Bytes> newVal = d.changedValue();
      if (newVal.isPresent()) {
        value = newVal.get();
      }
    }

    // Re-encode: branch RLP list with 16 raw-RLP children + decoded-payload value.
    // writeBytes(Bytes.EMPTY) and writeNull() are byte-identical (both emit 0x80), so an empty
    // branch value round-trips exactly through writeBytes — no special-casing needed.
    final Bytes[] finalChildren = children;
    final Bytes finalValue = value;
    return RLP.encode(
        out -> {
          out.startList();
          for (int i = 0; i < BRANCH_CHILDREN; i++) {
            out.writeRaw(finalChildren[i]);
          }
          out.writeBytes(finalValue);
          out.endList();
        });
  }

  /**
   * Applies short-node DIFF entries to a short base node and re-encodes to node RLP.
   *
   * <p>Working fields: {@code path} (decoded payload, as captured by {@code readBytes()}) and
   * {@code valueRlp} (raw RLP, as captured by {@code readAsRlp().raw()}). Re-encoded with {@code
   * writeBytes(path)} and {@code writeRaw(valueRlp)}.
   */
  private static Bytes reconstructShort(final Bytes baseNode, final List<Bytes> diffEntries) {
    final ShortFields base = parseShortFields(baseNode);
    Bytes path = base.path;
    Bytes valueRlp = base.valueRlp;

    for (final Bytes diffEntry : diffEntries) {
      final Decoded d = decode(diffEntry);
      requireDiffEntry(d);
      if (d.isBranchNode()) {
        throw new IllegalArgumentException(
            "reconstruct type mismatch: base is a short node but diff entry is a branch-node diff"
                + " (metadata 0x"
                + Integer.toHexString(Byte.toUnsignedInt(d.metadata()))
                + ")");
      }
      // Patch path if KEY_CHANGED.
      final Optional<Bytes> newPath = d.changedKey();
      if (newPath.isPresent()) {
        path = newPath.get();
      }
      // Patch value if VALUE_CHANGED.
      final Optional<Bytes> newVal = d.changedShortNodeValue();
      if (newVal.isPresent()) {
        valueRlp = newVal.get();
      }
    }

    // Re-encode: short-node RLP list with decoded-payload path + raw-RLP value.
    final Bytes finalPath = path;
    final Bytes finalValueRlp = valueRlp;
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(finalPath);
          out.writeRaw(finalValueRlp);
          out.endList();
        });
  }

  /**
   * Validates that a decoded entry encountered during {@link #reconstruct} is an applicable DIFF
   * entry — i.e. neither a FULL checkpoint nor a deletion tombstone. The caller is responsible for
   * slicing the entry chain at checkpoints/tombstones before invoking {@code reconstruct}.
   *
   * @param d the decoded diff entry
   * @throws IllegalArgumentException if {@code d} is a FULL entry or a deletion tombstone
   */
  private static void requireDiffEntry(final Decoded d) {
    if (d.isFull() || d.isDeletion()) {
      throw new IllegalArgumentException(
          "reconstruct expects DIFF entries only; got FULL or deletion tombstone entry"
              + " (metadata 0x"
              + Integer.toHexString(Byte.toUnsignedInt(d.metadata()))
              + ")");
    }
  }

  /**
   * Encodes the diff between two branch-node RLPs as a DIFF entry.
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
   * @throws IllegalArgumentException if either argument cannot be decoded as a 17-item RLP list, or
   *     if any branch child ref or value exceeds 255 bytes
   */
  private static Bytes encodeBranchDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
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

  /**
   * Encodes the diff between two short-node (2-item) RLPs as a DIFF entry.
   *
   * <h3>Wire layout (§5.3 short-node DIFF)</h3>
   *
   * <pre>
   * [metadata: ENTRY_FULL=0, NODE_IS_BRANCH=0, KEY_CHANGED?, VALUE_CHANGED?]
   * if KEY_CHANGED:   [2-byte big-endian length][new path bytes]   (decoded item 0)
   * if VALUE_CHANGED: [2-byte big-endian length][raw RLP of new value item] (readAsRlp().raw())
   * </pre>
   *
   * <p>Path (item 0) is captured as the decoded byte payload via {@link RLPInput#readBytes()}.
   * Value (item 1) is captured as raw RLP via {@link RLPInput#readAsRlp()}{@code .raw()} — handles
   * both leaf values (RLP byte-strings) and extension child refs (hash byte-strings or inline
   * lists) uniformly. The 2-byte length prefix accommodates values larger than 255 bytes.
   */
  private static Bytes encodeShortDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
    final ShortFields oldFields = parseShortFields(oldNodeRlp);
    final ShortFields newFields = parseShortFields(newNodeRlp);

    final boolean keyChanged = !oldFields.path.equals(newFields.path);
    final boolean valueChanged = !oldFields.valueRlp.equals(newFields.valueRlp);

    byte metadata = 0;
    if (keyChanged) {
      metadata |= KEY_CHANGED;
    }
    if (valueChanged) {
      metadata |= VALUE_CHANGED;
    }

    final List<Bytes> parts = new ArrayList<>();
    parts.add(Bytes.of(metadata));

    if (keyChanged) {
      parts.add(frameShortField(newFields.path));
    }
    if (valueChanged) {
      parts.add(frameShortField(newFields.valueRlp));
    }

    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /**
   * Frames a short-node field with a 2-byte big-endian length prefix.
   *
   * <p>Short-node values may exceed 255 bytes (unlike branch child refs which are ≤33 bytes), so we
   * use a 2-byte prefix to future-proof the format.
   */
  private static Bytes frameShortField(final Bytes field) {
    final int len = field.size();
    if (len > 65535) {
      throw new IllegalArgumentException(
          "Short-node field too large for 2-byte length prefix: " + len);
    }
    return Bytes.concatenate(Bytes.of((byte) ((len >> 8) & 0xFF), (byte) (len & 0xFF)), field);
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
  // Short-node parsing helper
  // -------------------------------------------------------------------------

  /**
   * Returns the arity (list item count) of an RLP-encoded trie node.
   *
   * <p>Trie nodes are only ever 17-item branch nodes or 2-item short nodes (extension or leaf). All
   * list sizes other than 2 (short node) or 17 (branch) are rejected.
   *
   * @param nodeRlp the raw RLP bytes of the trie node
   * @return 2 (short node) or 17 (branch node)
   * @throws IllegalArgumentException if the RLP list has any other count
   */
  static int nodeArity(final Bytes nodeRlp) {
    final RLPInput in = RLP.input(nodeRlp);
    final int count = in.enterList();
    if (count != 2 && count != 17) {
      throw new IllegalArgumentException(
          "Expected a 2-item short node or 17-item branch node RLP list, got " + count + " items");
    }
    return count;
  }

  /**
   * Parses a short-node (2-item) RLP into path and value fields.
   *
   * <p>Item 0 (path) is captured as the decoded byte payload via {@link RLPInput#readBytes()} — the
   * compact-encoded path nibbles exactly as written by {@code out.writeBytes(encodedPath)} in
   * LeafNode and ExtensionNode. Item 1 (value) is captured as raw RLP via {@link
   * RLPInput#readAsRlp()}{@code .raw()} — handles leaf values (RLP byte-strings) and extension
   * child-refs (hash byte-strings or inline lists) uniformly. Reconstruct item 1 with {@code
   * out.writeRaw(valueRlp)}.
   */
  private static ShortFields parseShortFields(final Bytes nodeRlp) {
    final RLPInput in = RLP.input(nodeRlp);
    final int count = in.enterList();
    if (count != 2) {
      throw new IllegalArgumentException(
          "Expected 2-item short node RLP list, got " + count + " items");
    }
    final Bytes path = in.readBytes(); // item 0: compact-encoded path bytes
    final Bytes valueRlp = in.readAsRlp().raw(); // item 1: raw RLP of value/child-ref
    in.leaveList();
    return new ShortFields(path, valueRlp);
  }

  private static final class ShortFields {
    final Bytes path;
    final Bytes valueRlp;

    ShortFields(final Bytes path, final Bytes valueRlp) {
      this.path = path;
      this.valueRlp = valueRlp;
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
   * available. For short-node DIFF entries, {@link #changedKey()} and {@link
   * #changedShortNodeValue()} are available, and {@link #isShortNodeDiff()} identifies them.
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

    /**
     * Returns {@code true} iff this FULL entry's body is a 32-byte CAS reference ({@link
     * TrieNodeDiffCodec#HASH_REF} set) rather than inline node RLP.
     */
    public boolean isHashRef() {
      return (metadata & HASH_REF) != 0;
    }

    /**
     * Returns the 32-byte keccak256 CAS reference of a HASH_REF FULL entry.
     *
     * @return the referenced body's content hash
     * @throws IllegalStateException if this is not a HASH_REF entry, or the body is not exactly 32
     *     bytes (malformed entry)
     */
    public Bytes32 refHash() {
      if (!isFull() || !isHashRef()) {
        throw new IllegalStateException("refHash() called on a non-HASH_REF entry");
      }
      if (body.size() != 32) {
        throw new IllegalStateException(
            "malformed HASH_REF entry: expected 32-byte body, got " + body.size());
      }
      return Bytes32.wrap(body);
    }

    // ------------------------------------------------------------------
    // FULL-entry accessor
    // ------------------------------------------------------------------

    /**
     * Returns the full node RLP for a FULL entry.
     *
     * @return raw RLP bytes of the trie node
     * @throws IllegalStateException if this is a diff entry (i.e. {@link #isFull()} is {@code
     *     false}), or if this is a HASH_REF entry (use {@link #refHash()} and resolve via the CAS
     *     store instead)
     */
    public Bytes fullNode() {
      if (!isFull()) {
        throw new IllegalStateException(
            "fullNode() called on a diff entry; use diff accessors (Task 1.2–1.4)");
      }
      if (isHashRef()) {
        throw new IllegalStateException(
            "fullNode() called on a HASH_REF entry; resolve the body via the CAS store using"
                + " refHash()");
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
    // Short-node DIFF accessors (Task 1.3)
    // ------------------------------------------------------------------

    /**
     * Returns {@code true} iff this is a short-node diff entry ({@link
     * TrieNodeDiffCodec#ENTRY_FULL} clear, {@link TrieNodeDiffCodec#NODE_IS_BRANCH} clear, and not
     * a {@link TrieNodeDiffCodec#DELETION} tombstone — a tombstone also has both of those bits
     * clear but carries no short-node fields, so it must not be mistaken for a short-node diff).
     */
    public boolean isShortNodeDiff() {
      return !isFull() && !isBranchNode() && !isDeletion();
    }

    /**
     * Returns the new compact-encoded path bytes if {@link TrieNodeDiffCodec#KEY_CHANGED} is set,
     * otherwise {@link Optional#empty()}.
     *
     * <p>The returned bytes are the decoded item 0 payload of the new short node — the
     * compact-encoded path nibbles as written by {@code out.writeBytes(encodedPath)}.
     *
     * @return optional new path bytes
     * @throws IllegalStateException if called on a FULL entry or a branch DIFF entry
     */
    public Optional<Bytes> changedKey() {
      requireShortDiff("changedKey()");
      if ((metadata & KEY_CHANGED) == 0) {
        return Optional.empty();
      }
      // KEY_CHANGED field is the first framed field (2-byte big-endian length prefix).
      return Optional.of(readShortField(0));
    }

    /**
     * Returns the raw RLP of the new short-node value item if {@link
     * TrieNodeDiffCodec#VALUE_CHANGED} is set, otherwise {@link Optional#empty()}.
     *
     * <p>The returned bytes are the raw RLP of item 1 of the new short node, captured via {@link
     * RLPInput#readAsRlp()}{@code .raw()}. This is the same unit that {@code out.writeRaw()} would
     * write when reconstructing the node in Task 1.4.
     *
     * <p>This accessor is kept separate from {@link #changedValue()} (the branch terminal value
     * accessor) to avoid ambiguity: branch and short-node diff entries both may set VALUE_CHANGED
     * but store the field in incompatible formats.
     *
     * @return optional raw RLP bytes of the new value item
     * @throws IllegalStateException if called on a FULL entry or a branch DIFF entry
     */
    public Optional<Bytes> changedShortNodeValue() {
      requireShortDiff("changedShortNodeValue()");
      if ((metadata & VALUE_CHANGED) == 0) {
        return Optional.empty();
      }
      // If KEY_CHANGED is also set, the key field comes first; skip it.
      final int keyFieldSize = ((metadata & KEY_CHANGED) != 0) ? (2 + readShortFieldLength(0)) : 0;
      return Optional.of(readShortField(keyFieldSize));
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    private void requireBranchDiff(final String methodName) {
      if (isFull() || !isBranchNode()) {
        throw new IllegalStateException(
            methodName + " called on a non-branch DIFF entry; only valid for branch DIFF entries");
      }
    }

    private void requireShortDiff(final String methodName) {
      if (isFull() || isBranchNode() || isDeletion()) {
        throw new IllegalStateException(
            methodName
                + " called on a non-short-node DIFF entry (FULL, branch, or deletion tombstone);"
                + " only valid for short-node DIFF entries");
      }
    }

    /**
     * Reads a 2-byte big-endian length prefix from {@code body} at {@code offset} and returns the
     * field bytes that follow.
     */
    private Bytes readShortField(final int offset) {
      final int len = readShortFieldLength(offset);
      return body.slice(offset + 2, len);
    }

    /** Reads a 2-byte big-endian length from {@code body} at {@code offset} without advancing. */
    private int readShortFieldLength(final int offset) {
      final int hi = Byte.toUnsignedInt(body.get(offset));
      final int lo = Byte.toUnsignedInt(body.get(offset + 1));
      return (hi << 8) | lo;
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
