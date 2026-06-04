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

import java.util.Objects;

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
 *   <li><b>DIFF</b> ({@link #ENTRY_FULL} clear): delta-compressed representation. Parsing is
 *       implemented in Tasks 1.2–1.4; calling {@link Decoded#fullNode()} on a diff entry throws
 *       {@link IllegalStateException}.
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

  // -------------------------------------------------------------------------
  // Decoding
  // -------------------------------------------------------------------------

  /**
   * Decodes an entry produced by {@link #encodeFull} (or, in future tasks, a diff encoder).
   *
   * @param entry the raw entry bytes; must not be {@code null} and must be at least 1 byte long
   * @return a {@link Decoded} value exposing the metadata and (for FULL entries) the node RLP
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
   * For FULL entries, {@link #fullNode()} returns the node RLP. Diff accessors ({@code
   * changedChildIndices}, {@code newChildHashes}, {@code changedKey}, {@code changedValue}) are
   * added in Tasks 1.2–1.4.
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
    // Diff accessors — TODO(Task 1.2–1.4): implement when diff encoding is added.
    // ------------------------------------------------------------------

    // TODO(Task 1.2): changedChildIndices() — list of branch child indices that changed
    // TODO(Task 1.2): newChildHashes() — map of child index → new child hash
    // TODO(Task 1.3): changedKey() — new path segment for short-node key changes
    // TODO(Task 1.3): changedValue() — new embedded value for short/branch value changes
  }
}
