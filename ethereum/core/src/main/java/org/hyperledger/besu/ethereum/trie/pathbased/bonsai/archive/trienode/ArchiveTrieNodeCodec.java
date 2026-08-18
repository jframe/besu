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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.CREATION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.DELETION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.ENTRY_FULL;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.REPLACEMENT;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;

/**
 * Codec for {@link ArchiveTrieNodeEntry} instances. Provides methods to encode/decode entries and
 * reconstruct a node's bytes from a FULL entry and a list of DIFF entries.
 *
 * <p>DIFF entries are encoded as a sequence of binary COPY/SKIP/INSERT ops applied to the previous
 * node's bytes:
 *
 * <ul>
 *   <li>COPY(n) — emit n bytes from the old node at current old_pos, advance old_pos
 *   <li>SKIP(n) — advance old_pos by n, no output
 *   <li>INSERT(n) — followed by n bytes of new data, emit them (old_pos unchanged)
 * </ul>
 *
 * <p>After all ops, any remaining bytes in the old node are implicitly appended (zero-cost trailing
 * suffix). Op word is 2 bytes big-endian: bits[15:14] = type (00=COPY, 01=SKIP, 10=INSERT),
 * bits[13:0] = length (max 16383). This format is trie-structure agnostic: it works for MPT, PBT
 * (EIP-8297), or any future encoding without modification.
 *
 * <p>If the patch body would be at least as large as the new node, {@link #encodeDiff} falls back
 * to a REPLACEMENT entry (metadata {@code REPLACEMENT}, without {@code ENTRY_FULL}) bounding the
 * worst case. REPLACEMENT entries carry the full new-node bytes (accessible via {@link
 * ArchiveTrieNodeEntry#fullNode()}). Unlike standalone FULL entries, {@link
 * ArchiveTrieNodeEntry#isFull()} is {@code false} for REPLACEMENT entries, so readers that use
 * {@code isFull()} to detect FULL checkpoints will not stop at them. In the diff list passed to
 * {@link #reconstruct}, REPLACEMENT entries replace the current running node value.
 */
public final class ArchiveTrieNodeCodec {

  private static final int OP_COPY = 0;
  private static final int OP_SKIP = 1;
  private static final int OP_INSERT = 2;
  private static final int OP_MAX_LENGTH = 0x3FFF; // 14 bits

  private ArchiveTrieNodeCodec() {}

  /** Layout: {@code [ENTRY_FULL]} ‖ {@code nodeBytes}. */
  public static Bytes encodeFull(final Bytes nodeBytes) {
    Objects.requireNonNull(nodeBytes, "nodeBytes must not be null");
    return Bytes.concatenate(Bytes.of(ENTRY_FULL), nodeBytes);
  }

  /**
   * Encodes the diff from {@code oldNode} to {@code newNode} as a binary patch entry, or as a
   * REPLACEMENT entry when {@code oldNode} is null (creation), {@code newNode} is null (deletion),
   * or the patch body would be at least as large as the new node.
   */
  public static Bytes encodeDiff(final Bytes oldNode, final Bytes newNode) {
    if (oldNode == null && newNode == null) {
      throw new IllegalArgumentException("encodeDiff: both old and new nodes are null");
    } else if (oldNode == null) {
      return Bytes.concatenate(Bytes.of((byte) (ENTRY_FULL | CREATION)), newNode);
    } else if (newNode == null) {
      return Bytes.of(DELETION);
    }

    final Bytes patch = encodePatch(oldNode, newNode);
    if (patch.size() >= newNode.size()) {
      // Fallback: REPLACEMENT carries the full new node but does NOT set ENTRY_FULL, so readers
      // that use isFull() as a checkpoint signal (like ArchiveHistoryReader) do not stop here.
      return Bytes.concatenate(Bytes.of(REPLACEMENT), newNode);
    }
    return Bytes.concatenate(Bytes.of((byte) 0x00), patch);
  }

  public static ArchiveTrieNodeEntry decode(final Bytes entry) {
    Objects.requireNonNull(entry, "entry must not be null");
    if (entry.isEmpty()) {
      throw new IllegalArgumentException("Entry must be at least 1 byte (metadata byte)");
    }
    return new ArchiveTrieNodeEntry(entry.get(0), entry.slice(1));
  }

  /**
   * Reconstructs a node by applying each diff entry's patch (or replacement) to the base FULL node
   * in order.
   *
   * <p>REPLACEMENT entries in the diff list are handled by substituting their full node bytes as
   * the new running value, rather than applying a patch.
   *
   * @param fullEntry a FULL codec entry (from {@link #encodeFull}), not a DIFF or REPLACEMENT
   * @param diffEntries zero or more DIFF or REPLACEMENT entries (not standalone FULL, not deletion)
   *     in ascending block order
   * @return the reconstructed node bytes after all diffs are applied
   * @throws IllegalArgumentException if {@code fullEntry} is not FULL, or any diff entry is a
   *     standalone FULL or a deletion tombstone
   */
  public static Bytes reconstruct(final Bytes fullEntry, final List<Bytes> diffEntries) {
    Objects.requireNonNull(fullEntry, "fullEntry must not be null");
    Objects.requireNonNull(diffEntries, "diffEntries must not be null");
    final ArchiveTrieNodeEntry base = decode(fullEntry);
    if (!base.isFull()) {
      throw new IllegalArgumentException("reconstruct: fullEntry must be a FULL entry");
    }
    Bytes node = base.fullNode();
    for (final Bytes diffEntry : diffEntries) {
      final ArchiveTrieNodeEntry entry = decode(diffEntry);
      if (entry.isDeletion()) {
        throw new IllegalArgumentException(
            "reconstruct: diff list must not contain deletion entries");
      } else if (entry.isReplacement()) {
        node = entry.fullNode();
      } else if (entry.isFull()) {
        throw new IllegalArgumentException(
            "reconstruct: diff list must not contain standalone FULL entries");
      } else {
        node = applyPatch(node, entry.patchBody());
      }
    }
    return node;
  }

  // ---------------------------------------------------------------------------
  // Internal: patch encoding and application
  // ---------------------------------------------------------------------------

  /**
   * Produces a binary patch body encoding the single-region diff from {@code old} to {@code
   * newNode}. Finds the longest common prefix and suffix, then encodes the changed middle region as
   * COPY(prefix) + INSERT(newMid) + SKIP(oldMidLen). The implicit trailing COPY of the suffix costs
   * zero bytes.
   */
  private static Bytes encodePatch(final Bytes old, final Bytes newNode) {
    // Common prefix length
    int prefix = 0;
    while (prefix < old.size()
        && prefix < newNode.size()
        && old.get(prefix) == newNode.get(prefix)) {
      prefix++;
    }

    // Common suffix length (working inward from end, staying within the post-prefix region)
    int oldEnd = old.size();
    int newEnd = newNode.size();
    while (oldEnd > prefix && newEnd > prefix && old.get(oldEnd - 1) == newNode.get(newEnd - 1)) {
      oldEnd--;
      newEnd--;
    }
    // old[prefix..oldEnd) is the old middle; newNode[prefix..newEnd) is the new middle
    final int oldMidLen = oldEnd - prefix;
    final int newMidLen = newEnd - prefix;

    final List<Bytes> parts = new ArrayList<>(3);
    if (prefix > 0) {
      parts.add(encodeOp(OP_COPY, prefix));
    }
    if (newMidLen > 0) {
      parts.add(encodeOp(OP_INSERT, newMidLen));
      parts.add(newNode.slice(prefix, newMidLen));
    }
    if (oldMidLen > 0) {
      parts.add(encodeOp(OP_SKIP, oldMidLen));
    }
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /** Applies a binary patch body to {@code base}, producing the reconstructed node. */
  private static Bytes applyPatch(final Bytes base, final Bytes patchBody) {
    final List<Bytes> out = new ArrayList<>();
    int oldPos = 0;
    int patchPos = 0;

    while (patchPos + 1 < patchBody.size()) {
      final int hi = Byte.toUnsignedInt(patchBody.get(patchPos));
      final int lo = Byte.toUnsignedInt(patchBody.get(patchPos + 1));
      patchPos += 2;
      final int opType = (hi >> 6) & 0x3;
      final int length = ((hi & 0x3F) << 8) | lo;

      switch (opType) {
        case OP_COPY -> {
          out.add(base.slice(oldPos, length));
          oldPos += length;
        }
        case OP_SKIP -> oldPos += length;
        case OP_INSERT -> {
          out.add(patchBody.slice(patchPos, length));
          patchPos += length;
        }
        default -> throw new IllegalArgumentException("unknown patch op type: " + opType);
      }
    }

    if (patchPos != patchBody.size()) {
      throw new IllegalArgumentException(
          "patch body has a trailing unpaired byte at position " + patchPos);
    }

    // Implicit: copy remaining old bytes (the common suffix)
    if (oldPos < base.size()) {
      out.add(base.slice(oldPos));
    }

    return Bytes.concatenate(out.toArray(new Bytes[0]));
  }

  private static Bytes encodeOp(final int type, final int length) {
    if (length < 0 || length > OP_MAX_LENGTH) {
      throw new IllegalArgumentException(
          "patch op length out of 14-bit range [0, " + OP_MAX_LENGTH + "]: " + length);
    }
    final int word = (type << 14) | length;
    return Bytes.of((byte) ((word >> 8) & 0xFF), (byte) (word & 0xFF));
  }
}
