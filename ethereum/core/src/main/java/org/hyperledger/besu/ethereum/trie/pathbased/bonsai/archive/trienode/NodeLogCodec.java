/*
 * Copyright contributors to Besu.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Semantic DIFF encode/reconstruct for trie nodes via {@link NodeCodecAdapter}.
 *
 * <p>Mutations are encoded as an op-stream wire format. Each op begins with a 1-byte header:
 * bits[7:6] = opType (0=CHILD_CHANGE, 1=VALUE_CHANGE, 2=PATH_CHANGE, 3=TYPE_CHANGE), bits[5:0] =
 * field (child position for CHILD_CHANGE, 0 for VALUE/PATH_CHANGE, NodeType ordinal for
 * TYPE_CHANGE). CHILD_CHANGE and VALUE_CHANGE are followed by a presence byte (0=absent, 1=present)
 * and, if present, a 2-byte big-endian length followed by that many bytes. PATH_CHANGE is followed
 * directly by a 2-byte length and bytes (path is never null). TYPE_CHANGE carries its value in the
 * field bits and has no further payload.
 *
 * <p>Two guards prevent incorrect or bloated entries:
 *
 * <ul>
 *   <li><b>Size guard:</b> if the mutation body is as large as the new node, a FULL entry is
 *       stored.
 *   <li><b>Correctness guard:</b> the mutations are re-applied and the result is compared to the
 *       original new node; if they differ, a FULL entry is stored.
 * </ul>
 */
public final class NodeLogCodec {

  private static final int OP_CHILD = 0;
  private static final int OP_VALUE = 1;
  private static final int OP_PATH = 2;
  private static final int OP_TYPE = 3;
  private static final int OP_TYPE_SHIFT = 6;
  private static final int FIELD_MASK = 0x3F;

  private NodeLogCodec() {}

  /**
   * Encodes a trie-node transition as a codec entry.
   *
   * <ul>
   *   <li>{@code oldNode == null} — creation: FULL|CREATION entry.
   *   <li>{@code newNode == null} — deletion tombstone.
   *   <li>otherwise — semantic DIFF with size and correctness guards; falls back to FULL if needed.
   * </ul>
   *
   * @param adapter the codec adapter for the node format
   * @param oldNode the previous node bytes, or null for creation
   * @param newNode the new node bytes, or null for deletion
   * @return the encoded entry bytes
   */
  public static Bytes encodeDiff(
      final NodeCodecAdapter adapter, final Bytes oldNode, final Bytes newNode) {
    if (oldNode == null && newNode == null) {
      throw new IllegalArgumentException("encodeDiff: both old and new nodes are null");
    }
    final int tag = adapter.formatTag();
    if (oldNode == null) {
      // Creation: FULL | CREATION, tagged.
      // encodeFull only sets ENTRY_FULL; we also need CREATION, so build the metadata directly.
      final byte meta =
          (byte)
              (ArchiveTrieNodeEntry.ENTRY_FULL
                  | ArchiveTrieNodeEntry.CREATION
                  | (tag << ArchiveTrieNodeEntry.FORMAT_SHIFT));
      return Bytes.concatenate(Bytes.of(meta), newNode);
    }
    if (newNode == null) {
      // Deletion tombstone: single byte
      return Bytes.of(
          (byte) (ArchiveTrieNodeEntry.DELETION | (tag << ArchiveTrieNodeEntry.FORMAT_SHIFT)));
    }

    final NodeLog priorModel = adapter.parse(oldNode);
    final NodeLog nextModel = adapter.parse(newNode);
    final List<NodeMutation> mutations = NodeLogDiffer.diff(priorModel, nextModel);
    final Bytes body = encodeMutations(mutations);

    // Size guard: never store a diff larger than or equal to the full node.
    if (body.size() >= newNode.size()) {
      return prependTag(tag, ArchiveTrieNodeCodec.encodeFull(newNode));
    }

    // Correctness guard: re-encode to verify byte-exact round-trip.
    final Bytes reproduced = adapter.encode(NodeLogDiffer.apply(priorModel, mutations));
    if (!reproduced.equals(newNode)) {
      return prependTag(tag, ArchiveTrieNodeCodec.encodeFull(newNode));
    }

    final byte metadata =
        (byte) (ArchiveTrieNodeEntry.ENTRY_DIFF | (tag << ArchiveTrieNodeEntry.FORMAT_SHIFT));
    return Bytes.concatenate(Bytes.of(metadata), body);
  }

  /**
   * Reconstructs the node bytes from a FULL base entry and a sequence of DIFF entries.
   *
   * @param fullEntry the FULL codec entry for the oldest version
   * @param diffEntries ordered DIFF entries to apply in sequence
   * @return the reconstructed node bytes after all mutations are applied
   */
  public static Bytes reconstruct(final Bytes fullEntry, final List<Bytes> diffEntries) {
    final ArchiveTrieNodeEntry base = ArchiveTrieNodeCodec.decode(fullEntry);
    if (!base.isFull()) {
      throw new IllegalArgumentException("reconstruct: fullEntry must be FULL");
    }
    NodeCodecAdapter adapter = NodeCodecAdapters.byTag(base.formatTag());
    NodeLog model = adapter.parse(base.fullNode());
    for (final Bytes diffEntry : diffEntries) {
      final ArchiveTrieNodeEntry entry = ArchiveTrieNodeCodec.decode(diffEntry);
      if (entry.isDeletion()) {
        throw new IllegalArgumentException("reconstruct: diff list has a deletion");
      }
      if (entry.isFull()) {
        throw new IllegalArgumentException("reconstruct: diff list has a standalone FULL");
      }
      adapter = NodeCodecAdapters.byTag(entry.formatTag());
      model = NodeLogDiffer.apply(model, decodeMutations(entry.patchBody()));
    }
    return adapter.encode(model);
  }

  /**
   * ORs the format tag into the metadata byte of an existing FULL entry without clearing other
   * bits.
   */
  private static Bytes prependTag(final int tag, final Bytes fullEntry) {
    final byte meta =
        (byte) (Byte.toUnsignedInt(fullEntry.get(0)) | (tag << ArchiveTrieNodeEntry.FORMAT_SHIFT));
    return Bytes.concatenate(Bytes.of(meta), fullEntry.slice(1));
  }

  /**
   * Encodes a list of mutations as an op-stream wire body.
   *
   * @param mutations the mutations to encode
   * @return the encoded bytes
   */
  static Bytes encodeMutations(final List<NodeMutation> mutations) {
    final List<Bytes> parts = new ArrayList<>();
    for (final NodeMutation m : mutations) {
      if (m instanceof NodeMutation.ChildChange c) {
        parts.add(header(OP_CHILD, c.pos()));
        appendOptional(parts, c.next());
      } else if (m instanceof NodeMutation.ValueChange v) {
        parts.add(header(OP_VALUE, 0));
        appendOptional(parts, v.next());
      } else if (m instanceof NodeMutation.PathChange p) {
        parts.add(header(OP_PATH, 0));
        appendBytes(parts, p.next()); // path is never null
      } else if (m instanceof NodeMutation.TypeChange t) {
        parts.add(header(OP_TYPE, t.next().ordinal())); // value in field bits; no payload
      }
    }
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /**
   * Decodes an op-stream wire body back into a list of mutations.
   *
   * @param body the encoded bytes
   * @return the decoded mutations
   */
  static List<NodeMutation> decodeMutations(final Bytes body) {
    final List<NodeMutation> ops = new ArrayList<>();
    int pos = 0;
    while (pos < body.size()) {
      final int hdr = Byte.toUnsignedInt(body.get(pos++));
      final int opType = hdr >> OP_TYPE_SHIFT;
      final int field = hdr & FIELD_MASK;
      switch (opType) {
        case OP_CHILD -> {
          final int consumed = optionalConsumed(body, pos);
          final Optional<Bytes> next = readOptional(body, pos);
          pos += consumed;
          ops.add(new NodeMutation.ChildChange(field, next));
        }
        case OP_VALUE -> {
          final int consumed = optionalConsumed(body, pos);
          final Optional<Bytes> next = readOptional(body, pos);
          pos += consumed;
          ops.add(new NodeMutation.ValueChange(next));
        }
        case OP_PATH -> {
          final int len =
              (Byte.toUnsignedInt(body.get(pos)) << 8) | Byte.toUnsignedInt(body.get(pos + 1));
          pos += 2;
          final Bytes next = body.slice(pos, len);
          pos += len;
          ops.add(new NodeMutation.PathChange(next));
        }
        case OP_TYPE -> ops.add(new NodeMutation.TypeChange(NodeLog.NodeType.values()[field]));
        default -> throw new IllegalArgumentException("unknown NodeLog op type: " + opType);
      }
    }
    return ops;
  }

  private static Bytes header(final int opType, final int field) {
    return Bytes.of((byte) ((opType << OP_TYPE_SHIFT) | (field & FIELD_MASK)));
  }

  /** Writes a presence byte, then [len:2][bytes] if present. */
  private static void appendOptional(final List<Bytes> parts, final Optional<Bytes> value) {
    if (value.isEmpty()) {
      parts.add(Bytes.of((byte) 0));
    } else {
      parts.add(Bytes.of((byte) 1));
      appendBytes(parts, value.get());
    }
  }

  /** Writes [len:2][bytes]. Throws if data exceeds the 2-byte length field capacity. */
  private static void appendBytes(final List<Bytes> parts, final Bytes data) {
    if (data.size() > 0xFFFF) {
      throw new IllegalArgumentException(
          "op data too large for 2-byte length field: " + data.size());
    }
    parts.add(Bytes.of((byte) (data.size() >> 8), (byte) (data.size() & 0xFF)));
    parts.add(data);
  }

  /** Returns the Optional value at body[pos], which starts with a presence byte. */
  private static Optional<Bytes> readOptional(final Bytes body, final int pos) {
    if (Byte.toUnsignedInt(body.get(pos)) == 0) {
      return Optional.empty();
    }
    final int len =
        (Byte.toUnsignedInt(body.get(pos + 1)) << 8) | Byte.toUnsignedInt(body.get(pos + 2));
    return Optional.of(body.slice(pos + 3, len));
  }

  /**
   * Returns the number of bytes consumed by the optional field starting at body[pos] (presence byte
   * + optional [len:2][bytes]).
   */
  private static int optionalConsumed(final Bytes body, final int pos) {
    if (Byte.toUnsignedInt(body.get(pos)) == 0) {
      return 1;
    }
    final int len =
        (Byte.toUnsignedInt(body.get(pos + 1)) << 8) | Byte.toUnsignedInt(body.get(pos + 2));
    return 1 + 2 + len;
  }
}
