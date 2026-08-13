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

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.BRANCH_CHILDREN;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.CREATION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.DELETION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.ENTRY_FULL;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.KEY_CHANGED;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.NODE_IS_BRANCH;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.SINGLE_CHILD_CHANGED;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.VALUE_CHANGED;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;

/**
 * Codec for {@link ArchiveTrieNodeEntry} instances. Provides methods to encode/decode entries and
 * reconstruct a node's RLP from a FULL entry and a list of DIFF entries.
 *
 * <p>Field framing: the short-node path carries a 2-byte big-endian length prefix and the branch
 * terminal value a 1-byte one; everything else is self-delimiting raw RLP or fixed width
 * (single-child index, child mask). Being last in the entry is not on its own enough to drop a
 * prefix — see {@code frameBranchValue}.
 *
 * <p>The branch terminal value never actually occurs here: Bonsai account and storage tries are
 * keyed by 32-byte hashes, so all keys are 64 nibbles, no key is a proper prefix of another, and no
 * key terminates at a branch. The path is kept for correctness. Variable-length-keyed tries
 * (transaction/receipt) do use that slot, but never reach this codec.
 */
public final class ArchiveTrieNodeCodec {
  private static final int SHORT_NODE_ARITY = 2;
  private static final int BRANCH_NODE_ARITY = BRANCH_CHILDREN + 1;
  private static final int SHORT_NODE_PATH_MAX_LENGTH = 0xFFFF;

  private ArchiveTrieNodeCodec() {}

  /** Layout: {@code [ENTRY_FULL]} ‖ {@code nodeRlp}. */
  public static Bytes encodeFull(final Bytes nodeRlp) {
    Objects.requireNonNull(nodeRlp, "nodeRlp must not be null");
    return Bytes.concatenate(Bytes.of(ENTRY_FULL), nodeRlp);
  }

  public static Bytes encodeDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
    final boolean created = oldNodeRlp == null;
    final boolean deleted = newNodeRlp == null;
    if (created && deleted) {
      throw new IllegalArgumentException("encodeDiff: both old and new node RLPs are null");
    } else if (created) {
      return Bytes.concatenate(Bytes.of((byte) (ENTRY_FULL | CREATION)), newNodeRlp);
    } else if (deleted) {
      return Bytes.of(DELETION);
    }

    final int arity = nodeArity(newNodeRlp);
    if (arity != nodeArity(oldNodeRlp)) {
      return encodeFull(newNodeRlp);
    } else if (arity == BRANCH_NODE_ARITY) {
      return encodeBranchDiff(oldNodeRlp, newNodeRlp);
    } else {
      return encodeShortDiff(oldNodeRlp, newNodeRlp);
    }
  }

  public static ArchiveTrieNodeEntry decode(final Bytes entry) {
    Objects.requireNonNull(entry, "entry must not be null");
    if (entry.isEmpty()) {
      throw new IllegalArgumentException("Entry must be at least 1 byte (metadata byte)");
    }
    return new ArchiveTrieNodeEntry(entry.get(0), entry.slice(1));
  }

  public static Bytes reconstruct(final Bytes fullEntry, final List<Bytes> diffEntries) {
    Objects.requireNonNull(fullEntry, "fullEntry must not be null");
    Objects.requireNonNull(diffEntries, "diffEntries must not be null");
    final ArchiveTrieNodeEntry base = decode(fullEntry);
    if (!base.isFull()) {
      throw new IllegalArgumentException("reconstruct: fullEntry must be a FULL entry");
    }
    final Bytes baseNode = base.fullNode();
    return nodeArity(baseNode) == BRANCH_NODE_ARITY
        ? reconstructBranch(baseNode, diffEntries)
        : reconstructShort(baseNode, diffEntries);
  }

  private static Bytes encodeBranchDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
    final BranchFields oldFields = parseBranchFields(oldNodeRlp);
    final BranchFields newFields = parseBranchFields(newNodeRlp);

    int childMask = 0;
    int changedCount = 0;
    int soleChangedIndex = -1;
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if (!oldFields.children()[i].equals(newFields.children()[i])) {
        childMask |= (1 << i);
        changedCount++;
        soleChangedIndex = i;
      }
    }
    final boolean valueChanged = !oldFields.value().equals(newFields.value());

    byte metadata = NODE_IS_BRANCH;
    if (valueChanged) metadata |= VALUE_CHANGED;
    if (changedCount == 1) metadata |= SINGLE_CHILD_CHANGED;

    final List<Bytes> parts = new ArrayList<>();
    if (changedCount == 1) {
      // Common case: a single key update touches exactly one child slot in each branch node
      // along its path — encode the index directly instead of spending 2 bytes on a bitmask.
      parts.add(Bytes.of(metadata));
      parts.add(Bytes.of((byte) soleChangedIndex));
      parts.add(newFields.children()[soleChangedIndex]); // raw RLP, self-delimiting
    } else {
      parts.add(Bytes.of(metadata));
      parts.add(Bytes.of((byte) ((childMask >> 8) & 0xFF), (byte) (childMask & 0xFF)));
      for (int i = 0; i < BRANCH_CHILDREN; i++) {
        if ((childMask & (1 << i)) != 0) {
          parts.add(newFields.children()[i]); // raw RLP, self-delimiting
        }
      }
    }
    if (valueChanged) parts.add(frameBranchValue(newFields.value()));
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /**
   * Frames the branch terminal value with a 1-byte length prefix. The prefix is not redundant
   * despite the value being last: {@link org.hyperledger.besu.ethereum.rlp.RLPInput#readAsRlp()}
   * eagerly prepares the item *after* the one it reads, so a preceding child ref would choke on raw
   * value bytes that do not happen to start with a valid RLP header.
   */
  private static Bytes frameBranchValue(final Bytes value) {
    final int len = value.size();
    if (len > 255) {
      throw new IllegalArgumentException("Branch value too large for 1-byte length prefix: " + len);
    }
    return Bytes.concatenate(Bytes.of((byte) len), value);
  }

  private static Bytes encodeShortDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
    final ShortFields oldFields = parseShortFields(oldNodeRlp);
    final ShortFields newFields = parseShortFields(newNodeRlp);
    final boolean keyChanged = !oldFields.path.equals(newFields.path);
    final boolean valueChanged = !oldFields.valueRlp.equals(newFields.valueRlp);

    byte metadata = 0;
    if (keyChanged) metadata |= KEY_CHANGED;
    if (valueChanged) metadata |= VALUE_CHANGED;

    final List<Bytes> parts = new ArrayList<>();
    parts.add(Bytes.of(metadata));
    if (keyChanged) parts.add(frameShortField(newFields.path));
    if (valueChanged) parts.add(newFields.valueRlp); // raw RLP, self-delimiting, no length prefix
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /**
   * Frames the short-node path (decoded payload, not self-delimiting) with a 2-byte length prefix.
   */
  private static Bytes frameShortField(final Bytes field) {
    final int len = field.size();
    if (len > SHORT_NODE_PATH_MAX_LENGTH) {
      throw new IllegalArgumentException(
          "Short-node path too large for 2-byte length prefix: " + len);
    }
    return Bytes.concatenate(Bytes.of((byte) ((len >> 8) & 0xFF), (byte) (len & 0xFF)), field);
  }

  private static Bytes reconstructBranch(final Bytes baseNode, final List<Bytes> diffEntries) {
    final BranchFields base = parseBranchFields(baseNode);
    final Bytes[] children = Arrays.copyOf(base.children(), BRANCH_CHILDREN);
    Bytes value = base.value();
    for (final Bytes diffEntry : diffEntries) {
      final ArchiveTrieNodeEntry entry = decode(diffEntry);
      requireDiffEntry(entry);
      if (!entry.isBranchNode()) {
        throw new IllegalArgumentException(
            "reconstruct type mismatch: base is branch, diff is not");
      }
      for (final Map.Entry<Integer, Bytes> e : entry.changedChildRefs().entrySet()) {
        children[e.getKey()] = e.getValue();
      }
      value = entry.changedValue().orElse(value);
    }

    final BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    rlpOutput.startList();
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      rlpOutput.writeRaw(children[i]);
    }
    rlpOutput.writeBytes(value);
    rlpOutput.endList();
    return rlpOutput.encoded();
  }

  private static Bytes reconstructShort(final Bytes baseNode, final List<Bytes> diffEntries) {
    final ShortFields base = parseShortFields(baseNode);
    Bytes path = base.path;
    Bytes valueRlp = base.valueRlp;
    for (final Bytes diffEntry : diffEntries) {
      final ArchiveTrieNodeEntry entry = decode(diffEntry);
      requireDiffEntry(entry);
      if (entry.isBranchNode()) {
        throw new IllegalArgumentException(
            "reconstruct type mismatch: base is short, diff is branch");
      }
      path = entry.changedKey().orElse(path);
      valueRlp = entry.changedShortNodeValue().orElse(valueRlp);
    }

    final BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    rlpOutput.startList();
    rlpOutput.writeBytes(path);
    rlpOutput.writeRaw(valueRlp);
    rlpOutput.endList();
    return rlpOutput.encoded();
  }

  private static void requireDiffEntry(final ArchiveTrieNodeEntry entry) {
    if (entry.isFull() || entry.isDeletion()) {
      throw new IllegalArgumentException("reconstruct expects DIFF entries only");
    }
  }

  private static int nodeArity(final Bytes nodeRlp) {
    final RLPInput in = RLP.input(nodeRlp);
    final int count = in.enterList();
    if (count != SHORT_NODE_ARITY && count != BRANCH_NODE_ARITY) {
      throw new IllegalArgumentException(
          "Expected a "
              + SHORT_NODE_ARITY
              + "-item short node or "
              + BRANCH_NODE_ARITY
              + "-item branch node RLP list, got "
              + count
              + " items");
    }
    return count;
  }

  private static BranchFields parseBranchFields(final Bytes nodeRlp) {
    final RLPInput in = RLP.input(nodeRlp);
    final int count = in.enterList();
    if (count != BRANCH_NODE_ARITY) {
      throw new IllegalArgumentException(
          "Expected " + BRANCH_NODE_ARITY + "-item branch node RLP list, got " + count);
    }
    final Bytes[] children = new Bytes[BRANCH_CHILDREN];
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      children[i] = in.readAsRlp().raw();
    }
    final Bytes value = in.nextIsNull() ? readNullAsEmpty(in) : in.readBytes();
    in.leaveList();
    return new BranchFields(children, value);
  }

  private static Bytes readNullAsEmpty(final RLPInput in) {
    in.skipNext();
    return Bytes.EMPTY;
  }

  private record BranchFields(Bytes[] children, Bytes value) {}

  private static ShortFields parseShortFields(final Bytes nodeRlp) {
    final RLPInput in = RLP.input(nodeRlp);
    final int count = in.enterList();
    if (count != 2) {
      throw new IllegalArgumentException("Expected 2-item short node RLP list, got " + count);
    }
    final Bytes path = in.readBytes();
    final Bytes valueRlp = in.readAsRlp().raw();
    in.leaveList();
    return new ShortFields(path, valueRlp);
  }

  private record ShortFields(Bytes path, Bytes valueRlp) {}
}
