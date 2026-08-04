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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * The decoded, typed view of one archived trie-node history entry, produced by {@link
 * ArchiveTrieNodeCodec#decode(Bytes)}. An instance is one of three shapes, distinguished by the
 * predicates below: FULL (the complete node RLP, via {@link #fullNode()}), a branch or short-node
 * DIFF (a structural delta against the prior version), or a deletion tombstone (no body at all).
 * Only the codec constructs instances and only it knows which accessors are valid for a given
 * metadata byte — callers must check the relevant predicate before calling a shape-specific
 * accessor; calling the wrong one throws {@link IllegalStateException}.
 */
public final class ArchiveTrieNodeEntry {

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
  static final int BRANCH_CHILDREN = 16;

  private final byte metadata;
  private final Bytes body;

  ArchiveTrieNodeEntry(final byte metadata, final Bytes body) {
    this.metadata = metadata;
    this.body = body;
  }

  public byte metadata() {
    return metadata;
  }

  public boolean isFull() {
    return (metadata & ENTRY_FULL) != 0;
  }

  public boolean isCreation() {
    return (metadata & CREATION) != 0;
  }

  public boolean isDeletion() {
    return (metadata & DELETION) != 0;
  }

  public boolean isBranchNode() {
    return (metadata & NODE_IS_BRANCH) != 0;
  }

  public Bytes fullNode() {
    if (!isFull()) {
      throw new IllegalStateException("fullNode() called on a diff entry");
    }
    return body;
  }

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

  public Map<Integer, Bytes> changedChildRefs() {
    requireBranchDiff("changedChildRefs()");
    final int mask = readChildMask();
    int offset = 2;
    final Map<Integer, Bytes> result = new LinkedHashMap<>();
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if ((mask & (1 << i)) != 0) {
        final int len = Byte.toUnsignedInt(body.get(offset));
        offset += 1;
        result.put(i, body.slice(offset, len));
        offset += len;
      }
    }
    return Collections.unmodifiableMap(result);
  }

  public Optional<Bytes> changedValue() {
    requireBranchDiff("changedValue()");
    if ((metadata & VALUE_CHANGED) == 0) {
      return Optional.empty();
    }
    int offset = offsetAfterChildRefs(readChildMask());
    final int len = Byte.toUnsignedInt(body.get(offset));
    offset += 1;
    return Optional.of(body.slice(offset, len));
  }

  private void requireBranchDiff(final String methodName) {
    if (isFull() || !isBranchNode()) {
      throw new IllegalStateException(methodName + " called on a non-branch DIFF entry");
    }
  }

  private int readChildMask() {
    final int hi = Byte.toUnsignedInt(body.get(0));
    final int lo = Byte.toUnsignedInt(body.get(1));
    return (hi << 8) | lo;
  }

  private int offsetAfterChildRefs(final int mask) {
    int offset = 2;
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if ((mask & (1 << i)) != 0) {
        final int len = Byte.toUnsignedInt(body.get(offset));
        offset += 1 + len;
      }
    }
    return offset;
  }

  public boolean isShortNodeDiff() {
    return !isFull() && !isBranchNode() && !isDeletion();
  }

  public Optional<Bytes> changedKey() {
    requireShortDiff("changedKey()");
    if ((metadata & KEY_CHANGED) == 0) {
      return Optional.empty();
    }
    return Optional.of(readShortField(0));
  }

  public Optional<Bytes> changedShortNodeValue() {
    requireShortDiff("changedShortNodeValue()");
    if ((metadata & VALUE_CHANGED) == 0) {
      return Optional.empty();
    }
    final int keyFieldSize = ((metadata & KEY_CHANGED) != 0) ? (2 + readShortFieldLength(0)) : 0;
    return Optional.of(readShortField(keyFieldSize));
  }

  private void requireShortDiff(final String methodName) {
    if (isFull() || isBranchNode() || isDeletion()) {
      throw new IllegalStateException(methodName + " called on a non-short-node DIFF entry");
    }
  }

  private Bytes readShortField(final int offset) {
    final int len = readShortFieldLength(offset);
    return body.slice(offset + 2, len);
  }

  private int readShortFieldLength(final int offset) {
    final int hi = Byte.toUnsignedInt(body.get(offset));
    final int lo = Byte.toUnsignedInt(body.get(offset + 1));
    return (hi << 8) | lo;
  }
}
