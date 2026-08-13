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

import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * The decoded, typed view of one archived trie-node history entry, produced by {@link
 * ArchiveTrieNodeCodec#decode(Bytes)}. One of three shapes — FULL ({@link #fullNode()}), a branch
 * or short-node DIFF, or a deletion tombstone — selected by the predicates below. Callers must
 * check the relevant predicate before calling a shape-specific accessor; calling the wrong one
 * throws {@link IllegalStateException}.
 */
public final class ArchiveTrieNodeEntry {

  /** Metadata flags for the entry's type */
  public static final byte ENTRY_FULL = 0b0000_0001;

  public static final byte NODE_IS_BRANCH = 0b0000_0010;
  public static final byte KEY_CHANGED = 0b0000_0100;
  public static final byte VALUE_CHANGED = 0b0000_1000;
  public static final byte CREATION = 0b0001_0000;
  public static final byte DELETION = 0b0010_0000;
  public static final byte SINGLE_CHILD_CHANGED = 0b0100_0000;

  static final int BRANCH_CHILDREN = 16;

  private final byte metadata;
  private final Bytes body;

  ArchiveTrieNodeEntry(final byte metadata, final Bytes body) {
    this.metadata = metadata;
    this.body = body;
  }

  public boolean isFull() {
    return (metadata & ENTRY_FULL) != 0;
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

  public Map<Integer, Bytes> changedChildRefs() {
    requireBranchDiff("changedChildRefs()");
    if (isSingleChildChanged()) {
      final int index = Byte.toUnsignedInt(body.get(0));
      return Map.of(index, readRlpItem(1));
    }

    final int mask = readChildMask();
    int offset = 2;
    final Map<Integer, Bytes> result = new LinkedHashMap<>();
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if ((mask & (1 << i)) != 0) {
        final Bytes childRlp = readRlpItem(offset);
        result.put(i, childRlp);
        offset += childRlp.size();
      }
    }
    return Collections.unmodifiableMap(result);
  }

  public Optional<Bytes> changedValue() {
    requireBranchDiff("changedValue()");
    if ((metadata & VALUE_CHANGED) == 0) {
      return Optional.empty();
    }

    final int offset = offsetAfterChildRefs();
    final int len = Byte.toUnsignedInt(body.get(offset));
    return Optional.of(body.slice(offset + 1, len));
  }

  private void requireBranchDiff(final String methodName) {
    if (isFull() || !isBranchNode()) {
      throw new IllegalStateException(methodName + " called on a non-branch DIFF entry");
    }
  }

  private boolean isSingleChildChanged() {
    return (metadata & SINGLE_CHILD_CHANGED) != 0;
  }

  private int readChildMask() {
    final int hi = Byte.toUnsignedInt(body.get(0));
    final int lo = Byte.toUnsignedInt(body.get(1));
    return (hi << 8) | lo;
  }

  private Bytes readRlpItem(final int offset) {
    // Not readAsRlp(): it peeks past the item and can misread a trailing length prefix as RLP.
    final int itemSize = RLP.input(body.slice(offset)).currentSize();
    return body.slice(offset, itemSize);
  }

  private int offsetAfterChildRefs() {
    if (isSingleChildChanged()) {
      return 1 + readRlpItem(1).size();
    }

    final int mask = readChildMask();
    int offset = 2;
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if ((mask & (1 << i)) != 0) {
        offset += readRlpItem(offset).size();
      }
    }
    return offset;
  }

  public Optional<Bytes> changedKey() {
    requireShortDiff("changedKey()");
    if ((metadata & KEY_CHANGED) == 0) {
      return Optional.empty();
    }

    return Optional.of(readShortField());
  }

  public Optional<Bytes> changedShortNodeValue() {
    requireShortDiff("changedShortNodeValue()");
    if ((metadata & VALUE_CHANGED) == 0) {
      return Optional.empty();
    }

    // Self-delimiting RLP with nothing following, so no length prefix or offset needed
    final int keyFieldSize = ((metadata & KEY_CHANGED) != 0) ? (2 + readShortFieldLength()) : 0;
    return Optional.of(readRlpItem(keyFieldSize));
  }

  private void requireShortDiff(final String methodName) {
    if (isFull() || isBranchNode() || isDeletion()) {
      throw new IllegalStateException(methodName + " called on a non-short-node DIFF entry");
    }
  }

  private Bytes readShortField() {
    final int len = readShortFieldLength();
    return body.slice(2, len);
  }

  private int readShortFieldLength() {
    final int hi = Byte.toUnsignedInt(body.get(0));
    final int lo = Byte.toUnsignedInt(body.get(1));
    return (hi << 8) | lo;
  }
}
