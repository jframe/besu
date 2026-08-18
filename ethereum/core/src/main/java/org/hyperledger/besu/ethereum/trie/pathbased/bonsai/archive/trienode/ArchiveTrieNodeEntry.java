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

import org.apache.tuweni.bytes.Bytes;

/**
 * The decoded, typed view of one archived trie-node history entry, produced by {@link
 * ArchiveTrieNodeCodec#decode(Bytes)}. One of four shapes — FULL ({@link #fullNode()}), a binary
 * patch DIFF ({@link #patchBody()}), a REPLACEMENT ({@link #fullNode()} when {@link
 * #isReplacement()}), or a deletion tombstone — selected by the predicates below. Callers must
 * check the relevant predicate before calling a shape-specific accessor; calling the wrong one
 * throws {@link IllegalStateException}.
 *
 * <p>REPLACEMENT entries are produced by {@link ArchiveTrieNodeCodec#encodeDiff} as a fallback when
 * the binary patch would be at least as large as the new node. They carry the full new-node bytes
 * (accessible via {@link #fullNode()}), but unlike standalone FULL entries they are not treated as
 * FULL checkpoints by readers — callers that inspect {@link #isFull()} will see {@code false}, so
 * they flow through reconstruction pipelines as diff-list entries rather than base checkpoints.
 */
public final class ArchiveTrieNodeEntry {

  public static final byte ENTRY_FULL = 0b0000_0001;

  /**
   * Set on entries produced by {@link ArchiveTrieNodeCodec#encodeDiff} when the binary patch would
   * be at least as large as the new node. The {@link #ENTRY_FULL} bit is NOT set, so {@link
   * #isFull()} returns {@code false} and readers do not treat these as FULL checkpoints. The node
   * bytes are still accessible via {@link #fullNode()}.
   */
  static final byte REPLACEMENT = 0b0000_0010;

  public static final byte CREATION = 0b0001_0000;
  public static final byte DELETION = 0b0010_0000;

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

  /**
   * True when this entry was produced by {@link ArchiveTrieNodeCodec#encodeDiff} falling back to a
   * full-node encoding because the binary patch was at least as large as the new node. The full new
   * node bytes are accessible via {@link #fullNode()}. Unlike standalone FULL entries, {@link
   * #isFull()} returns {@code false} for REPLACEMENT entries so they do not act as FULL
   * checkpoints.
   */
  public boolean isReplacement() {
    return (metadata & REPLACEMENT) != 0;
  }

  /**
   * Returns the full node bytes. Valid when {@link #isFull()} or {@link #isReplacement()} is true.
   *
   * @throws IllegalStateException if called on a diff or deletion entry
   */
  public Bytes fullNode() {
    if (!isFull() && !isReplacement()) {
      throw new IllegalStateException("fullNode() called on a non-full, non-replacement entry");
    }
    return body;
  }

  /**
   * Returns the raw binary patch body (COPY/SKIP/INSERT op sequence). Only valid when {@link
   * #isFull()}, {@link #isReplacement()}, and {@link #isDeletion()} are all false.
   *
   * @throws IllegalStateException if called on a full, replacement, or deletion entry
   */
  public Bytes patchBody() {
    if (isFull() || isReplacement() || isDeletion()) {
      throw new IllegalStateException("patchBody() called on a non-diff entry");
    }
    return body;
  }
}
