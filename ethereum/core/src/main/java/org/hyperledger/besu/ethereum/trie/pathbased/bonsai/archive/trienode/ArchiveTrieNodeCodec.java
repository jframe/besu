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

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.ENTRY_FULL;

import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;

/**
 * Low-level byte codec for {@link ArchiveTrieNodeEntry} instances.
 *
 * <p>This class provides {@link #encodeFull} and {@link #decode} — the two primitives used by
 * {@code NodeLogCodec}. The semantic DIFF format (COPY/SKIP/INSERT/REPLACE byte-patch) previously
 * implemented here now lives in {@code NodeLogCodec}.
 */
public final class ArchiveTrieNodeCodec {

  private ArchiveTrieNodeCodec() {}

  /** Layout: {@code [ENTRY_FULL]} ‖ {@code nodeBytes}. */
  public static Bytes encodeFull(final Bytes nodeBytes) {
    Objects.requireNonNull(nodeBytes, "nodeBytes must not be null");
    return Bytes.concatenate(Bytes.of(ENTRY_FULL), nodeBytes);
  }

  public static ArchiveTrieNodeEntry decode(final Bytes entry) {
    Objects.requireNonNull(entry, "entry must not be null");
    if (entry.isEmpty()) {
      throw new IllegalArgumentException("Entry must be at least 1 byte (metadata byte)");
    }
    return new ArchiveTrieNodeEntry(entry.get(0), entry.slice(1));
  }
}
