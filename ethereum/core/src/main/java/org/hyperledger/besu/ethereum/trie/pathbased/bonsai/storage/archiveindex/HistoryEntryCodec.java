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

import org.apache.tuweni.bytes.Bytes;

/**
 * Wraps a {@link TrieNodeDiffCodec} payload with two outer bytes: an entry-level type (FULL vs
 * FULL_CREATION vs DIFF, decided by the writer's checkpoint/depth/creation rules — see
 * ArchiveTrieBuilder) and a running countSinceFull. Keeping these outside the diff-codec payload
 * lets a reader learn "is this a FULL?" and "how many DIFFs back is the checkpoint?" without
 * decoding the inner payload, and lets the writer decide the next entry's type with no extra read.
 */
public final class HistoryEntryCodec {

  public enum EntryType {
    DIFF((byte) 0x00),
    FULL((byte) 0x01),
    FULL_CREATION((byte) 0x02);

    private final byte code;

    EntryType(final byte code) {
      this.code = code;
    }

    static EntryType fromCode(final byte code) {
      for (final EntryType t : values()) {
        if (t.code == code) {
          return t;
        }
      }
      throw new IllegalArgumentException("unknown HistoryEntryCodec type code: " + code);
    }
  }

  private HistoryEntryCodec() {}

  public static Bytes encode(
      final EntryType type, final int countSinceFull, final Bytes diffCodecPayload) {
    if (countSinceFull < 0 || countSinceFull > 0xFF) {
      throw new IllegalArgumentException("countSinceFull out of byte range: " + countSinceFull);
    }
    return Bytes.concatenate(
        Bytes.of(type.code), Bytes.of((byte) countSinceFull), diffCodecPayload);
  }

  public static Decoded decode(final Bytes raw) {
    final EntryType type = EntryType.fromCode(raw.get(0));
    final int countSinceFull = raw.get(1) & 0xFF;
    final Bytes diffCodecPayload = raw.slice(2);
    return new Decoded(type, countSinceFull, diffCodecPayload);
  }

  /** Decoded view of one history entry. */
  public static final class Decoded {
    private final EntryType type;
    private final int countSinceFull;
    private final Bytes diffCodecPayload;

    private Decoded(final EntryType type, final int countSinceFull, final Bytes diffCodecPayload) {
      this.type = type;
      this.countSinceFull = countSinceFull;
      this.diffCodecPayload = diffCodecPayload;
    }

    public boolean isFull() {
      return type == EntryType.FULL || type == EntryType.FULL_CREATION;
    }

    public EntryType type() {
      return type;
    }

    public int countSinceFull() {
      return countSinceFull;
    }

    /**
     * The raw TrieNodeDiffCodec-encoded payload — pass directly to {@link
     * TrieNodeDiffCodec#reconstruct(Bytes, java.util.List)} as either the {@code fullEntry} (when
     * {@link #isFull()}) or one element of {@code diffEntries} (otherwise).
     */
    public Bytes diffCodecPayload() {
      return diffCodecPayload;
    }
  }
}
