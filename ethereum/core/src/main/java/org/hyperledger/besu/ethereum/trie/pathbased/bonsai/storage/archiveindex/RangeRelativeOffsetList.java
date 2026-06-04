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
import java.util.OptionalInt;

import org.apache.tuweni.bytes.Bytes;

/**
 * A compact, immutable list of range-relative block offsets for a single trie node within one
 * 1,000,000-block range (design §5.2 / L4 layer).
 *
 * <p>Each offset is in {@code [0, 0xFFFFFF]} and is packed as a 3-byte big-endian entry. The list
 * is stored as the VALUE of a KV entry, so {@link #toBytes()} / {@link #fromBytes(Bytes)} provide
 * clean serialisation. {@link #latestLeq(int)} answers "latest change ≤ T" via binary search over
 * fixed-width slots.
 *
 * <p>Invariants:
 *
 * <ul>
 *   <li>Offsets are appended in non-decreasing order.
 *   <li>Re-appending the current tail offset is a no-op (idempotent).
 * </ul>
 */
public final class RangeRelativeOffsetList {

  /** Maximum value that fits in 3 bytes (= 2^24 − 1 = 16,777,215). */
  private static final int MAX_OFFSET = 0xFFFFFF;

  private static final int ENTRY_BYTES = 3;

  private static final RangeRelativeOffsetList EMPTY =
      new RangeRelativeOffsetList(Bytes.EMPTY, Integer.MIN_VALUE);

  /** The packed buffer. Length is always a multiple of {@value ENTRY_BYTES}. */
  private final Bytes buf;

  /**
   * Cached value of the last appended offset, or {@link Integer#MIN_VALUE} when the list is empty.
   * Stored alongside {@code buf} to avoid re-reading on every {@code append()} call.
   */
  private final int lastOffset;

  private RangeRelativeOffsetList(final Bytes buf, final int lastOffset) {
    this.buf = buf;
    this.lastOffset = lastOffset;
  }

  // -------------------------------------------------------------------------
  // Factory methods
  // -------------------------------------------------------------------------

  /** Returns an empty list. */
  public static RangeRelativeOffsetList empty() {
    return EMPTY;
  }

  /**
   * Wraps an existing packed buffer. The buffer length must be a multiple of 3, otherwise an {@link
   * IllegalArgumentException} is thrown.
   */
  public static RangeRelativeOffsetList fromBytes(final Bytes packed) {
    if (packed.isEmpty()) {
      return EMPTY;
    }
    if (packed.size() % ENTRY_BYTES != 0) {
      throw new IllegalArgumentException(
          "Packed offset buffer length must be a multiple of 3, got " + packed.size());
    }
    // Read the last entry to prime lastOffset
    int n = packed.size() / ENTRY_BYTES;
    int last = readEntry(packed, n - 1);
    return new RangeRelativeOffsetList(packed, last);
  }

  // -------------------------------------------------------------------------
  // Core operations
  // -------------------------------------------------------------------------

  /**
   * Returns a new list with {@code offset} appended as a 3-byte big-endian entry.
   *
   * <ul>
   *   <li>If {@code offset == lastOffset} → returns {@code this} (idempotent).
   *   <li>If {@code offset < lastOffset} → throws {@link IllegalArgumentException}.
   *   <li>If {@code offset} is outside {@code [0, 0xFFFFFF]} → throws {@link
   *       IllegalArgumentException}.
   * </ul>
   */
  public RangeRelativeOffsetList append(final int offset) {
    if (offset < 0 || offset > MAX_OFFSET) {
      throw new IllegalArgumentException("Offset must be in [0, 0xFFFFFF] but was " + offset);
    }
    // When the list is empty, lastOffset is the Integer.MIN_VALUE sentinel, which is strictly less
    // than any valid (non-negative) offset already accepted above — so the comparisons below are
    // safe without an explicit isEmpty() guard.
    if (offset == lastOffset) {
      return this; // idempotent no-op
    }
    if (offset < lastOffset) {
      throw new IllegalArgumentException(
          "Offsets must be appended in non-decreasing order: "
              + offset
              + " < current tail "
              + lastOffset);
    }
    Bytes entry =
        Bytes.of(
            (byte) ((offset >> 16) & 0xFF), (byte) ((offset >> 8) & 0xFF), (byte) (offset & 0xFF));
    // TODO(stage-3): replace Bytes.concatenate with a growable byte[] builder to avoid O(n^2)
    // copying when appending many offsets to one node's list during block import.
    return new RangeRelativeOffsetList(Bytes.concatenate(buf, entry), offset);
  }

  /**
   * Returns the packed byte buffer. Empty list returns {@link Bytes#EMPTY}.
   *
   * <p>For storage: use {@code fromBytes(list.toBytes())} to reconstruct.
   */
  public Bytes toBytes() {
    return buf;
  }

  /**
   * Returns the largest stored offset that is ≤ {@code target}, using binary search over the
   * fixed-width slots. Returns {@link OptionalInt#empty()} if the list is empty or all stored
   * offsets are greater than {@code target}.
   */
  public OptionalInt latestLeq(final int target) {
    int n = size();
    if (n == 0) {
      return OptionalInt.empty();
    }
    // Binary search for the rightmost slot whose value ≤ target.
    // Maintain: all slots in [0, lo) have value ≤ target (known good),
    //            all slots in [hi, n) have value > target.
    int lo = 0;
    int hi = n;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (readEntry(buf, mid) <= target) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    // lo is the index of the first entry > target; lo-1 is the rightmost ≤ target.
    if (lo == 0) {
      return OptionalInt.empty();
    }
    return OptionalInt.of(readEntry(buf, lo - 1));
  }

  /** Returns the number of entries in the list. */
  public int size() {
    return buf.size() / ENTRY_BYTES;
  }

  /** Returns {@code true} if the list contains no entries. */
  public boolean isEmpty() {
    return buf.isEmpty();
  }

  // -------------------------------------------------------------------------
  // equals / hashCode
  // -------------------------------------------------------------------------

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof RangeRelativeOffsetList other)) return false;
    return Objects.equals(buf, other.buf);
  }

  @Override
  public int hashCode() {
    return buf.hashCode();
  }

  @Override
  public String toString() {
    return "RangeRelativeOffsetList{size=" + size() + ", bytes=" + buf + "}";
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /** Reads the 3-byte big-endian entry at slot index {@code i} from the buffer. */
  private static int readEntry(final Bytes b, final int i) {
    int base = i * ENTRY_BYTES;
    return ((b.get(base) & 0xFF) << 16)
        | ((b.get(base + 1) & 0xFF) << 8)
        | (b.get(base + 2) & 0xFF);
  }
}
