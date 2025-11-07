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
package org.hyperledger.besu.ethereum.rlp;

import static com.google.common.base.Preconditions.checkState;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Optimized RLPOutput implementation that pre-allocates buffer space and writes directly.
 *
 * <p>Key optimizations: 1. Pre-allocates output buffer based on estimated size (avoids
 * reallocation) 2. Writes directly to buffer instead of accumulating in ArrayList 3. Thread-local
 * buffer pool for reuse across multiple encodings 4. Minimizes object allocations
 *
 * <p>Usage:
 *
 * <pre>
 * // Estimate size upfront (approximate is fine, will grow if needed)
 * int estimatedSize = estimateTransactionSize(tx);
 * PreAllocatedRLPOutput output = new PreAllocatedRLPOutput(estimatedSize);
 * tx.writeTo(output);
 * Bytes encoded = output.encoded();
 * </pre>
 *
 * <p>For maximum performance with thread-local pooling:
 *
 * <pre>
 * PreAllocatedRLPOutput output = PreAllocatedRLPOutput.get();
 * try {
 *   output.reset(estimatedSize);
 *   tx.writeTo(output);
 *   return output.encoded();
 * } finally {
 *   output.returnToPool();
 * }
 * </pre>
 */
public class PreAllocatedRLPOutput implements RLPOutput {

  /** Thread-local pool for buffer reuse */
  private static final ThreadLocal<PreAllocatedRLPOutput> POOL =
      ThreadLocal.withInitial(() -> new PreAllocatedRLPOutput(4096));

  /** Growth factor when buffer needs to expand */
  private static final double GROWTH_FACTOR = 1.5;

  /** Direct byte buffer for writing */
  private MutableBytes buffer;

  /** Current write position in buffer */
  private int position;

  /** Stack for tracking list sizes and positions */
  private int[] listStack;

  /** Current depth in list stack */
  private int stackDepth;

  /**
   * Creates a new pre-allocated RLP output with the specified capacity.
   *
   * @param capacity initial buffer capacity in bytes
   */
  public PreAllocatedRLPOutput(final int capacity) {
    this.buffer = MutableBytes.create(capacity);
    this.listStack = new int[16]; // Support up to 16 levels of nesting
    this.stackDepth = 0;
    this.position = 0;
  }

  /**
   * Gets a pooled instance for the current thread.
   *
   * @return a reusable PreAllocatedRLPOutput
   */
  public static PreAllocatedRLPOutput get() {
    return POOL.get();
  }

  /**
   * Resets this output for reuse with a new encoding operation.
   *
   * @param estimatedSize estimated size of the encoding in bytes
   */
  public void reset(final int estimatedSize) {
    this.position = 0;
    this.stackDepth = 0;

    // Resize buffer if needed
    if (estimatedSize > buffer.size()) {
      buffer = MutableBytes.create(estimatedSize);
    }
  }

  /**
   * Returns this instance to the thread-local pool. Should be called after encoding is complete and
   * result has been copied.
   */
  public void returnToPool() {
    // Buffer stays allocated for reuse
    position = 0;
    stackDepth = 0;
  }

  @Override
  public void writeBytes(final Bytes v) {
    final int size = RLPEncodingHelpers.elementSize(v);
    ensureCapacity(size);
    position = RLPEncodingHelpers.writeElement(v, buffer, position);
  }

  @Override
  public void writeByte(final byte b) {
    ensureCapacity(1);
    if (b == 0) {
      buffer.set(position++, (byte) 0x80);
    } else {
      buffer.set(position++, b);
    }
  }

  @Override
  public void writeRaw(final Bytes v) {
    ensureCapacity(v.size());
    v.copyTo(buffer, position);
    position += v.size();
  }

  @Override
  public void startList() {
    // Reserve space for list header (we'll fill it in later)
    // List headers can be 1-5 bytes depending on payload size
    ensureCapacity(5); // Conservative: max header size

    // Grow stack if needed
    if (stackDepth >= listStack.length) {
      int[] newStack = new int[listStack.length * 2];
      System.arraycopy(listStack, 0, newStack, 0, listStack.length);
      listStack = newStack;
    }

    // Save current position as list start
    listStack[stackDepth++] = position;

    // Skip past header space (we'll come back to fill it in)
    position += 5; // Reserve max header size
  }

  @Override
  public void endList() {
    checkState(stackDepth > 0, "endList() called with no matching startList()");

    // Get the start position of this list
    final int listStart = listStack[--stackDepth];
    final int payloadStart = listStart + 5; // We reserved 5 bytes for header
    final int payloadSize = position - payloadStart;

    // Calculate actual header size
    final int headerSize = RLPEncodingHelpers.listHeaderSize(payloadSize);

    // Move payload if header is smaller than we reserved
    if (headerSize < 5) {
      final int shift = 5 - headerSize;
      // Shift payload left
      buffer.slice(payloadStart, payloadSize).copyTo(buffer, listStart + headerSize);
      position -= shift;
    }

    // Write the actual header
    RLPEncodingHelpers.writeListHeader(payloadSize, buffer, listStart);
  }

  /**
   * Returns the encoded RLP data.
   *
   * @return encoded bytes
   */
  public Bytes encoded() {
    checkState(stackDepth == 0, "A list has been started but not ended");
    return buffer.slice(0, position).copy();
  }

  /**
   * Returns the size of the encoded data.
   *
   * @return size in bytes
   */
  public int encodedSize() {
    return position;
  }

  /**
   * Ensures the buffer has at least the specified additional capacity.
   *
   * @param additionalBytes additional bytes needed
   */
  private void ensureCapacity(final int additionalBytes) {
    final int requiredSize = position + additionalBytes;
    if (requiredSize > buffer.size()) {
      // Grow buffer
      final int newSize = Math.max(requiredSize, (int) (buffer.size() * GROWTH_FACTOR));
      final MutableBytes newBuffer = MutableBytes.create(newSize);
      buffer.copyTo(newBuffer, 0);
      buffer = newBuffer;
    }
  }
}
