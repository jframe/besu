/*
 * Copyright Hyperledger Besu Contributors.
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
package org.hyperledger.besu.ethereum.core.transaction;

import org.hyperledger.besu.datatypes.Address;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Represents a single frame within a FRAME transaction (EIP-8141). A frame encodes an execution
 * unit with a mode, optional target address, gas limit, and calldata.
 */
public final class Frame {

  /** DEFAULT mode: normal call execution. */
  public static final byte MODE_DEFAULT = 0;

  /** VERIFY mode: verification call; data is not committed in the sig hash. */
  public static final byte MODE_VERIFY = 1;

  /** SENDER mode: execution context uses the explicit sender. */
  public static final byte MODE_SENDER = 2;

  private final byte mode;
  private final Optional<Address> target;
  private final long gasLimit;
  private final Bytes data;

  /**
   * Constructs a Frame.
   *
   * @param mode the frame mode (one of MODE_DEFAULT, MODE_VERIFY, MODE_SENDER)
   * @param target the target address, or empty to indicate the entry-point (0x00...00AA)
   * @param gasLimit the gas limit for this frame
   * @param data the calldata for this frame
   */
  public Frame(
      final byte mode, final Optional<Address> target, final long gasLimit, final Bytes data) {
    this.mode = mode;
    this.target = target;
    this.gasLimit = gasLimit;
    this.data = data;
  }

  /**
   * Returns the frame mode.
   *
   * @return the mode byte
   */
  public byte getMode() {
    return mode;
  }

  /**
   * Returns the frame target address, if specified.
   *
   * @return optional target address
   */
  public Optional<Address> getTarget() {
    return target;
  }

  /**
   * Returns the gas limit for this frame.
   *
   * @return gas limit
   */
  public long getGasLimit() {
    return gasLimit;
  }

  /**
   * Returns the calldata for this frame.
   *
   * @return frame data bytes
   */
  public Bytes getData() {
    return data;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof Frame that)) return false;
    return mode == that.mode
        && gasLimit == that.gasLimit
        && Objects.equals(target, that.target)
        && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, target, gasLimit, data);
  }

  @Override
  public String toString() {
    return "Frame{"
        + "mode="
        + mode
        + ", target="
        + target
        + ", gasLimit="
        + gasLimit
        + ", data="
        + data
        + '}';
  }
}
