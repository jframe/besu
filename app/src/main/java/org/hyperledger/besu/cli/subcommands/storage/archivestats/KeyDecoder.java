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
package org.hyperledger.besu.cli.subcommands.storage.archivestats;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Decodes archive raw keys into {@code (prefix, blockNumber)}. */
public final class KeyDecoder {

  private KeyDecoder() {}

  /**
   * Decode a raw archive key into its prefix and block-number components.
   *
   * @param cf which CF the key came from (drives prefix length).
   * @param rawKey the raw key bytes from the iterator.
   * @return the decoded prefix (copied) and block number.
   * @throws IllegalArgumentException if the key length does not match {@link
   *     ArchiveCf#expectedKeyLength()}.
   */
  public static Decoded decode(final ArchiveCf cf, final byte[] rawKey) {
    if (rawKey.length != cf.expectedKeyLength()) {
      throw new IllegalArgumentException(
          "Bad archive key for "
              + cf
              + ": expected "
              + cf.expectedKeyLength()
              + " bytes, got "
              + rawKey.length);
    }
    final byte[] prefix = Arrays.copyOfRange(rawKey, 0, cf.prefixBytes());
    final long blockNumber =
        ByteBuffer.wrap(rawKey, cf.prefixBytes(), ArchiveCf.BLOCK_NUMBER_SUFFIX_BYTES).getLong();
    return new Decoded(prefix, blockNumber);
  }

  /**
   * Result of decoding a raw key.
   *
   * @param prefix natural-key bytes (defensively copied)
   * @param blockNumber block number from the trailing 8-byte big-endian suffix
   */
  public record Decoded(byte[] prefix, long blockNumber) {}
}
