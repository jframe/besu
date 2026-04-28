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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

class KeyDecoderTest {

  @Test
  void decodesAccountKey() {
    final byte[] hash = filledBytes(32, (byte) 0xab);
    final byte[] key = concat(hash, blockSuffix(123_456L));

    final KeyDecoder.Decoded result = KeyDecoder.decode(ArchiveCf.ACCOUNT, key);

    assertThat(result.prefix()).containsExactly(hash);
    assertThat(result.blockNumber()).isEqualTo(123_456L);
  }

  @Test
  void decodesStorageKey() {
    final byte[] account = filledBytes(32, (byte) 0x11);
    final byte[] slot = filledBytes(32, (byte) 0x22);
    final byte[] key = concat(account, slot, blockSuffix(7_777_777L));

    final KeyDecoder.Decoded result = KeyDecoder.decode(ArchiveCf.STORAGE, key);

    assertThat(result.prefix()).hasSize(64);
    assertThat(Arrays.copyOfRange(result.prefix(), 0, 32)).containsExactly(account);
    assertThat(Arrays.copyOfRange(result.prefix(), 32, 64)).containsExactly(slot);
    assertThat(result.blockNumber()).isEqualTo(7_777_777L);
  }

  @Test
  void decodesBlockNumberZero() {
    final byte[] key = concat(filledBytes(32, (byte) 0), blockSuffix(0L));
    assertThat(KeyDecoder.decode(ArchiveCf.ACCOUNT, key).blockNumber()).isZero();
  }

  @Test
  void rejectsAccountKeyWithWrongLength() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> KeyDecoder.decode(ArchiveCf.ACCOUNT, new byte[39]))
        .withMessageContaining("expected 40 bytes");
  }

  @Test
  void rejectsStorageKeyWithWrongLength() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> KeyDecoder.decode(ArchiveCf.STORAGE, new byte[71]))
        .withMessageContaining("expected 72 bytes");
  }

  @Test
  void prefixIsACopyNotASlice() {
    final byte[] key = concat(filledBytes(32, (byte) 0x55), blockSuffix(1L));
    final KeyDecoder.Decoded result = KeyDecoder.decode(ArchiveCf.ACCOUNT, key);
    key[0] = (byte) 0xff; // mutate input
    assertThat(result.prefix()[0]).isEqualTo((byte) 0x55); // unchanged
  }

  private static byte[] blockSuffix(final long blockNumber) {
    return ByteBuffer.allocate(8).putLong(blockNumber).array();
  }

  private static byte[] filledBytes(final int length, final byte value) {
    final byte[] arr = new byte[length];
    Arrays.fill(arr, value);
    return arr;
  }

  private static byte[] concat(final byte[]... parts) {
    int len = 0;
    for (final byte[] p : parts) {
      len += p.length;
    }
    final byte[] out = new byte[len];
    int pos = 0;
    for (final byte[] p : parts) {
      System.arraycopy(p, 0, out, pos, p.length);
      pos += p.length;
    }
    return out;
  }
}
