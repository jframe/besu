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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class FrameTest {

  @Test
  void frameConstructionWithTarget() {
    final Address target = Address.fromHexString("0xdead000000000000000000000000000000000000");
    final Frame frame =
        new Frame(Frame.MODE_DEFAULT, Optional.of(target), 100_000L, Bytes.fromHexString("0xaabb"));

    assertThat(frame.getMode()).isEqualTo(Frame.MODE_DEFAULT);
    assertThat(frame.getTarget()).contains(target);
    assertThat(frame.getGasLimit()).isEqualTo(100_000L);
    assertThat(frame.getData()).isEqualTo(Bytes.fromHexString("0xaabb"));
  }

  @Test
  void frameConstructionWithoutTarget() {
    final Frame frame =
        new Frame(Frame.MODE_VERIFY, Optional.empty(), 50_000L, Bytes.fromHexString("0xcafe"));

    assertThat(frame.getMode()).isEqualTo(Frame.MODE_VERIFY);
    assertThat(frame.getTarget()).isEmpty();
    assertThat(frame.getGasLimit()).isEqualTo(50_000L);
    assertThat(frame.getData()).isEqualTo(Bytes.fromHexString("0xcafe"));
  }

  @Test
  void frameSenderMode() {
    final Frame frame = new Frame(Frame.MODE_SENDER, Optional.empty(), 0L, Bytes.EMPTY);
    assertThat(frame.getMode()).isEqualTo(Frame.MODE_SENDER);
  }

  @Test
  void frameModeConstants() {
    assertThat(Frame.MODE_DEFAULT).isEqualTo((byte) 0);
    assertThat(Frame.MODE_VERIFY).isEqualTo((byte) 1);
    assertThat(Frame.MODE_SENDER).isEqualTo((byte) 2);
  }

  @Test
  void frameEquality() {
    final Address target = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final Frame frame1 =
        new Frame(Frame.MODE_DEFAULT, Optional.of(target), 21_000L, Bytes.of(0x01));
    final Frame frame2 =
        new Frame(Frame.MODE_DEFAULT, Optional.of(target), 21_000L, Bytes.of(0x01));
    final Frame frame3 =
        new Frame(Frame.MODE_VERIFY, Optional.of(target), 21_000L, Bytes.of(0x01));

    assertThat(frame1).isEqualTo(frame2);
    assertThat(frame1).isNotEqualTo(frame3);
    assertThat(frame1.hashCode()).isEqualTo(frame2.hashCode());
  }

  @Test
  void frameToString() {
    final Frame frame =
        new Frame(Frame.MODE_DEFAULT, Optional.empty(), 30_000L, Bytes.fromHexString("0x1234"));
    final String str = frame.toString();

    assertThat(str).contains("mode=0");
    assertThat(str).contains("gasLimit=30000");
  }
}
