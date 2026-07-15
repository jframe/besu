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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ChangeCountResultTest {

  @Test
  void recordAndReadBackPerDepthCounts() {
    final ChangeCountResult r = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    r.recordMutation(3, false);
    r.recordMutation(3, false);
    r.recordMutation(3, true);
    assertThat(r.mutationsByDepth()[3]).isEqualTo(3L);
    assertThat(r.deletionsByDepth()[3]).isEqualTo(1L);
  }

  @Test
  void mergeSumsDepthArraysAndUnionsSampledLifetimes() {
    final ChangeCountResult a = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    final ChangeCountResult b = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    final Bytes key = Bytes.fromHexString("0x0102");
    a.recordMutation(5, false);
    a.recordSampledWrite(key, 5);
    a.recordSampledWrite(key, 5);
    b.recordMutation(5, false);
    b.recordSampledWrite(key, 5); // same key, later block range

    a.merge(b);

    assertThat(a.mutationsByDepth()[5]).isEqualTo(2L);
    assertThat(a.sampledLifetime().get(key)).containsExactly(5, 3); // depth 5, lifetime 3
  }

  @Test
  void accountDeltaAccumulatesByRange() {
    final ChangeCountResult r = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    r.recordAccountDelta(50_000L, +1); // range 0
    r.recordAccountDelta(150_000L, +1); // range 1
    r.recordAccountDelta(150_001L, -1); // range 1
    assertThat(r.accountDeltaByRange()[0]).isEqualTo(1L);
    assertThat(r.accountDeltaByRange()[1]).isEqualTo(0L);
  }
}
