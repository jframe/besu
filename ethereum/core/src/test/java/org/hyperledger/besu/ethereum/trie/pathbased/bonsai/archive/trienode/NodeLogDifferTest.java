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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.NodeLog.NodeType.BRANCH;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.NodeLog.NodeType.EXTENSION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.NodeLog.NodeType.LEAF;

import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class NodeLogDifferTest {

  private static Bytes ref(final int seed) {
    final byte[] b = new byte[33];
    b[0] = (byte) 0xa0;
    for (int i = 1; i < 33; i++) b[i] = (byte) (i + seed);
    return Bytes.wrap(b);
  }

  private static NodeLog branch(final SortedMap<Integer, Bytes> children, final Bytes value) {
    return new NodeLog(BRANCH, Bytes.EMPTY, children, Optional.ofNullable(value));
  }

  @Test
  void applyOfDiffReconstructsNext_sparseChildChange() {
    final TreeMap<Integer, Bytes> a = new TreeMap<>();
    a.put(1, ref(1));
    a.put(5, ref(5));
    final TreeMap<Integer, Bytes> b = new TreeMap<>();
    b.put(1, ref(1)); // unchanged
    b.put(5, ref(99)); // changed
    final NodeLog prior = branch(a, null);
    final NodeLog next = branch(b, null);

    final List<NodeMutation> diff = NodeLogDiffer.diff(prior, next);
    assertThat(diff).hasSize(1); // only slot 5 changed
    assertThat(NodeLogDiffer.apply(prior, diff)).isEqualTo(next);
  }

  @Test
  void applyOfDiffReconstructsNext_childAddedAndRemoved() {
    final TreeMap<Integer, Bytes> a = new TreeMap<>();
    a.put(1, ref(1));
    final TreeMap<Integer, Bytes> b = new TreeMap<>();
    b.put(2, ref(2)); // slot 1 removed, slot 2 added
    final NodeLog prior = branch(a, null);
    final NodeLog next = branch(b, null);

    assertThat(NodeLogDiffer.apply(prior, NodeLogDiffer.diff(prior, next))).isEqualTo(next);
  }

  @Test
  void applyOfDiffReconstructsNext_valuePathAndTypeChanges() {
    final NodeLog prior =
        new NodeLog(
            EXTENSION,
            Bytes.of(0x01, 0x02),
            new TreeMap<>(java.util.Map.of(0, ref(1))),
            Optional.empty());
    final NodeLog next =
        new NodeLog(
            LEAF, Bytes.of(0x01, 0x02, 0x10), new TreeMap<>(), Optional.of(Bytes.of(0xde, 0xad)));

    final List<NodeMutation> diff = NodeLogDiffer.diff(prior, next);
    assertThat(NodeLogDiffer.apply(prior, diff)).isEqualTo(next);
  }

  @Test
  void diffOfIdenticalModelsIsEmpty() {
    final NodeLog m = branch(new TreeMap<>(java.util.Map.of(3, ref(3))), Bytes.of(0x07));
    assertThat(NodeLogDiffer.diff(m, m)).isEmpty();
    assertThat(NodeLogDiffer.apply(m, List.of())).isEqualTo(m);
  }
}
