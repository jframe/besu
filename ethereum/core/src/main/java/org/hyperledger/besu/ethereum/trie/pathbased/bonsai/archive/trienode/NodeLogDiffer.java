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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.tuweni.bytes.Bytes;

/** Semantic diff and apply operations for trie nodes. */
public final class NodeLogDiffer {
  private NodeLogDiffer() {}

  /**
   * Compute the semantic diff between two NodeLog instances.
   *
   * <p>Drops unchanged elements. The invariant apply(prior, diff(prior, next)) == next is
   * preserved.
   *
   * @param prior the base node
   * @param next the target node
   * @return a list of mutations to transform prior into next
   */
  public static List<NodeMutation> diff(final NodeLog prior, final NodeLog next) {
    final List<NodeMutation> ops = new ArrayList<>();

    // Type change
    if (prior.type() != next.type()) {
      ops.add(new NodeMutation.TypeChange(next.type()));
    }

    // Path change
    if (!prior.path().equals(next.path())) {
      ops.add(new NodeMutation.PathChange(next.path()));
    }

    // Value change
    if (!prior.value().equals(next.value())) {
      ops.add(new NodeMutation.ValueChange(next.value()));
    }

    // Child changes: union of all positions from both nodes
    final SortedSet<Integer> positions = new TreeSet<>();
    positions.addAll(prior.children().keySet());
    positions.addAll(next.children().keySet());

    for (final int pos : positions) {
      final Optional<Bytes> p = Optional.ofNullable(prior.children().get(pos));
      final Optional<Bytes> n = Optional.ofNullable(next.children().get(pos));
      if (!p.equals(n)) {
        ops.add(new NodeMutation.ChildChange(pos, n));
      }
    }

    return ops;
  }

  /**
   * Apply a list of mutations to a base NodeLog.
   *
   * @param base the base node
   * @param mutations the mutations to apply
   * @return a new NodeLog with all mutations applied
   */
  public static NodeLog apply(final NodeLog base, final List<NodeMutation> mutations) {
    NodeLog.NodeType type = base.type();
    Bytes path = base.path();
    final TreeMap<Integer, Bytes> children = new TreeMap<>(base.children());
    Optional<Bytes> value = base.value();

    for (final NodeMutation m : mutations) {
      if (m instanceof NodeMutation.TypeChange t) {
        type = t.next();
      } else if (m instanceof NodeMutation.PathChange p) {
        path = p.next();
      } else if (m instanceof NodeMutation.ValueChange v) {
        value = v.next();
      } else if (m instanceof NodeMutation.ChildChange c) {
        if (c.next().isPresent()) {
          children.put(c.pos(), c.next().get());
        } else {
          children.remove(c.pos());
        }
      }
    }

    return new NodeLog(type, path, children, value);
  }
}
