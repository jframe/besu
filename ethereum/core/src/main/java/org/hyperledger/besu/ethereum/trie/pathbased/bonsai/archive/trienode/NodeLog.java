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

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;

/** Value type representing a trie node's complete state at a point in time. */
public final class NodeLog {
  public enum NodeType {
    BRANCH,
    EXTENSION,
    LEAF
  }

  private final NodeType type;
  private final Bytes path; // Bytes.EMPTY when absent
  private final SortedMap<Integer, Bytes> children; // position -> canonical slot ref bytes
  private final Optional<Bytes> value;

  public NodeLog(
      final NodeType type,
      final Bytes path,
      final SortedMap<Integer, Bytes> children,
      final Optional<Bytes> value) {
    this.type = type;
    this.path = path;
    this.children = new TreeMap<>(children);
    this.value = value;
  }

  public NodeType type() {
    return type;
  }

  public Bytes path() {
    return path;
  }

  public SortedMap<Integer, Bytes> children() {
    return Collections.unmodifiableSortedMap(children);
  }

  public Optional<Bytes> value() {
    return value;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final NodeLog other = (NodeLog) obj;
    return type == other.type
        && Objects.equals(path, other.path)
        && Objects.equals(children, other.children)
        && Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, path, children, value);
  }

  @Override
  public String toString() {
    return "NodeLog{"
        + "type="
        + type
        + ", path="
        + path
        + ", children="
        + children
        + ", value="
        + value
        + '}';
  }
}
