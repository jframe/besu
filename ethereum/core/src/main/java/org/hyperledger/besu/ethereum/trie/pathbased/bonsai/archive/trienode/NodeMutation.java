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

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** Sealed interface representing a semantic mutation to a trie node. */
public sealed interface NodeMutation
    permits NodeMutation.ChildChange,
        NodeMutation.ValueChange,
        NodeMutation.PathChange,
        NodeMutation.TypeChange {

  /** A child reference at a specific position changed or was added/removed. */
  record ChildChange(int pos, Optional<Bytes> next) implements NodeMutation {}

  /** The node's value changed. */
  record ValueChange(Optional<Bytes> next) implements NodeMutation {}

  /** The node's path changed. */
  record PathChange(Bytes next) implements NodeMutation {}

  /** The node's type changed. */
  record TypeChange(NodeLog.NodeType next) implements NodeMutation {}
}
