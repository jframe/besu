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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Resolves a trie node's RLP as of a target block. FULL-only: a single {@code getLatestBefore}
 * lookup — the greatest version at or before the target is the node's content at that block
 * (reachability argument, roadmap §2.2). PR2 turns this into a diff-reconstruction walk.
 */
public final class ArchiveHistoryReader {

  private final ArchiveNodeHistoryStore historyStore;

  public ArchiveHistoryReader(final ArchiveNodeHistoryStore historyStore) {
    this.historyStore = Objects.requireNonNull(historyStore, "historyStore must not be null");
  }

  public Optional<Bytes> nodeAt(final Bytes naturalKey, final long targetBlock) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    if (targetBlock < 0) {
      throw new IllegalArgumentException("targetBlock must be >= 0, got " + targetBlock);
    }
    return historyStore.getLatestBefore(naturalKey, targetBlock);
  }
}
