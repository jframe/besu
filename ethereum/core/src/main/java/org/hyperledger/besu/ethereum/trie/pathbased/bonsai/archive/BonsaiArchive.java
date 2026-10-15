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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import org.hyperledger.besu.ethereum.core.Synchronizer;

import java.io.Closeable;
import java.util.function.LongSupplier;

/**
 * Lifecycle handle for the Bonsai archive's background components: trie-node capture, the flat-DB
 * archive migrator and, when archive proofs are enabled, the trie-node roller. Built by {@link
 * BonsaiArchiveFactory}.
 */
public interface BonsaiArchive extends Closeable {

  /**
   * Starts the background components. Must be called exactly once, after the synchronizer and the
   * sync state exist.
   *
   * @param synchronizer the node's synchronizer
   * @param bestChainHeight the best known chain height (the consensus client's latest payload when
   *     known, else the best peer estimate, never below the local head)
   * @throws IllegalStateException if called more than once
   */
  void start(Synchronizer synchronizer, LongSupplier bestChainHeight);

  /** Stops background work and shuts down executors. Must run before the storage closes. */
  @Override
  void close();
}
