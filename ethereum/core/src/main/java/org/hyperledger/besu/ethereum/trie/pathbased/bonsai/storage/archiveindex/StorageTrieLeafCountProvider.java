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

import org.apache.tuweni.bytes.Bytes;

/**
 * Supplies, for a given contract account, the number of leaves (occupied storage slots) in its
 * storage trie. {@link TrieLogChangeCounter} uses this to bound how deep each changed slot's
 * trie-node path runs: storage tries are per-contract and typically far smaller than the global
 * account trie, so pricing a slot's path depth against the global account leaf count drives it far
 * too deep and massively over-counts mid-depth node writes (the dominant cause of the size
 * estimator over-shooting the real column family).
 */
@FunctionalInterface
public interface StorageTrieLeafCountProvider {

  /**
   * Number of occupied storage slots for the given account, used as the leaf count {@code N} for
   * the storage trie's shape model. Implementations may cap and cache the result; the value is only
   * used logarithmically (to derive an expected path depth), so a capped count is acceptable.
   *
   * @param accountHash the 32-byte keccak account hash identifying the contract
   * @return the (possibly capped) number of storage slots, {@code 0} if the account has no storage
   */
  long leafCount(Bytes accountHash);
}
