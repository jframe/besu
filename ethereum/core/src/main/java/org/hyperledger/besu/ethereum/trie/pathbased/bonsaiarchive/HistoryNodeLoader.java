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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Thin {@link NodeLoader} adapter: one instance per trie (the account trie, or one per open storage
 * trie), delegating all bounded state to a shared {@link HistoryNodeCache}.
 */
final class HistoryNodeLoader implements NodeLoader {

  private final HistoryNodeCache cache;
  private final byte domain;
  private final Bytes naturalKeyPrefix; // null for the account trie; accountHash for a storage trie

  HistoryNodeLoader(final HistoryNodeCache cache, final byte domain, final Bytes naturalKeyPrefix) {
    this.cache = cache;
    this.domain = domain;
    this.naturalKeyPrefix = naturalKeyPrefix;
  }

  @Override
  public Optional<Bytes> getNode(final Bytes location, final Bytes32 hash) {
    if (hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.empty();
    }
    final Bytes naturalKey =
        naturalKeyPrefix == null ? location : Bytes.concatenate(naturalKeyPrefix, location);
    return cache.get(domain, naturalKey);
  }
}
