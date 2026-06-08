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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiTrieNodeStrategyTest {

  private SegmentedInMemoryKeyValueStorage kv;
  private BonsaiTrieNodeStrategy strategy;

  @BeforeEach
  void setUp() {
    kv = new SegmentedInMemoryKeyValueStorage();
    strategy = new BonsaiTrieNodeStrategy();
  }

  @Test
  void putAccountTrieNode_prependsHash() {
    final Bytes location = Bytes.of(0x01, 0x02);
    final Bytes node = Bytes.of(0xAA, 0xBB, 0xCC);
    final Bytes32 nodeHash = org.hyperledger.besu.crypto.Hash.keccak256(node);

    final SegmentedKeyValueStorageTransaction tx = kv.startTransaction();
    strategy.putFlatAccountTrieNode(kv, tx, location, nodeHash, node);
    tx.commit();

    final byte[] raw = kv.get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe()).orElseThrow();
    assertThat(raw).hasSize(32 + node.size());
    assertThat(Bytes32.wrap(raw, 0)).isEqualTo(nodeHash);
    assertThat(Bytes.wrap(raw, 32, raw.length - 32)).isEqualTo(node);
  }

  @Test
  void getAccountTrieNode_stripsHashPrefix() {
    final Bytes location = Bytes.of(0x03);
    final Bytes node = Bytes.of(0xDE, 0xAD, 0xBE, 0xEF);
    final Bytes32 nodeHash = org.hyperledger.besu.crypto.Hash.keccak256(node);

    final SegmentedKeyValueStorageTransaction tx = kv.startTransaction();
    strategy.putFlatAccountTrieNode(kv, tx, location, nodeHash, node);
    tx.commit();

    final Bytes result = strategy.getFlatAccountTrieNode(location, nodeHash, kv).orElseThrow();
    assertThat(result).isEqualTo(node);
  }

  @Test
  void putStorageTrieNode_prependsHash() {
    final Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(0x11)));
    final Bytes location = Bytes.of(0x05);
    final Bytes node = Bytes.of(0x99, 0x88, 0x77);
    final Bytes32 nodeHash = org.hyperledger.besu.crypto.Hash.keccak256(node);

    final SegmentedKeyValueStorageTransaction tx = kv.startTransaction();
    strategy.putFlatStorageTrieNode(kv, tx, accountHash, location, nodeHash, node);
    tx.commit();

    final Bytes storageKey = Bytes.concatenate(accountHash.getBytes(), location);
    final byte[] raw = kv.get(TRIE_BRANCH_STORAGE, storageKey.toArrayUnsafe()).orElseThrow();
    assertThat(raw).hasSize(32 + node.size());
    assertThat(Bytes32.wrap(raw, 0)).isEqualTo(nodeHash);
    assertThat(Bytes.wrap(raw, 32, raw.length - 32)).isEqualTo(node);
  }

  @Test
  void getStorageTrieNode_stripsHashPrefix() {
    final Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(0x22)));
    final Bytes location = Bytes.of(0x06);
    final Bytes node = Bytes.of(0x11, 0x22, 0x33);
    final Bytes32 nodeHash = org.hyperledger.besu.crypto.Hash.keccak256(node);

    final SegmentedKeyValueStorageTransaction tx = kv.startTransaction();
    strategy.putFlatStorageTrieNode(kv, tx, accountHash, location, nodeHash, node);
    tx.commit();

    final Bytes result =
        strategy.getFlatStorageTrieNode(accountHash, location, nodeHash, kv).orElseThrow();
    assertThat(result).isEqualTo(node);
  }
}
