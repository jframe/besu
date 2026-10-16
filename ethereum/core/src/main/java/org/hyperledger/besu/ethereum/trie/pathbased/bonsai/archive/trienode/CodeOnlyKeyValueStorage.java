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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The base of the roller's scratch storage: empty, except that contract code is read from head
 * storage. Code is keyed by code hash and never deleted, so a head read is valid at any block.
 */
final class CodeOnlyKeyValueStorage extends SegmentedInMemoryKeyValueStorage {

  private final SegmentedKeyValueStorage headStorage;

  CodeOnlyKeyValueStorage(final SegmentedKeyValueStorage headStorage) {
    super(List.of(ACCOUNT_INFO_STATE, CODE_STORAGE, ACCOUNT_STORAGE_STORAGE, TRIE_BRANCH_STORAGE));
    this.headStorage = Objects.requireNonNull(headStorage);
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    return segment == CODE_STORAGE ? headStorage.get(segment, key) : super.get(segment, key);
  }
}
