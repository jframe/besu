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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Persisted contiguous covered window {@code [indexStartBlock, lastIndexedBlock]} for archive
 * proofs.
 */
public final class ArchiveNodeHistoryProgress {

  public static final long UNSET_LAST_INDEXED = -1L;
  public static final long UNSET_INDEX_START = Long.MAX_VALUE;
  private static final byte[] PROGRESS_KEY =
      "ARCHIVE_TRIE_HISTORY_PROGRESS_KEY".getBytes(StandardCharsets.UTF_8);

  private volatile long indexStartBlock = UNSET_INDEX_START;
  private volatile long lastIndexedBlock = UNSET_LAST_INDEXED;

  public ArchiveNodeHistoryProgress() {}

  public boolean covers(final long block) {
    return lastIndexedBlock != UNSET_LAST_INDEXED
        && block >= indexStartBlock
        && block <= lastIndexedBlock;
  }

  public long lastIndexedBlock() {
    return lastIndexedBlock;
  }

  public void setLastIndexedBlock(final long block) {
    this.lastIndexedBlock = block;
  }

  public long indexStartBlock() {
    return indexStartBlock;
  }

  public void setIndexStartBlock(final long block) {
    this.indexStartBlock = Math.min(this.indexStartBlock, block);
  }

  public Bytes toBytes() {
    return Bytes.concatenate(
        Bytes.ofUnsignedLong(indexStartBlock), Bytes.ofUnsignedLong(lastIndexedBlock));
  }

  public void save(final SegmentedKeyValueStorageTransaction tx) {
    tx.put(TRIE_BRANCH_STORAGE, PROGRESS_KEY, toBytes().toArrayUnsafe());
  }

  public static ArchiveNodeHistoryProgress load(final SegmentedKeyValueStorage storage) {
    final ArchiveNodeHistoryProgress progress = new ArchiveNodeHistoryProgress();
    final Optional<byte[]> raw = storage.get(TRIE_BRANCH_STORAGE, PROGRESS_KEY);
    raw.ifPresent(
        bytes -> {
          final Bytes b = Bytes.wrap(bytes);
          progress.indexStartBlock = b.getLong(0);
          progress.lastIndexedBlock = b.getLong(8);
        });
    return progress;
  }
}
