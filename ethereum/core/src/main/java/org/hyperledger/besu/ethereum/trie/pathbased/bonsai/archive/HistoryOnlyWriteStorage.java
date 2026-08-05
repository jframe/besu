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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_NODE_HISTORY_ARCHIVE;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;

/**
 * A {@link SegmentedKeyValueStorage} decorator that permits writes only to {@link
 * KeyValueSegmentIdentifier#TRIE_NODE_HISTORY_ARCHIVE}. Writes to every other segment are silently
 * dropped; reads are forwarded to the delegate unchanged.
 *
 * <p>This wrapper is placed around the node's real storage when the trie-node history walker
 * replays historical blocks. {@code persist()} emits flat account/storage/code writes and
 * world-metadata keys alongside trie-node writes; without this filter those writes would overwrite
 * chain-head state and corrupt the live node.
 *
 * <p>Note: non-transactional mutations ({@link #tryDelete} and {@link #clear}) are forwarded to the
 * delegate unconditionally. Callers must not invoke these methods on non-history segments through
 * this wrapper.
 */
public class HistoryOnlyWriteStorage implements SegmentedKeyValueStorage {

  private final SegmentedKeyValueStorage delegate;

  /**
   * Instantiates a new HistoryOnlyWriteStorage.
   *
   * @param delegate the underlying storage to forward reads and history writes to
   */
  public HistoryOnlyWriteStorage(final SegmentedKeyValueStorage delegate) {
    this.delegate = delegate;
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    return delegate.get(segment, key);
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    return delegate.getNearestBefore(segmentIdentifier, key);
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    return delegate.getNearestAfter(segmentIdentifier, key);
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    return new FilteringTransaction(delegate.startTransaction());
  }

  @Override
  public SegmentedKeyValueStorageTransaction startLowPriorityTransaction() throws StorageException {
    return new FilteringTransaction(delegate.startLowPriorityTransaction());
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier) {
    return delegate.stream(segmentIdentifier);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey) {
    return delegate.streamFromKey(segmentIdentifier, startKey);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey, final byte[] endKey) {
    return delegate.streamFromKey(segmentIdentifier, startKey, endKey);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier) {
    return delegate.streamKeys(segmentIdentifier);
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    return delegate.tryDelete(segmentIdentifier, key);
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return delegate.getAllKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return delegate.getAllValuesFromKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public void clear(final SegmentIdentifier segmentIdentifier) {
    delegate.clear(segmentIdentifier);
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public void close() {
    // Non-owning decorator: do not close the delegate.
  }

  private record FilteringTransaction(SegmentedKeyValueStorageTransaction delegate)
      implements SegmentedKeyValueStorageTransaction {

    @Override
    public void put(final SegmentIdentifier segment, final byte[] key, final byte[] value) {
      if (TRIE_NODE_HISTORY_ARCHIVE.equals(segment)) {
        delegate.put(segment, key, value);
      }
      // Every other segment is intentionally dropped: the walker replays historical blocks, so
      // its flat account/storage/code and world-metadata writes must never reach real storage
      // and overwrite chain-head state.
    }

    @Override
    public void remove(final SegmentIdentifier segment, final byte[] key) {
      if (TRIE_NODE_HISTORY_ARCHIVE.equals(segment)) {
        delegate.remove(segment, key);
      }
    }

    @Override
    public void commit() {
      delegate.commit();
    }

    @Override
    public void rollback() {
      delegate.rollback();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
