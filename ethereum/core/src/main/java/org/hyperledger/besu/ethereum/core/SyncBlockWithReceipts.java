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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Hash;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;

public class SyncBlockWithReceipts {
  private final SyncBlock block;
  private final List<SyncTransactionReceipt> syncReceipts;

  public SyncBlockWithReceipts(
      final SyncBlock block, final List<SyncTransactionReceipt> syncReceipts) {
    this.block = block;
    this.syncReceipts = syncReceipts;
  }

  public BlockHeader getHeader() {
    return block.getHeader();
  }

  public SyncBlock getBlock() {
    return block;
  }

  public List<SyncTransactionReceipt> getSyncReceipts() {
    return syncReceipts;
  }

  /**
   * Lazily decode the sync receipts into full TransactionReceipt objects. This should only be
   * called at the storage boundary when receipts need to be persisted to the database.
   *
   * @return List of fully decoded TransactionReceipt objects
   */
  public List<TransactionReceipt> getReceipts() {
    return syncReceipts.stream()
        .map(sr -> sr.getReceiptSupplier().get())
        .collect(Collectors.toList());
  }

  public long getNumber() {
    return block.getHeader().getNumber();
  }

  public Hash getHash() {
    return block.getHash();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SyncBlockWithReceipts that = (SyncBlockWithReceipts) o;
    return Objects.equals(block, that.block) && Objects.equals(syncReceipts, that.syncReceipts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(block, syncReceipts);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("block", block)
        .add("syncReceipts", syncReceipts)
        .toString();
  }
}
