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
package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class TransactionIndexer implements BlockAddedObserver {
  private final BlockchainStorage blockchainStorage;
  private final ExecutorService txIndexerExecutorService;

  public TransactionIndexer(
      final BlockchainStorage blockchainStorage, final ExecutorService txIndexerExecutorService) {
    this.blockchainStorage = blockchainStorage;
    this.txIndexerExecutorService = txIndexerExecutorService;
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    if (event.getEventType() == BlockAddedEvent.EventType.HEAD_ADVANCED) {
      txIndexerExecutorService.execute(
          () -> {
            BlockchainStorage.Updater updater = blockchainStorage.updater();
            indexTransactionsForBlock(
                updater, event.getBlock().getHash(), event.getBlock().getBody().getTransactions());
          });
    }
  }

  private void indexTransactionsForBlock(
      final BlockchainStorage.Updater updater, final Hash blockHash, final List<Transaction> txs) {
    for (int index = 0; index < txs.size(); index++) {
      final Hash txHash = txs.get(index).getHash();
      final TransactionLocation loc = new TransactionLocation(blockHash, index);
      updater.putTransactionLocation(txHash, loc);
    }
  }
}
