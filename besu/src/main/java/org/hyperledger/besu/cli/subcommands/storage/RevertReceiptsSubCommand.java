/*
 * Copyright Hyperledger Besu Contributors.
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

package org.hyperledger.besu.cli.subcommands.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage.TRANSACTION_RECEIPTS_PREFIX;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.VariablesStorage;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** Migrates compacted receipts back to previous format */
@CommandLine.Command(
    name = "revert-receipts",
    description = "Migrate code to code by hash",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class RevertReceiptsSubCommand implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RevertReceiptsSubCommand.class);

  @SuppressWarnings("unused")
  @CommandLine.ParentCommand
  private StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Override
  public void run() {
    final BesuController besuController = createBesuController();
    final StorageProvider storageProvider = besuController.getStorageProvider();

    final VariablesStorage variablesStorage = storageProvider.createVariablesStorage();
    final ProtocolSchedule protocolSchedule = besuController.getProtocolSchedule();
    final KeyValueStorage blockchainStorage =
        storageProvider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.BLOCKCHAIN);
    final KeyValueStoragePrefixedKeyBlockchainStorage compactedPrefixedBlockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            blockchainStorage,
            variablesStorage,
            ScheduleBasedBlockHeaderFunctions.create(protocolSchedule),
            true);

    LOG.info("Receipt revert started");

    final Bytes startKey =
        Bytes.concatenate(TRANSACTION_RECEIPTS_PREFIX, Bytes.repeat((byte) 0, 32));
    final Bytes endKey =
        Bytes.concatenate(TRANSACTION_RECEIPTS_PREFIX, Bytes.repeat((byte) 0xff, 32));
    blockchainStorage
        .streamFromKey(startKey.toArrayUnsafe(), endKey.toArrayUnsafe())
        .forEach(
            keyValuePair -> {
              KeyValueStoragePrefixedKeyBlockchainStorage.Updater updater =
                  compactedPrefixedBlockchainStorage.updater();
              final KeyValueStorageTransaction transaction = blockchainStorage.startTransaction();
              final Bytes receipt = Bytes.wrap(keyValuePair.getValue());
              final Hash blockHash = Hash.wrap(Bytes32.wrap(keyValuePair.getKey(), 1));
              final List<TransactionReceipt> decodedReceipts =
                  RLP.input(receipt).readList(TransactionReceipt::readFrom);
              updater.putTransactionReceipts(blockHash, decodedReceipts);
              transaction.commit();
            });

    LOG.info("Receipt revert completed");
  }

  private BesuController createBesuController() {
    return parentCommand.besuCommand.buildController();
  }
}
