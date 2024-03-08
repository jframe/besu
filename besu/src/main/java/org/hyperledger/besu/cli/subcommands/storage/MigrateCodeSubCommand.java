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

import static org.hyperledger.besu.ethereum.trie.bonsai.storage.flat.CodeHashCodeStorageStrategy.isCodeHashValue;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.bonsai.storage.flat.AccountHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.bonsai.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/** Migrates code stored by account hash to being stored by code hash */
@Command(
    name = "migrate-code",
    description = "Migrate code to code by hash",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class MigrateCodeSubCommand implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MigrateCodeSubCommand.class);

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

    final AccountHashCodeStorageStrategy accountHashCodeStorageStrategy =
        new AccountHashCodeStorageStrategy();
    final CodeHashCodeStorageStrategy codeHashCodeStorageStrategy =
        new CodeHashCodeStorageStrategy();
    final SegmentedKeyValueStorage codeStorage =
        storageProvider.getStorageBySegmentIdentifiers(
            List.of(KeyValueSegmentIdentifier.CODE_STORAGE));

    LOG.info("Code migration started");

    codeStorage.stream(KeyValueSegmentIdentifier.CODE_STORAGE)
        .filter(pair -> !isCodeHashValue(pair.getKey(), pair.getValue()))
        .forEach(
            keyValuePair -> {
              final SegmentedKeyValueStorageTransaction transaction =
                  codeStorage.startTransaction();
              final Bytes key = Bytes.wrap(keyValuePair.getKey());
              final Bytes value = Bytes.wrap(keyValuePair.getValue());
              final Hash accountHash = Hash.wrap(Bytes32.wrap(key));
              final Hash codeHash = Hash.hash(value);
              codeHashCodeStorageStrategy.putFlatCode(transaction, accountHash, codeHash, value);
              accountHashCodeStorageStrategy.removeFlatCode(transaction, accountHash, codeHash);
              transaction.commit();
            });

    LOG.info("Code migration completed");
  }

  private BesuController createBesuController() {
    return parentCommand.besuCommand.buildController();
  }
}
