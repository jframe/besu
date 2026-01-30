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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiFlatDbStrategyProvider extends FlatDbStrategyProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbStrategyProvider.class);

  public BonsaiFlatDbStrategyProvider(
      final MetricsSystem metricsSystem, final DataStorageConfiguration dataStorageConfiguration) {
    super(metricsSystem, dataStorageConfiguration);
  }

  @Override
  protected FlatDbMode getRequestedFlatDbMode(
      final DataStorageConfiguration dataStorageConfiguration) {
    // Check for archive mode first based on the data storage format
    if (dataStorageConfiguration.getDataStorageFormat() == DataStorageFormat.X_BONSAI_ARCHIVE) {
      return FlatDbMode.ARCHIVE;
    }
    return dataStorageConfiguration
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getFullFlatDbEnabled()
        ? FlatDbMode.FULL
        : FlatDbMode.PARTIAL;
  }

  @Override
  protected FlatDbMode alternativeFlatDbModeForExistingDatabase() {
    return FlatDbMode.PARTIAL;
  }

  public void upgradeToFullFlatDbMode(final SegmentedKeyValueStorage composedWorldStateStorage) {
    final SegmentedKeyValueStorageTransaction transaction =
        composedWorldStateStorage.startTransaction();
    LOG.info("setting FlatDbStrategy to FULL");
    transaction.put(
        TRIE_BRANCH_STORAGE, FLAT_DB_MODE, FlatDbMode.FULL.getVersion().toArrayUnsafe());
    transaction.commit();
    loadFlatDbStrategy(composedWorldStateStorage); // force reload of flat db reader strategy
  }

  public void downgradeToPartialFlatDbMode(
      final SegmentedKeyValueStorage composedWorldStateStorage) {
    final SegmentedKeyValueStorageTransaction transaction =
        composedWorldStateStorage.startTransaction();
    LOG.info("setting FlatDbStrategy to PARTIAL");
    transaction.put(
        TRIE_BRANCH_STORAGE, FLAT_DB_MODE, FlatDbMode.PARTIAL.getVersion().toArrayUnsafe());
    transaction.commit();
    loadFlatDbStrategy(composedWorldStateStorage); // force reload of flat db reader strategy
  }

  /**
   * Upgrades the FLAT_DB_MODE to ARCHIVE mode and reloads the strategy.
   *
   * @param composedWorldStateStorage the world state storage
   */
  public void upgradeToArchiveDbMode(final SegmentedKeyValueStorage composedWorldStateStorage) {
    LOG.info("Upgrading FLAT_DB_MODE to ARCHIVE");
    final SegmentedKeyValueStorageTransaction transaction =
        composedWorldStateStorage.startTransaction();
    transaction.put(
        TRIE_BRANCH_STORAGE, FLAT_DB_MODE, FlatDbMode.ARCHIVE.getVersion().toArrayUnsafe());
    transaction.commit();
    loadFlatDbStrategy(composedWorldStateStorage); // force reload of flat db reader strategy
  }

  @Override
  protected FlatDbStrategy createFlatDbStrategy(
      final FlatDbMode flatDbMode,
      final MetricsSystem metricsSystem,
      final CodeStorageStrategy codeStorageStrategy) {
    if (flatDbMode == FlatDbMode.FULL) {
      return new BonsaiFullFlatDbStrategy(metricsSystem, codeStorageStrategy);
    } else if (flatDbMode == FlatDbMode.ARCHIVE) {
      return new BonsaiArchiveFlatDbStrategy(metricsSystem, codeStorageStrategy);
    } else {
      return new BonsaiPartialFlatDbStrategy(metricsSystem, codeStorageStrategy);
    }
  }

  /**
   * Returns the archive flat DB strategy if the current mode is ARCHIVE, otherwise returns null.
   * This allows callers to access archive-specific methods like setWriteContext/clearWriteContext.
   *
   * @return the BonsaiArchiveFlatDbStrategy instance, or null if not in archive mode
   */
  public BonsaiArchiveFlatDbStrategy getArchiveFlatDbStrategy() {
    if (flatDbMode == FlatDbMode.ARCHIVE && flatDbStrategy instanceof BonsaiArchiveFlatDbStrategy) {
      return (BonsaiArchiveFlatDbStrategy) flatDbStrategy;
    }
    return null;
  }
}
