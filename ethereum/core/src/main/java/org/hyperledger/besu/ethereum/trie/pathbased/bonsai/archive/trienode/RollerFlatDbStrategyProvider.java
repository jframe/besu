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

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

/**
 * A flat-DB strategy provider fixed to {@link RollerFlatDbStrategy}; nothing is read from storage.
 */
final class RollerFlatDbStrategyProvider extends BonsaiFlatDbStrategyProvider {

  RollerFlatDbStrategyProvider(final CodeStorageStrategy codeStorageStrategy) {
    super(new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    this.flatDbMode = FlatDbMode.PARTIAL;
    this.flatDbStrategy = new RollerFlatDbStrategy(codeStorageStrategy);
  }

  @Override
  public void loadFlatDbStrategy(final SegmentedKeyValueStorage composedWorldStateStorage) {
    // fixed strategy: never derived from storage
  }
}
