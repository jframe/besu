/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.bonsai;

import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;

public class BonsaiWorldStateKeyValueStorageFactory {

  private final DataStorageConfiguration dataStorageConfiguration;

  public BonsaiWorldStateKeyValueStorageFactory(
      final DataStorageConfiguration dataStorageConfiguration) {
    this.dataStorageConfiguration = dataStorageConfiguration;
  }

  public BonsaiWorldStateKeyValueStorage create(final StorageProvider storageProvider) {
    return dataStorageConfiguration.isBonsaiLightModeEnabled()
        ? new BonsaiLightWorldStateKeyValueStorage(storageProvider)
        : new BonsaiWorldStateKeyValueStorage(storageProvider);
  }
}
