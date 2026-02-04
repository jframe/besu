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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Optional;
import java.util.function.Supplier;

public class BonsaiArchiveWorldState extends BonsaiWorldState {

  private Optional<BonsaiContext> readContext = Optional.empty();
  private Optional<BonsaiContext> writeContext = Optional.empty();

  public BonsaiArchiveWorldState(
      final BonsaiWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    super(archive, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache);
  }

  public Supplier<Optional<BonsaiContext>> getReadContextSupplier() {
    return () -> readContext;
  }

  public Supplier<Optional<BonsaiContext>> getWriteContextSupplier() {
    return () -> writeContext;
  }

  public void setReadContext(final BonsaiContext context) {
    this.readContext = Optional.of(context);
  }

  public void setWriteContext(final BonsaiContext context) {
    this.writeContext = Optional.of(context);
  }

  public void clearReadContext() {
    this.readContext = Optional.empty();
  }

  public void clearWriteContext() {
    this.writeContext = Optional.empty();
  }
}
