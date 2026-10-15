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

import org.hyperledger.besu.ethereum.core.Synchronizer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.LongSupplier;

/** Used for every data storage format other than Bonsai archive; only releases the executors. */
final class NoOpBonsaiArchive implements BonsaiArchive {

  private final List<ExecutorService> executors;

  NoOpBonsaiArchive(final ExecutorService... executors) {
    this.executors = List.of(executors);
  }

  @Override
  public void start(final Synchronizer synchronizer, final LongSupplier bestChainHeight) {
    // nothing to start
  }

  @Override
  public void close() {
    executors.forEach(ExecutorService::shutdownNow);
  }
}
