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
package org.hyperledger.besu.ethereum.eth.sync.validatorsync;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class LoadHeadersStep
    implements Function<ValidatorSyncRange, CompletableFuture<List<BlockHeader>>> {

  private final Blockchain blockchain;

  public LoadHeadersStep(final Blockchain blockchain) {
    this.blockchain = blockchain;
  }

  @Override
  public CompletableFuture<List<BlockHeader>> apply(final ValidatorSyncRange validatorSyncRange) {
    long startBlockNumber = validatorSyncRange.lowerBlockNumber();
    long endBlockNumber = validatorSyncRange.upperBlockNumber();

    return CompletableFuture.supplyAsync(
        () -> {
          List<BlockHeader> headers = new ArrayList<>();
          for (long blockNumber = startBlockNumber; blockNumber <= endBlockNumber; blockNumber++) {
            blockchain.getBlockHeader(blockNumber).ifPresent(headers::add);
          }
          return headers;
        });
  }
}
