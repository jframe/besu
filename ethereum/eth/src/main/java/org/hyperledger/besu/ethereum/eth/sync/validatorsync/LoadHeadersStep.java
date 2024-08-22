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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LoadHeadersStep implements Function<ValidatorSyncRange, Stream<BlockHeader>> {

  private final Blockchain blockchain;

  public LoadHeadersStep(final Blockchain blockchain) {
    this.blockchain = blockchain;
  }

  @Override
  public Stream<BlockHeader> apply(final ValidatorSyncRange validatorSyncRange) {
    long startBlockNumber = validatorSyncRange.lowerBlockNumber();
    long endBlockNumber = validatorSyncRange.upperBlockNumber();

    List<BlockHeader> headers =
        Stream.iterate(startBlockNumber, n -> n + 1)
            .limit(endBlockNumber - startBlockNumber)
            .map(blockchain::getBlockHeader)
            .flatMap(Optional::stream)
            .collect(Collectors.toList());
    checkForDuplicateHeaders(headers);
    return headers.stream();
  }

  private void checkForDuplicateHeaders(final List<BlockHeader> headers) {
    final Set<Hash> uniqueHeaders = new HashSet<>();
    for (BlockHeader header : headers) {
      if (!uniqueHeaders.add(header.getHash())) {
        throw new IllegalArgumentException("Duplicate headers found: " + header.getHash());
      }
    }
  }
}
