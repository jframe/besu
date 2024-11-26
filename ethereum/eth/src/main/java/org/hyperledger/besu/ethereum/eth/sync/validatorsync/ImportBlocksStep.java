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
package org.hyperledger.besu.ethereum.eth.sync.validatorsync;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportBlocksStep implements Consumer<List<Block>> {
  private static final Logger LOG = LoggerFactory.getLogger(ImportBlocksStep.class);
  private static final long PRINT_DELAY = TimeUnit.SECONDS.toMillis(30L);

  protected final ProtocolContext protocolContext;
  private final EthContext ethContext;
  private long accumulatedTime = 0L;
  private OptionalLong logStartBlock = OptionalLong.empty();
  private final BlockHeader pivotHeader;

  public ImportBlocksStep(
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final BlockHeader pivotHeader) {
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.pivotHeader = pivotHeader;
  }

  @Override
  public void accept(final List<Block> blocks) {
    final long startTime = System.nanoTime();
    for (final Block block : blocks) {
      protocolContext.getBlockchain().unsafeImportBlock(block);
      LOG.atTrace().setMessage("Imported block {}").addArgument(block::toLogString).log();
    }
    if (logStartBlock.isEmpty()) {
      logStartBlock = OptionalLong.of(blocks.get(0).getHeader().getNumber());
    }
    final long lastBlock = blocks.get(blocks.size() - 1).getHeader().getNumber();
    int peerCount = -1; // ethContext is not available in tests
    if (ethContext != null && ethContext.getEthPeers().peerCount() >= 0) {
      peerCount = ethContext.getEthPeers().peerCount();
    }
    final long endTime = System.nanoTime();

    accumulatedTime += TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS);
    if (accumulatedTime > PRINT_DELAY) {
      final long blocksPercent = getBlocksPercent(lastBlock, pivotHeader.getNumber());
      LOG.info(
          "Block import progress: {} of {} ({}%), Peer count: {}",
          lastBlock, pivotHeader.getNumber(), blocksPercent, peerCount);
      LOG.debug(
          "Completed importing chain segment {} to {} ({} blocks in {}ms), Peer count: {}",
          logStartBlock.getAsLong(),
          lastBlock,
          lastBlock - logStartBlock.getAsLong() + 1,
          accumulatedTime,
          peerCount);
      accumulatedTime = 0L;
      logStartBlock = OptionalLong.empty();
    }
  }

  @VisibleForTesting
  protected static long getBlocksPercent(final long lastBlock, final long totalBlocks) {
    if (totalBlocks == 0) {
      return 0;
    }
    return (100 * lastBlock / totalBlocks);
  }
}
