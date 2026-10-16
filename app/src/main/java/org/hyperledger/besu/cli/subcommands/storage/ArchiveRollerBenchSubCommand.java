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
package org.hyperledger.besu.cli.subcommands.storage;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiArchiveWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeRoller;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTruncation;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/** Benchmark-only: times the archive trie-node roller over a fixed range. Not shipped. */
@Command(
    name = "x-archive-roller-bench",
    hidden = true,
    description = "Benchmark the Bonsai archive trie-node roller (development only).",
    mixinStandardHelpOptions = true)
public class ArchiveRollerBenchSubCommand implements Runnable {

  @SuppressWarnings("unused")
  @ParentCommand
  private StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Option(names = "--seed-mode", required = true)
  private ArchiveTrieNodeRoller.SeedMode seedMode;

  @Option(names = "--truncate-to", required = true)
  private long truncateTo;

  @Option(names = "--blocks", defaultValue = "500")
  private long blocks;

  @Option(names = "--timeout-minutes", defaultValue = "60")
  private long timeoutMinutes;

  @Override
  public void run() {
    final PrintWriter out = spec.commandLine().getOut();
    final BesuController controller = parentCommand.besuCommand.setupControllerBuilder().build();
    try {
      final BonsaiArchiveWorldStateProvider provider =
          (BonsaiArchiveWorldStateProvider) controller.getProtocolContext().getWorldStateArchive();
      final BonsaiWorldStateKeyValueStorage headStorage = provider.getWorldStateKeyValueStorage();
      final ArchiveTrieNodeStrategy strategy =
          (ArchiveTrieNodeStrategy) headStorage.getTrieNodeStrategy();
      final ArchiveCoverageTracker coverage = strategy.getCoverageTracker();
      final MutableBlockchain blockchain = controller.getProtocolContext().getBlockchain();
      final long head = blockchain.getChainHeadBlockNumber();
      final long target = truncateTo + blocks;
      checkArgument(target <= head, "truncate-to + blocks (%s) exceeds head %s", target, head);

      ArchiveTruncation.truncateTo(headStorage.getComposedWorldStateStorage(), truncateTo);
      // the controller armed the capture gate at best height - maxLayers/2; let it admit the range
      strategy.armFrontier(() -> target);
      out.printf(
          "truncated archive to %d (head %d), indexing %d blocks with %s%n",
          truncateTo, head, blocks, seedMode);
      out.flush();

      final long maxLayers =
          parentCommand
              .besuCommand
              .getDataStorageConfiguration()
              .getExtraStorageConfiguration()
              .getMaxLayersToLoad();
      final ArchiveTrieNodeRoller roller =
          new ArchiveTrieNodeRoller(
              provider,
              blockchain,
              strategy,
              () -> target,
              Executors.newSingleThreadScheduledExecutor(),
              new NoOpMetricsSystem(),
              maxLayers,
              1,
              256 /* MAX_BATCH */,
              seedMode);
      final List<Long> stamps = new ArrayList<>();
      final long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(timeoutMinutes);
      final long t0 = System.nanoTime();
      long seen = truncateTo;
      roller.start();
      while (seen < target) {
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException(
              "timed out at block " + seen + " (see roller errors above)");
        }
        final long now = coverage.lastIndexedBlock().orElse(-1L);
        if (now > seen) {
          final long ts = System.nanoTime();
          for (long b = seen + 1; b <= now; b++) {
            stamps.add(ts);
          }
          seen = now;
        }
        Thread.sleep(1);
      }
      roller.close();

      final double seconds = (stamps.get(stamps.size() - 1) - t0) / 1e9;
      final List<Double> perBlockMs = new ArrayList<>();
      long prev = t0;
      for (final long ts : stamps) {
        perBlockMs.add((ts - prev) / 1e6);
        prev = ts;
      }
      final List<Double> sorted = perBlockMs.stream().sorted().toList();
      out.printf(
          "RESULT seedMode=%s from=%d to=%d head=%d seconds=%.3f blocksPerSecond=%.3f p50Ms=%.1f p95Ms=%.1f%n",
          seedMode,
          truncateTo + 1,
          target,
          head,
          seconds,
          blocks / seconds,
          sorted.get(sorted.size() / 2),
          sorted.get((int) Math.floor(sorted.size() * 0.95)));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } finally {
      controller.close();
    }
  }
}
