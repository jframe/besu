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
package org.hyperledger.besu.cli.subcommands.storage.archivestats;

import org.hyperledger.besu.cli.subcommands.storage.StorageSubCommand;
import org.hyperledger.besu.cli.util.VersionProvider;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

/**
 * Scans a stopped Bonsai-archive node's archive column families and emits histograms, per-range
 * counters, and a Markdown summary describing the empirical distribution of modifications.
 */
@Command(
    name = "archive-stats",
    description =
        "Scan ACCOUNT_INFO_STATE_ARCHIVE and ACCOUNT_STORAGE_ARCHIVE column families "
            + "and emit distribution statistics. Requires the node to be stopped.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class ArchiveStatsSubCommand implements Runnable {

  @SuppressWarnings("unused")
  @ParentCommand
  private StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @Spec
  private CommandSpec spec;

  @Option(
      names = {"--output"},
      description = "Directory to write reports into (default: ${DEFAULT-VALUE})",
      defaultValue = "./archive-stats")
  Path output;

  @Option(
      names = {"--range-size"},
      description = "Block range partition size (default: ${DEFAULT-VALUE})",
      defaultValue = "1000000")
  long rangeSize;

  @Option(
      names = {"--fp-sweep"},
      description =
          "Comma-separated k:m bloom-filter sizing pairs to evaluate analytically "
              + "(default: ${DEFAULT-VALUE})",
      defaultValue = "7:1048576,7:2097152,10:1048576,10:2097152",
      split = ",")
  List<String> fpSweep;

  @Option(
      names = {"--account-class-boundaries"},
      description =
          "Four ascending modification-count thresholds for account class binning "
              + "(default: ${DEFAULT-VALUE})",
      defaultValue = "3,50,10000,1000000",
      split = ",")
  List<Long> accountClassBoundaries;

  @Option(
      names = {"--storage-class-boundaries"},
      description =
          "Four ascending modification-count thresholds for storage slot class binning "
              + "(default: ${DEFAULT-VALUE})",
      defaultValue = "1,10,1000,100000",
      split = ",")
  List<Long> storageClassBoundaries;

  @Option(
      names = {"--cf"},
      description = "Which CFs to scan: account, storage, or both (default: ${DEFAULT-VALUE})",
      defaultValue = "both")
  String cfSelector;

  @Option(
      names = {"--progress-interval-seconds"},
      description = "Progress log interval (default: ${DEFAULT-VALUE})",
      defaultValue = "30")
  long progressIntervalSeconds;

  @Option(
      names = {"--max-keys"},
      description = "Stop scanning each CF after this many keys (testing only)")
  Long maxKeys;

  @Option(
      names = {"--memory-budget-mb"},
      description = "Defensive memory budget in MB (default: ${DEFAULT-VALUE})",
      defaultValue = "1024")
  long memoryBudgetMb;

  @Override
  public void run() {
    final PrintWriter out = spec.commandLine().getOut();
    spec.commandLine().usage(out);
    out.println("(implementation pending — see plan)");
  }
}
