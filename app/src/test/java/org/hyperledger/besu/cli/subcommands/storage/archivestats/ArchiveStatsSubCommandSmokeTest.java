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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.cli.CommandTestAbstract;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ArchiveStatsSubCommandSmokeTest extends CommandTestAbstract {

  @Test
  public void storageHelpListsArchiveStatsSubCommand() {
    parseCommand("storage", "--help");

    assertThat(commandOutput.toString(UTF_8)).contains("archive-stats");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void archiveStatsHelpPrintsExpectedFlags() {
    parseCommand("storage", "archive-stats", "--help");

    final String output = commandOutput.toString(UTF_8);
    assertThat(output).contains("archive-stats");
    assertThat(output).contains("--output");
    assertThat(output).contains("--range-size");
    assertThat(output).contains("--fp-sweep");
    assertThat(output).contains("--account-class-boundaries");
    assertThat(output).contains("--storage-class-boundaries");
    assertThat(output).contains("--cf");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }
}
