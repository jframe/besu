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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.cli.util.VersionProvider;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class ArchiveStatsSubCommandSmokeTest {

  @Test
  void usagePrintsExpectedFlags() {
    final StringWriter out = new StringWriter();
    final CommandLine cmd =
        new CommandLine(new ArchiveStatsSubCommand(new PrintWriter(out)), new TestFactory());
    cmd.usage(new PrintWriter(out));
    final String text = out.toString();
    assertThat(text).contains("archive-stats");
    assertThat(text).contains("--output");
    assertThat(text).contains("--range-size");
    assertThat(text).contains("--fp-sweep");
    assertThat(text).contains("--account-class-boundaries");
    assertThat(text).contains("--storage-class-boundaries");
    assertThat(text).contains("--cf");
  }

  /**
   * Picocli factory that supplies a stub {@link VersionProvider} (which has no public no-arg
   * constructor and so cannot be created by Picocli's default factory).
   */
  private static final class TestFactory implements CommandLine.IFactory {
    private final CommandLine.IFactory defaultFactory = CommandLine.defaultFactory();

    @Override
    public <K> K create(final Class<K> cls) throws Exception {
      if (VersionProvider.class.equals(cls)) {
        return cls.cast(new VersionProvider(Map::of));
      }
      return defaultFactory.create(cls);
    }
  }
}
