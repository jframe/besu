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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.HistoryKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeHistoryReader;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Simulates a mid-batch crash by running a migration to completion, then constructing a second,
 * independent migrator over the same storage and confirming it re-anchors from the persisted
 * progress and history (not from any in-memory state, since none is shared) and continues
 * correctly.
 */
class ArchiveMigrationCrashResumeTest extends BonsaiFlatDbToArchiveMigratorTestBase {

  @Test
  void secondMigratorInstanceResumesFromPersistedHistoryAfterFirstStops() {
    final var firstMigrator = createMigratorWithRealTrieLogsAndArchiveTrieBuilder();
    appendBlocks(10);
    firstMigrator.migrate().join();
    final long progressAfterFirstRun = firstMigrator.getMigrationProgress().orElseThrow();
    firstMigrator.close();

    appendBlocks(5); // simulate more blocks arriving before the "restart"

    final var secondMigrator = createMigratorWithRealTrieLogsAndArchiveTrieBuilder();
    secondMigrator.migrate().join();

    assertThat(secondMigrator.getMigrationProgress())
        .hasValueSatisfying(p -> assertThat(p).isGreaterThan(progressAfterFirstRun));

    // The account trie root for the final block must be present in the shared history CF,
    // confirming that the second migrator correctly re-anchored from committed storage rather
    // than any discarded in-memory state.
    final TrieNodeHistoryReader reader = new TrieNodeHistoryReader(storage);
    assertThat(
            reader.nodeAt(
                HistoryKey.DOMAIN_ACCOUNT,
                Bytes.EMPTY,
                secondMigrator.getMigrationProgress().get()))
        .isPresent();
  }
}
