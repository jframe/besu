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
package org.hyperledger.besu.cli.subcommands.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConvertToForestMetadataTest {

  @Test
  void flipMetadataToForestWritesForestMetadataReadableByDatabaseMetadata(
      @TempDir final Path dataDir) throws IOException {
    new ConvertToForestSubCommand().flipMetadataToForest(dataDir);

    final DatabaseMetadata read = DatabaseMetadata.lookUpFrom(dataDir);

    assertThat(read.getVersionedStorageFormat().getFormat()).isEqualTo(DataStorageFormat.FOREST);
    assertThat(read.getVersionedStorageFormat().getVersion()).isEqualTo(3);
  }
}
