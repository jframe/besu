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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.junit.jupiter.api.Test;

class RocksDBColumnarKeyValueStorageMergeOperatorTest {

  private enum MergeTestSegment implements SegmentIdentifier {
    MERGE_SEGMENT(new byte[] {1}, true),
    DEFAULT("default".getBytes(StandardCharsets.UTF_8), false);

    private final byte[] id;
    private final boolean usesAppendMergeOperator;

    MergeTestSegment(final byte[] id, final boolean usesAppendMergeOperator) {
      this.id = id;
      this.usesAppendMergeOperator = usesAppendMergeOperator;
    }

    @Override
    public String getName() {
      return name();
    }

    @Override
    public byte[] getId() {
      return id;
    }

    @Override
    public boolean containsStaticData() {
      return false;
    }

    @Override
    public boolean isEligibleToHighSpecFlag() {
      return false;
    }

    @Override
    public boolean usesAppendMergeOperator() {
      return usesAppendMergeOperator;
    }
  }

  @Test
  void mergeConcatenatesWithoutDelimiterAndSurvivesCompaction() throws Exception {
    try (SegmentedKeyValueStorage storage =
        new OptimisticRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder()
                .databaseDir(Files.createTempDirectory("mergeOperatorTest"))
                .build(),
            List.of(MergeTestSegment.DEFAULT, MergeTestSegment.MERGE_SEGMENT),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      final byte[] key = "k".getBytes(StandardCharsets.UTF_8);
      final SegmentedKeyValueStorageTransaction tx1 = storage.startTransaction();
      tx1.merge(MergeTestSegment.MERGE_SEGMENT, key, new byte[] {1, 2, 3});
      tx1.commit();

      final SegmentedKeyValueStorageTransaction tx2 = storage.startTransaction();
      tx2.merge(MergeTestSegment.MERGE_SEGMENT, key, new byte[] {4, 5, 6});
      tx2.commit();

      assertThat(storage.get(MergeTestSegment.MERGE_SEGMENT, key))
          .contains(new byte[] {1, 2, 3, 4, 5, 6});
    }
  }
}
