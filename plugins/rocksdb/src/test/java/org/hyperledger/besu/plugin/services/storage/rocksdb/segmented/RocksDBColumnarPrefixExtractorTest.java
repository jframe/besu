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

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.nio.charset.StandardCharsets;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyOptions;

class RocksDBColumnarPrefixExtractorTest {

  @Test
  void returnsAppliedLengthForAccountArchiveSegment() {
    try (final ColumnFamilyOptions options = new ColumnFamilyOptions()) {
      final OptionalInt applied =
          RocksDBColumnarKeyValueStorage.applyPrefixExtractor(segmentWithPrefix(32), options);
      assertThat(applied).isEqualTo(OptionalInt.of(32));
    }
  }

  @Test
  void returnsAppliedLengthForStorageArchiveSegment() {
    try (final ColumnFamilyOptions options = new ColumnFamilyOptions()) {
      final OptionalInt applied =
          RocksDBColumnarKeyValueStorage.applyPrefixExtractor(segmentWithPrefix(64), options);
      assertThat(applied).isEqualTo(OptionalInt.of(64));
    }
  }

  @Test
  void returnsEmptyAndDoesNotApplyExtractorForSegmentWithoutPrefix() {
    try (final ColumnFamilyOptions options = new ColumnFamilyOptions()) {
      final OptionalInt applied =
          RocksDBColumnarKeyValueStorage.applyPrefixExtractor(segmentWithoutPrefix(), options);
      assertThat(applied).isEmpty();
    }
  }

  private static SegmentIdentifier segmentWithPrefix(final int length) {
    return new TestSegment(OptionalInt.of(length));
  }

  private static SegmentIdentifier segmentWithoutPrefix() {
    return new TestSegment(OptionalInt.empty());
  }

  private record TestSegment(OptionalInt prefix) implements SegmentIdentifier {
    @Override
    public String getName() {
      return "TEST";
    }

    @Override
    public byte[] getId() {
      return "TEST".getBytes(StandardCharsets.UTF_8);
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
    public OptionalInt prefixLength() {
      return prefix;
    }
  }
}
