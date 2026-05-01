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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_FREEZER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_FREEZER;

import java.util.EnumSet;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

class KeyValueSegmentIdentifierTest {

  private static final EnumSet<KeyValueSegmentIdentifier> ARCHIVE_SEGMENTS =
      EnumSet.of(
          ACCOUNT_INFO_STATE_ARCHIVE,
          ACCOUNT_STORAGE_ARCHIVE,
          ACCOUNT_INFO_STATE_FREEZER,
          ACCOUNT_STORAGE_FREEZER);

  @Test
  void accountArchiveSegmentsHave32BytePrefix() {
    assertThat(ACCOUNT_INFO_STATE_ARCHIVE.prefixLength()).isEqualTo(OptionalInt.of(32));
    assertThat(ACCOUNT_INFO_STATE_FREEZER.prefixLength()).isEqualTo(OptionalInt.of(32));
  }

  @Test
  void storageArchiveSegmentsHave64BytePrefix() {
    assertThat(ACCOUNT_STORAGE_ARCHIVE.prefixLength()).isEqualTo(OptionalInt.of(64));
    assertThat(ACCOUNT_STORAGE_FREEZER.prefixLength()).isEqualTo(OptionalInt.of(64));
  }

  @Test
  void allNonArchiveSegmentsHaveNoPrefixLength() {
    EnumSet.complementOf(ARCHIVE_SEGMENTS)
        .forEach(segment -> assertThat(segment.prefixLength()).as(segment.name()).isEmpty());
  }
}
