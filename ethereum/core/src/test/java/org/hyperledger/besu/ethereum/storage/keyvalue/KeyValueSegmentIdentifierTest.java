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

import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

class KeyValueSegmentIdentifierTest {

  @Test
  void accountArchiveSegmentsHave32BytePrefix() {
    assertThat(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE.prefixLength())
        .isEqualTo(OptionalInt.of(32));
    assertThat(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_FREEZER.prefixLength())
        .isEqualTo(OptionalInt.of(32));
  }

  @Test
  void storageArchiveSegmentsHave64BytePrefix() {
    assertThat(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE.prefixLength())
        .isEqualTo(OptionalInt.of(64));
    assertThat(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_FREEZER.prefixLength())
        .isEqualTo(OptionalInt.of(64));
  }

  @Test
  void nonArchiveSegmentsHaveNoPrefixLength() {
    assertThat(KeyValueSegmentIdentifier.DEFAULT.prefixLength()).isEmpty();
    assertThat(KeyValueSegmentIdentifier.BLOCKCHAIN.prefixLength()).isEmpty();
    assertThat(KeyValueSegmentIdentifier.WORLD_STATE.prefixLength()).isEmpty();
    assertThat(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE.prefixLength()).isEmpty();
    assertThat(KeyValueSegmentIdentifier.CODE_STORAGE.prefixLength()).isEmpty();
    assertThat(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE.prefixLength()).isEmpty();
    assertThat(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE.prefixLength()).isEmpty();
    assertThat(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE.prefixLength()).isEmpty();
    assertThat(KeyValueSegmentIdentifier.VARIABLES.prefixLength()).isEmpty();
  }
}
