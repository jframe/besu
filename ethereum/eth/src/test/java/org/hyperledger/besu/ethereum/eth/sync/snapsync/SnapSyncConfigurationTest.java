/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class SnapSyncConfigurationTest {

  @Test
  public void defaultFlatDbHealRangeCountIs256() {
    assertThat(SnapSyncConfiguration.getDefault().getFlatDbHealRangeCount())
        .isEqualTo(SnapSyncConfiguration.DEFAULT_FLAT_DB_HEAL_RANGE_COUNT);
  }

  @Test
  public void defaultFlatDbHealMaxOutstandingRequestsIs200() {
    assertThat(SnapSyncConfiguration.getDefault().getFlatDbHealMaxOutstandingRequests())
        .isEqualTo(SnapSyncConfiguration.DEFAULT_FLAT_DB_HEAL_MAX_OUTSTANDING_REQUESTS);
  }

  @Test
  public void defaultLocalFlatAccountCountToHealPerRequestIs1024() {
    assertThat(SnapSyncConfiguration.getDefault().getLocalFlatAccountCountToHealPerRequest())
        .isEqualTo(SnapSyncConfiguration.DEFAULT_LOCAL_FLAT_ACCOUNT_COUNT_TO_HEAL_PER_REQUEST);
  }
}
