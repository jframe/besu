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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

/**
 * A snap/2 pivot move failed before anything was written; the world state is unchanged and the move
 * can be retried.
 */
public class SnapV2MoveFailedException extends RuntimeException {

  public SnapV2MoveFailedException(final String message) {
    super(message);
  }

  public SnapV2MoveFailedException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
