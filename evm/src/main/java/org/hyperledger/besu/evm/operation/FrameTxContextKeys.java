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
package org.hyperledger.besu.evm.operation;

/** Context-variable keys used by FRAME transaction (EIP-8141) operations inside a MessageFrame. */
public final class FrameTxContextKeys {

  /** The approval scope set by the APPROVE opcode in a VERIFY frame. */
  public static final String FRAME_TX_APPROVAL_SCOPE = "FRAME_TX_APPROVAL_SCOPE";

  /** The index (0-based) of the currently executing frame within the FRAME transaction. */
  public static final String FRAME_TX_FRAME_INDEX = "FRAME_TX_FRAME_INDEX";

  /** Boolean flag: {@code true} if the current frame is executing in VERIFY mode. */
  public static final String FRAME_TX_IN_VERIFY = "FRAME_TX_IN_VERIFY";

  /**
   * The RLP-encoded transaction bytes, used by TXPARAMCOPY / TXPARAMSIZE to expose the raw
   * transaction data to EVM code.
   */
  public static final String FRAME_TX_PARAMS_BYTES = "FRAME_TX_PARAMS_BYTES";

  /**
   * The {@link org.hyperledger.besu.datatypes.Transaction} being executed, used by TXPARAMLOAD to
   * read individual transaction fields.
   */
  public static final String FRAME_TX_TRANSACTION = "FRAME_TX_TRANSACTION";

  private FrameTxContextKeys() {}
}
