/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.cli.options;

import picocli.CommandLine;

/** The RLP encoding CLI options. */
public class RLPOptions {

  @CommandLine.Option(
      hidden = true,
      names = {"--Xrlp-optimization-enabled"},
      description =
          "Enable optimized RLP encoder using pre-allocated buffers and thread-local pooling. "
              + "This optimization reduces CPU overhead during block processing and sync. "
              + "(default: ${DEFAULT-VALUE})",
      fallbackValue = "false",
      arity = "1")
  private final Boolean rlpOptimizationEnabled = Boolean.FALSE;

  /** Default constructor. */
  RLPOptions() {}

  /**
   * Create RLP options.
   *
   * @return the RLP options
   */
  public static RLPOptions create() {
    return new RLPOptions();
  }

  /**
   * Whether RLP optimization is enabled.
   *
   * @return true if enabled, false otherwise.
   */
  public Boolean isRlpOptimizationEnabled() {
    return rlpOptimizationEnabled;
  }
}
