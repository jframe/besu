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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import org.apache.tuweni.bytes.Bytes;

/** Seam between trie encoding formats and the format-neutral {@link NodeLog}. */
public interface NodeCodecAdapter {

  /** Canonical node bytes -> format-neutral model. */
  NodeLog parse(Bytes nodeBytes);

  /**
   * Model -> canonical node bytes. MUST satisfy {@code encode(parse(x)).equals(x)} for every valid
   * node.
   */
  Bytes encode(NodeLog model);

  /** Fan-out of this trie encoding (16 for MPT, 2 for PBT). */
  int arity();

  /** Format tag persisted in metadata-byte bits [7:6] (0 = MPT, 1 = PBT). */
  int formatTag();
}
