/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.chainexport;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/** The Rlp block exporter. */
public class RlpBlockExporter extends BlockExporter {

  /**
   * Instantiates a new Rlp block exporter.
   *
   * @param blockchain the blockchain
   */
  public RlpBlockExporter(final Blockchain blockchain) {
    super(blockchain);
  }

  @Override
  protected void exportBlock(
      final FileOutputStream outputStream,
      final Block block,
      final List<TransactionReceipt> receipts)
      throws IOException {
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    block.writeTo(rlpOut);
    rlpOut.writeList(receipts, (r, o) -> r.writeToForStorage(o, true));
    rlpOut.endList();
    Bytes encoded = rlpOut.encoded();
    outputStream.write(encoded.toArrayUnsafe());
  }
}
