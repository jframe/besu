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
package org.hyperledger.besu.ethereum.eth.sync.validatorsync;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportReceiptsStep implements Consumer<Map<BlockHeader, List<TransactionReceipt>>> {
  private static final Logger LOG = LoggerFactory.getLogger(ImportReceiptsStep.class);
  protected final ProtocolContext protocolContext;

  public ImportReceiptsStep(final ProtocolContext protocolContext) {
    this.protocolContext = protocolContext;
  }

  @Override
  public void accept(final Map<BlockHeader, List<TransactionReceipt>> receiptsByBlock) {
    for (var entry : receiptsByBlock.entrySet()) {
      protocolContext
          .getBlockchain()
          .unsafeImportReceipts(entry.getKey().getHash(), entry.getValue());
    }
    LOG.info(
        "Imported receipts starting from {}",
        receiptsByBlock.keySet().iterator().next().getNumber());
  }
}
