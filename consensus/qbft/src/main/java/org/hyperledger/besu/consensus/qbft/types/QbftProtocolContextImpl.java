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
package org.hyperledger.besu.consensus.qbft.types;

import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.qbft.core.QbftBlockInterface;
import org.hyperledger.besu.consensus.qbft.core.datatypes.ProtocolContext;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockImporter;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftBlockValidator;
import org.hyperledger.besu.consensus.qbft.core.datatypes.QbftValidatorProvider;

public class QbftProtocolContextImpl implements ProtocolContext {

  private final org.hyperledger.besu.ethereum.ProtocolContext besuProtocolContext;

  public QbftProtocolContextImpl(
      final org.hyperledger.besu.ethereum.ProtocolContext besuProtocolContext) {
    this.besuProtocolContext = besuProtocolContext;
  }

  @Override
  public QbftValidatorProvider getValidatorProvider() {
    return new QbftValidatorProviderImpl(
        besuProtocolContext.safeConsensusContext(BftContext.class).get().getValidatorProvider());
  }

  @Override
  public QbftBlockInterface getBlockInterface() {
    return new QbftBlockInterfaceImpl(
        besuProtocolContext.safeConsensusContext(BftContext.class).get().getBlockInterface());
  }

  @Override
  public QbftBlockValidator getBlockValidator() {
    return null;
  }

  @Override
  public QbftBlockImporter getBlockImporter() {
    return null;
  }
}
