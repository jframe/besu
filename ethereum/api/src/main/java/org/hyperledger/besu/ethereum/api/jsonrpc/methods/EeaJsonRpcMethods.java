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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import org.hyperledger.besu.enclave.Enclave;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApi;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.privacy.methods.eea.EeaSendRawTransaction;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.privacy.methods.priv.PrivGetEeaTransactionCount;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.privacy.methods.priv.PrivateEeaNonceProvider;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.PrivacyParameters;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.privacy.PrivateTransactionHandler;

import java.util.Map;

public class EeaJsonRpcMethods extends PrivacyApiGroupJsonRpcMethods {

  private final JsonRpcParameter parameter = new JsonRpcParameter();

  public EeaJsonRpcMethods(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule<?> protocolSchedule,
      final TransactionPool transactionPool,
      final PrivacyParameters privacyParameters) {
    super(blockchainQueries, protocolSchedule, transactionPool, privacyParameters);
  }

  @Override
  protected RpcApi getApiGroup() {
    return RpcApis.EEA;
  }

  @Override
  protected Map<String, JsonRpcMethod> create(
      final PrivateTransactionHandler privateTransactionHandler, final Enclave enclave) {
    return mapOf(
        new EeaSendRawTransaction(privateTransactionHandler, getTransactionPool(), parameter),
        new PrivGetEeaTransactionCount(
            parameter, new PrivateEeaNonceProvider(enclave, privateTransactionHandler)));
  }
}
