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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiArchiver;

import java.util.Optional;

public class DebugTriggerBonsaiArchiver implements JsonRpcMethod {

  private final Optional<BonsaiArchiver> bonsaiArchiver;

  public DebugTriggerBonsaiArchiver(final Optional<BonsaiArchiver> bonsaiArchiver) {
    this.bonsaiArchiver = bonsaiArchiver;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_TRIGGER_BONSAI_ARCHIVER.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    if (bonsaiArchiver.isEmpty()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INTERNAL_ERROR);
    }

    final BonsaiArchiver archiver = bonsaiArchiver.get();
    archiver.triggerArchiving();

    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), true);
  }
}
