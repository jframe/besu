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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.Synchronizer;

/**
 * RPC method to force start the Bonsai archiver, bypassing normal readiness checks.
 *
 * <p>This method allows operators to manually start the archiver even if migration is still in
 * progress. Use with caution - should only be called when you're certain the database is in a
 * consistent state.
 */
public class DebugStartBonsaiArchiver implements JsonRpcMethod {

  private final Synchronizer synchronizer;

  public DebugStartBonsaiArchiver(final Synchronizer synchronizer) {
    this.synchronizer = synchronizer;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_START_BONSAI_ARCHIVER.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext request) {
    final boolean started = synchronizer.forceStartBonsaiArchiver();
    if (!started) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(),
          new JsonRpcError(
              RpcErrorType.INTERNAL_ERROR,
              "Failed to start archiver - not configured or not in archive mode"));
    }

    return new JsonRpcSuccessResponse(
        request.getRequest().getId(), "Bonsai archiver force started");
  }
}
