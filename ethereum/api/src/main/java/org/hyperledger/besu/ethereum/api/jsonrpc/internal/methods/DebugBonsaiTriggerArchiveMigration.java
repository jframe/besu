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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiArchiveFlatDbMigrator;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debug RPC method to manually trigger the Bonsai archive flat DB migration.
 * This is useful for testing and debugging the migration process.
 */
public class DebugBonsaiTriggerArchiveMigration implements JsonRpcMethod {

  private static final Logger LOG =
      LoggerFactory.getLogger(DebugBonsaiTriggerArchiveMigration.class);

  public DebugBonsaiTriggerArchiveMigration() {
    // No parameters needed - uses static getInstance()
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_BONSAI_TRIGGER_ARCHIVE_MIGRATION.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Optional<BonsaiArchiveFlatDbMigrator> migrator =
        BonsaiArchiveFlatDbMigrator.getInstance();

    if (migrator.isEmpty()) {
      LOG.warn("Bonsai archive migrator is not available");
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(),
          RpcErrorType.INTERNAL_ERROR);
    }

    try {
      LOG.info("Manually triggering Bonsai archive migration via debug RPC");
      migrator.get().onInitialSyncCompleted();
      return new JsonRpcSuccessResponse(
          requestContext.getRequest().getId(),
          "Bonsai archive migration triggered successfully");
    } catch (final Exception e) {
      LOG.error("Failed to trigger Bonsai archive migration", e);
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(),
          RpcErrorType.INTERNAL_ERROR);
    }
  }
}
