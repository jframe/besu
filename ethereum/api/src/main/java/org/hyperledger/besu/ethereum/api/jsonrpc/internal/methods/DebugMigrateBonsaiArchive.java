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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbToArchiveMigrator.MigrationStrategy;

import java.util.Locale;

/**
 * RPC method to trigger migration of Bonsai flat DB from FULL mode to ARCHIVE mode.
 *
 * <p>This method allows operators to manually trigger the archive migration process. The migration
 * will process trie logs from the specified start block to end block, converting the flat database
 * to archive format with block number suffixes on all state keys.
 *
 * <p>Parameters: - startBlock (optional): The starting block number (defaults to 0) - endBlock
 * (optional): The ending block number (defaults to chain head) - strategy (optional): The migration
 * strategy to use: "sequential", "prefetch", or "full_pipeline" (defaults to "sequential")
 *
 * <p>The migration runs asynchronously in the background and returns immediately with a success
 * response indicating the migration has started.
 */
public class DebugMigrateBonsaiArchive implements JsonRpcMethod {

  private final Blockchain blockchain;
  private final Synchronizer synchronizer;

  public DebugMigrateBonsaiArchive(final Blockchain blockchain, final Synchronizer synchronizer) {
    this.blockchain = blockchain;
    this.synchronizer = synchronizer;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_MIGRATE_BONSAI_ARCHIVE.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext request) {
    final long chainHeadNumber = blockchain.getChainHeadBlockNumber();

    final long startBlock;
    try {
      startBlock = request.getOptionalParameter(0, Long.class).orElse(0L);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid start block parameter (index 0)", RpcErrorType.INVALID_PARAMS, e);
    }

    final long endBlock;
    try {
      endBlock = request.getOptionalParameter(1, Long.class).orElse(chainHeadNumber);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid end block parameter (index 1)", RpcErrorType.INVALID_PARAMS, e);
    }

    // Validate parameters
    if (startBlock < 0) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(),
          new JsonRpcError(RpcErrorType.INVALID_PARAMS, "Start block must be non-negative"));
    }

    if (endBlock < startBlock) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(),
          new JsonRpcError(
              RpcErrorType.INVALID_PARAMS,
              "End block must be greater than or equal to start block"));
    }

    if (endBlock > chainHeadNumber) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(),
          new JsonRpcError(
              RpcErrorType.INVALID_PARAMS,
              "End block exceeds chain head (" + chainHeadNumber + ")"));
    }

    // Parse optional strategy parameter (defaults to SEQUENTIAL)
    final MigrationStrategy strategy;
    try {
      final String strategyString =
          request.getOptionalParameter(2, String.class).orElse("sequential");
      strategy = parseStrategy(strategyString);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid strategy parameter (index 2)", RpcErrorType.INVALID_PARAMS, e);
    } catch (IllegalArgumentException e) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(),
          new JsonRpcError(
              RpcErrorType.INVALID_PARAMS,
              "Invalid strategy. Must be one of: sequential, prefetch, full_pipeline"));
    }

    // Pass resetProgress=true to override any saved progress when explicitly called via RPC
    final boolean started =
        synchronizer.migrateToBonsaiArchive(startBlock, endBlock, true, strategy);
    if (!started) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(),
          new JsonRpcError(RpcErrorType.INTERNAL_ERROR, "Failed to start migration"));
    }

    return new JsonRpcSuccessResponse(
        request.getRequest().getId(),
        "Migration started from block "
            + startBlock
            + " to block "
            + endBlock
            + " using "
            + strategy
            + " strategy");
  }

  private MigrationStrategy parseStrategy(final String strategyString) {
    return switch (strategyString.toLowerCase(Locale.ROOT)) {
      case "sequential" -> MigrationStrategy.SEQUENTIAL;
      case "prefetch" -> MigrationStrategy.PREFETCH;
      case "full_pipeline" -> MigrationStrategy.FULL_PIPELINE;
      default -> throw new IllegalArgumentException("Unknown strategy: " + strategyString);
    };
  }
}
