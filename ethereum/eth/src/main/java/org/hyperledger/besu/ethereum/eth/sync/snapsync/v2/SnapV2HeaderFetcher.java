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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Downloads block headers trusted through hash links: a known header, then its ancestors, each the
 * parent of the previous. Headers are only returned, never stored (cf. snap-new2 HeaderFetcher).
 */
public final class SnapV2HeaderFetcher {

  static final int HEADERS_PER_REQUEST = 192;
  // up to 10 peers per request
  private static final int RETRIES_AGAINST_OTHER_PEERS = 9;

  /** Requests up to {@code count} headers descending from {@code startHash}. */
  @FunctionalInterface
  public interface HeaderRequester {
    CompletableFuture<List<BlockHeader>> request(Hash startHash, long startNumber, int count);
  }

  private final HeaderRequester requester;
  private final int headersPerRequest;

  public SnapV2HeaderFetcher(final HeaderRequester requester, final int headersPerRequest) {
    this.requester = requester;
    this.headersPerRequest = headersPerRequest;
  }

  public static SnapV2HeaderFetcher create(
      final EthContext ethContext, final ProtocolSchedule protocolSchedule) {
    return new SnapV2HeaderFetcher(
        (startHash, startNumber, count) ->
            ethContext
                .getScheduler()
                .scheduleServiceTask(
                    () -> {
                      final PeerTaskExecutorResult<List<BlockHeader>> result =
                          ethContext
                              .getPeerTaskExecutor()
                              .execute(
                                  new GetHeadersFromPeerTask(
                                      startHash,
                                      startNumber,
                                      count,
                                      0,
                                      GetHeadersFromPeerTask.Direction.REVERSE,
                                      RETRIES_AGAINST_OTHER_PEERS,
                                      protocolSchedule));
                      if (result.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
                          || result.result().isEmpty()) {
                        return CompletableFuture.failedFuture(
                            new SnapV2MoveFailedException(
                                "Could not download headers below block "
                                    + (startNumber + 1)
                                    + ": "
                                    + result.responseCode()));
                      }
                      return CompletableFuture.completedFuture(result.result().get());
                    }),
        HEADERS_PER_REQUEST);
  }

  /**
   * Returns the headers from {@code lowest} up to {@code top} included, ascending, each linked to
   * the next by its hash.
   */
  public CompletableFuture<List<BlockHeader>> chainDown(final BlockHeader top, final long lowest) {
    final List<BlockHeader> descending = new ArrayList<>();
    descending.add(top);
    return walkDown(descending, lowest)
        .thenApply(
            headers -> {
              Collections.reverse(headers);
              return headers;
            });
  }

  private CompletableFuture<List<BlockHeader>> walkDown(
      final List<BlockHeader> descending, final long lowest) {
    final BlockHeader cursor = descending.getLast();
    if (cursor.getNumber() <= lowest) {
      return CompletableFuture.completedFuture(descending);
    }
    final int count = (int) Math.min(headersPerRequest, cursor.getNumber() - lowest);
    return requester
        .request(cursor.getParentHash(), cursor.getNumber() - 1, count)
        .thenCompose(
            batch -> {
              if (batch.isEmpty()) {
                return CompletableFuture.failedFuture(
                    new SnapV2MoveFailedException(
                        "No headers returned below block " + cursor.getNumber()));
              }
              final List<BlockHeader> linked = new ArrayList<>(batch.size() + 1);
              linked.add(cursor);
              linked.addAll(batch);
              try {
                verifyLinkage(cursor, linked);
              } catch (final RuntimeException e) {
                return CompletableFuture.failedFuture(
                    new SnapV2MoveFailedException(e.getMessage(), e));
              }
              for (final BlockHeader header : batch) {
                if (header.getNumber() < lowest) {
                  break;
                }
                descending.add(header);
              }
              return walkDown(descending, lowest);
            });
  }

  /**
   * Checks that {@code descending} starts at {@code top}, and that each header is the parent of the
   * one before it, one block lower.
   */
  static void verifyLinkage(final BlockHeader top, final List<BlockHeader> descending) {
    if (descending.isEmpty() || !descending.getFirst().getHash().equals(top.getHash())) {
      throw new IllegalArgumentException("headers do not start at block " + top.getNumber());
    }
    for (int i = 1; i < descending.size(); i++) {
      final BlockHeader child = descending.get(i - 1);
      final BlockHeader parent = descending.get(i);
      if (parent.getNumber() != child.getNumber() - 1
          || !parent.getHash().equals(child.getParentHash())) {
        throw new IllegalArgumentException(
            "header " + parent.getNumber() + " is not the parent of " + child.getNumber());
      }
    }
  }
}
