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
package org.hyperledger.besu.ethereum.eth.manager.task;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlockBody;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.PendingPeerRequest;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthPV62;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Requests bodies from a peer by header, matches up headers to bodies, and returns blocks. */
public class GetSyncBodiesFromPeerTask extends AbstractPeerRequestTask<List<SyncBlockBody>> {
  private static final Logger LOG = LoggerFactory.getLogger(GetSyncBodiesFromPeerTask.class);

  private final ProtocolSchedule protocolSchedule;
  private final List<BlockHeader> headers;
  private final Map<BodyIdentifier, List<BlockHeader>> bodyToHeaders = new HashMap<>();

  private GetSyncBodiesFromPeerTask(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final List<BlockHeader> headers,
      final MetricsSystem metricsSystem) {
    super(ethContext, EthPV62.GET_BLOCK_BODIES, metricsSystem);
    checkArgument(headers.size() > 0);
    this.protocolSchedule = protocolSchedule;

    this.headers = headers;
    headers.forEach(
        (header) -> {
          final BodyIdentifier bodyId = new BodyIdentifier(header);
          bodyToHeaders.putIfAbsent(bodyId, new ArrayList<>(headers.size()));
          bodyToHeaders.get(bodyId).add(header);
        });
  }

  public static GetSyncBodiesFromPeerTask forHeaders(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final List<BlockHeader> headers,
      final MetricsSystem metricsSystem) {
    return new GetSyncBodiesFromPeerTask(protocolSchedule, ethContext, headers, metricsSystem);
  }

  @Override
  protected PendingPeerRequest sendRequest() {
    final List<Hash> blockHashes =
        headers.stream().map(BlockHeader::getHash).collect(Collectors.toList());
    LOG.atTrace()
        .setMessage("Requesting {} bodies with hashes {}.")
        .addArgument(blockHashes.size())
        .addArgument(blockHashes)
        .log();
    final long minimumRequiredBlockNumber = headers.get(headers.size() - 1).getNumber();

    return sendRequestToPeer(
        peer -> {
          LOG.atTrace()
              .setMessage("Requesting {} bodies from peer {}.")
              .addArgument(blockHashes.size())
              .addArgument(peer)
              .log();
          return peer.getBodies(blockHashes);
        },
        minimumRequiredBlockNumber);
  }

  @Override
  protected Optional<List<SyncBlockBody>> processResponse(
      final boolean streamClosed, final MessageData message, final EthPeer peer) {
    if (streamClosed) {
      // All outstanding requests have been responded to, and we still haven't found the response
      // we wanted. It must have been empty or contain data that didn't match.
      peer.recordUselessResponse("bodies");
      return Optional.of(Collections.emptyList());
    }

    final BlockBodiesMessage bodiesMessage = BlockBodiesMessage.readFrom(message);
    final List<SyncBlockBody> bodies = bodiesMessage.syncBodies(protocolSchedule);
    if (bodies.size() == 0) {
      // Message contains no data - nothing to do
      LOG.debug("Message contains no data. Peer: {}", peer);
      return Optional.empty();
    } else if (bodies.size() > headers.size()) {
      // Message doesn't match our request - nothing to do
      LOG.debug("Message doesn't match our request. Peer: {}", peer);
      return Optional.empty();
    }

    // TODO: what do we have to do now?
    final List<SyncBlockBody> syncBlockBodies = new ArrayList<>(headers.size());
    for (final SyncBlockBody body : bodies) {
      final List<BlockHeader> headers = bodyToHeaders.get(new BodyIdentifier(body));
      if (headers == null) {
        // This message contains unrelated bodies - exit
        LOG.debug("This message contains unrelated bodies. Peer: {}", peer);
        return Optional.empty();
      }
      headers.forEach(h -> syncBlockBodies.add(body));
      // Clear processed headers
      headers.clear();
    }

    peer.recordResponseCount(bodies.size());
    LOG.atTrace()
        .setMessage(
            "Associated {} bodies with {} headers to get {} syncBlockBodies with these hashes: {}")
        .addArgument(bodies.size())
        .addArgument(headers.size())
        .addArgument(syncBlockBodies.size())
        .addArgument(() -> syncBlockBodies.stream().map(SyncBlockBody::toString).toList())
        .log();
    return Optional.of(syncBlockBodies);
  }

  static class BodyIdentifier {
    private final Bytes32 transactionsRoot;
    private final Bytes32 ommersHash;
    private final Bytes32 withdrawalsRoot;

    public BodyIdentifier(
        final Bytes32 transactionsRoot, final Bytes32 ommersHash, final Bytes32 withdrawalsRoot) {
      this.transactionsRoot = transactionsRoot;
      this.ommersHash = ommersHash;
      this.withdrawalsRoot = withdrawalsRoot;
    }

    public BodyIdentifier(final BlockBody body) {
      this(body.getTransactions(), body.getOmmers(), body.getWithdrawals());
    }

    public BodyIdentifier(
        final List<Transaction> transactions,
        final List<BlockHeader> ommers,
        final Optional<List<Withdrawal>> withdrawals) {
      this(
          BodyValidation.transactionsRoot(transactions),
          BodyValidation.ommersHash(ommers),
          withdrawals.map(BodyValidation::withdrawalsRoot).orElse(null));
    }

    public BodyIdentifier(final BlockHeader header) {
      this(
          header.getTransactionsRoot(),
          header.getOmmersHash(),
          header.getWithdrawalsRoot().orElse(null));
    }

    public BodyIdentifier(final SyncBlockBody syncBody) {
      this(syncBody.getTransactionsRoot(), syncBody.getOmmersHash(), syncBody.getWithdrawalsRoot());
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BodyIdentifier that = (BodyIdentifier) o;
      return Objects.equals(transactionsRoot, that.transactionsRoot)
          && Objects.equals(ommersHash, that.ommersHash)
          && Objects.equals(withdrawalsRoot, that.withdrawalsRoot);
    }

    @Override
    public int hashCode() {
      return Objects.hash(transactionsRoot, ommersHash, withdrawalsRoot);
    }
  }
}
