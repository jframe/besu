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
package org.hyperledger.besu.ethereum.eth.manager.peertask.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.PeerReputation;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.GetReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.ReceiptsMessage;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GetSyncReceiptsFromPeerTaskTest {

  @Test
  public void testGetSubProtocol() {
    GetSyncReceiptsFromPeerTask task =
        new GetSyncReceiptsFromPeerTask(Collections.emptyList(), null);
    assertThat(task.getSubProtocol()).isEqualTo(EthProtocol.get());
  }

  @Test
  public void testGetRequestMessage() {
    BlockHeader blockHeader1 = mockBlockHeader(1);
    TransactionReceipt receiptForBlock1 =
        new TransactionReceipt(1, 123, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader1.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock1)));

    BlockHeader blockHeader2 = mockBlockHeader(2);
    TransactionReceipt receiptForBlock2 =
        new TransactionReceipt(1, 456, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader2.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock2)));

    BlockHeader blockHeader3 = mockBlockHeader(3);
    TransactionReceipt receiptForBlock3 =
        new TransactionReceipt(1, 789, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader3.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock3)));

    GetSyncReceiptsFromPeerTask task =
        new GetSyncReceiptsFromPeerTask(List.of(blockHeader1, blockHeader2, blockHeader3), null);

    MessageData messageData = task.getRequestMessage();
    GetReceiptsMessage getReceiptsMessage = GetReceiptsMessage.readFrom(messageData);

    assertThat(getReceiptsMessage.getCode()).isEqualTo(EthProtocolMessages.GET_RECEIPTS);
    Iterable<Hash> hashesInMessage = getReceiptsMessage.hashes();
    List<Hash> expectedHashes =
        List.of(
            Hash.fromHexString(StringUtils.repeat("00", 31) + "11"),
            Hash.fromHexString(StringUtils.repeat("00", 31) + "21"),
            Hash.fromHexString(StringUtils.repeat("00", 31) + "31"));
    List<Hash> actualHashes = new ArrayList<>();
    hashesInMessage.forEach(actualHashes::add);

    assertThat(actualHashes).hasSize(3);
    assertThat(actualHashes.stream().sorted().toList())
        .isEqualTo(expectedHashes.stream().sorted().toList());
  }

  @Test
  public void testProcessResponseWithNullResponseMessage() {
    GetSyncReceiptsFromPeerTask task =
        new GetSyncReceiptsFromPeerTask(Collections.emptyList(), null);
    assertThatThrownBy(() -> task.processResponse(null))
        .isInstanceOf(InvalidPeerTaskResponseException.class);
  }

  @Test
  public void testProcessResponseForInvalidResponse() {
    BlockHeader blockHeader1 = mockBlockHeader(1);
    TransactionReceipt receiptForBlock1 =
        new TransactionReceipt(1, 123, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader1.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock1)));

    BlockHeader blockHeader2 = mockBlockHeader(2);
    TransactionReceipt receiptForBlock2 =
        new TransactionReceipt(1, 456, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader2.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock2)));

    BlockHeader blockHeader3 = mockBlockHeader(3);
    TransactionReceipt receiptForBlock3 =
        new TransactionReceipt(1, 789, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader3.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock3)));

    GetSyncReceiptsFromPeerTask task =
        new GetSyncReceiptsFromPeerTask(List.of(blockHeader1, blockHeader2, blockHeader3), null);
    ReceiptsMessage receiptsMessage =
        ReceiptsMessage.create(
            List.of(
                List.of(receiptForBlock1),
                List.of(receiptForBlock2),
                List.of(receiptForBlock3),
                List.of(
                    new TransactionReceipt(1, 101112, Collections.emptyList(), Optional.empty()))),
            TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);

    assertThatThrownBy(() -> task.processResponse(receiptsMessage))
        .isInstanceOf(InvalidPeerTaskResponseException.class);
  }

  @Test
  public void testProcessResponse() throws InvalidPeerTaskResponseException {
    BlockHeader blockHeader1 = mockBlockHeader(1);
    TransactionReceipt receiptForBlock1 =
        new TransactionReceipt(1, 123, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader1.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock1)));

    BlockHeader blockHeader2 = mockBlockHeader(2);
    TransactionReceipt receiptForBlock2 =
        new TransactionReceipt(1, 456, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader2.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock2)));

    BlockHeader blockHeader3 = mockBlockHeader(3);
    TransactionReceipt receiptForBlock3 =
        new TransactionReceipt(1, 789, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader3.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock3)));

    BlockHeader blockHeader4 = mockBlockHeader(4);
    Mockito.when(blockHeader4.getReceiptsRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);

    GetSyncReceiptsFromPeerTask task =
        new GetSyncReceiptsFromPeerTask(
            List.of(blockHeader1, blockHeader2, blockHeader3, blockHeader4), null);

    ReceiptsMessage receiptsMessage =
        ReceiptsMessage.create(
            List.of(
                List.of(receiptForBlock1), List.of(receiptForBlock2), List.of(receiptForBlock3)),
            TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);

    Map<BlockHeader, List<SyncTransactionReceipt>> resultMap =
        task.processResponse(receiptsMessage);

    assertThat(resultMap).hasSize(4);
    assertThat(resultMap.get(blockHeader4)).isEmpty();
    assertThat(resultMap.get(blockHeader1)).hasSize(1);
    assertThat(resultMap.get(blockHeader2)).hasSize(1);
    assertThat(resultMap.get(blockHeader3)).hasSize(1);

    // Verify that the sync receipts can be decoded back to the original receipts
    assertThat(resultMap.get(blockHeader1).getFirst().getReceiptSupplier().get())
        .isEqualTo(receiptForBlock1);
    assertThat(resultMap.get(blockHeader2).getFirst().getReceiptSupplier().get())
        .isEqualTo(receiptForBlock2);
    assertThat(resultMap.get(blockHeader3).getFirst().getReceiptSupplier().get())
        .isEqualTo(receiptForBlock3);
  }

  @Test
  public void testProcessResponseForOnlyPrefilledEmptyTrieReceiptsRoots()
      throws InvalidPeerTaskResponseException {
    BlockHeader blockHeader1 = mockBlockHeader(1);
    Mockito.when(blockHeader1.getReceiptsRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);

    GetSyncReceiptsFromPeerTask task = new GetSyncReceiptsFromPeerTask(List.of(blockHeader1), null);

    ReceiptsMessage receiptsMessage =
        ReceiptsMessage.create(
            Collections.emptyList(),
            TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);

    Map<BlockHeader, List<SyncTransactionReceipt>> resultMap =
        task.processResponse(receiptsMessage);

    assertThat(resultMap).hasSize(1);
    assertThat(resultMap.get(blockHeader1)).isEmpty();
  }

  @Test
  public void testProcessResponseWithMultipleBlocksHavingSameReceiptsRoot()
      throws InvalidPeerTaskResponseException {
    // Create two blocks with the same receipts root
    BlockHeader blockHeader1 = mockBlockHeader(1);
    BlockHeader blockHeader2 = mockBlockHeader(2);
    TransactionReceipt receipt =
        new TransactionReceipt(1, 123, Collections.emptyList(), Optional.empty());

    // Create sync receipt to compute the correct receipts root
    SyncTransactionReceipt syncReceipt = createSyncReceipt(receipt);
    Hash receiptsRoot = BodyValidation.syncReceiptsRoot(List.of(syncReceipt));

    Mockito.when(blockHeader1.getReceiptsRoot()).thenReturn(receiptsRoot);
    Mockito.when(blockHeader2.getReceiptsRoot()).thenReturn(receiptsRoot);

    GetSyncReceiptsFromPeerTask task =
        new GetSyncReceiptsFromPeerTask(List.of(blockHeader1, blockHeader2), null);

    ReceiptsMessage receiptsMessage =
        ReceiptsMessage.create(
            List.of(List.of(receipt)),
            TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);

    Map<BlockHeader, List<SyncTransactionReceipt>> resultMap =
        task.processResponse(receiptsMessage);

    // Both blocks should have the same receipts
    assertThat(resultMap).hasSize(2);
    assertThat(resultMap.get(blockHeader1)).hasSize(1);
    assertThat(resultMap.get(blockHeader2)).hasSize(1);

    // Verify the sync receipts compute the same root
    assertThat(BodyValidation.syncReceiptsRoot(resultMap.get(blockHeader1)))
        .isEqualTo(receiptsRoot);
    assertThat(BodyValidation.syncReceiptsRoot(resultMap.get(blockHeader2)))
        .isEqualTo(receiptsRoot);

    // Verify the receipts can be decoded back
    assertThat(resultMap.get(blockHeader1).getFirst().getReceiptSupplier().get())
        .isEqualTo(receipt);
    assertThat(resultMap.get(blockHeader2).getFirst().getReceiptSupplier().get())
        .isEqualTo(receipt);
  }

  @Test
  public void testGetPeerRequirementFilter() {
    BlockHeader blockHeader1 = mockBlockHeader(1);
    TransactionReceipt receiptForBlock1 =
        new TransactionReceipt(1, 123, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader1.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock1)));

    BlockHeader blockHeader2 = mockBlockHeader(2);
    TransactionReceipt receiptForBlock2 =
        new TransactionReceipt(1, 456, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader2.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock2)));

    BlockHeader blockHeader3 = mockBlockHeader(3);
    TransactionReceipt receiptForBlock3 =
        new TransactionReceipt(1, 789, Collections.emptyList(), Optional.empty());
    Mockito.when(blockHeader3.getReceiptsRoot())
        .thenReturn(BodyValidation.receiptsRoot(List.of(receiptForBlock3)));

    ProtocolSchedule protocolSchedule = Mockito.mock(ProtocolSchedule.class);
    Mockito.when(protocolSchedule.anyMatch(Mockito.any())).thenReturn(false);

    GetSyncReceiptsFromPeerTask task =
        new GetSyncReceiptsFromPeerTask(
            List.of(blockHeader1, blockHeader2, blockHeader3), protocolSchedule);

    EthPeer failForShortChainHeight = mockPeer(1);
    EthPeer successfulCandidate = mockPeer(5);

    assertThat(
            task.getPeerRequirementFilter()
                .test(EthPeerImmutableAttributes.from(failForShortChainHeight)))
        .isFalse();
    assertThat(
            task.getPeerRequirementFilter()
                .test(EthPeerImmutableAttributes.from(successfulCandidate)))
        .isTrue();
  }

  @Test
  public void testValidateResultForNoResults() {
    GetSyncReceiptsFromPeerTask task =
        new GetSyncReceiptsFromPeerTask(Collections.emptyList(), null);

    assertThat(task.validateResult(Collections.emptyMap()))
        .isEqualTo(PeerTaskValidationResponse.NO_RESULTS_RETURNED);
  }

  @Test
  public void testValidateResultForValidResults() {
    GetSyncReceiptsFromPeerTask task =
        new GetSyncReceiptsFromPeerTask(Collections.emptyList(), null);

    Map<BlockHeader, List<SyncTransactionReceipt>> map = new HashMap<>();
    map.put(mockBlockHeader(1), Collections.emptyList());

    assertThat(task.validateResult(map))
        .isEqualTo(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);
  }

  @Test
  public void testSyncReceiptsComputeSameReceiptsRoot() throws InvalidPeerTaskResponseException {
    BlockHeader blockHeader = mockBlockHeader(1);
    TransactionReceipt receipt1 =
        new TransactionReceipt(1, 100, Collections.emptyList(), Optional.empty());
    TransactionReceipt receipt2 =
        new TransactionReceipt(1, 200, Collections.emptyList(), Optional.empty());
    List<TransactionReceipt> receipts = List.of(receipt1, receipt2);

    // Create sync receipts to compute the expected receipts root
    SyncTransactionReceipt syncReceipt1 = createSyncReceipt(receipt1);
    SyncTransactionReceipt syncReceipt2 = createSyncReceipt(receipt2);
    List<SyncTransactionReceipt> expectedSyncReceipts = List.of(syncReceipt1, syncReceipt2);
    Hash expectedRoot = BodyValidation.syncReceiptsRoot(expectedSyncReceipts);

    Mockito.when(blockHeader.getReceiptsRoot()).thenReturn(expectedRoot);

    GetSyncReceiptsFromPeerTask task = new GetSyncReceiptsFromPeerTask(List.of(blockHeader), null);

    ReceiptsMessage receiptsMessage =
        ReceiptsMessage.create(
            List.of(receipts),
            TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);

    Map<BlockHeader, List<SyncTransactionReceipt>> resultMap =
        task.processResponse(receiptsMessage);

    // Verify that the sync receipts from the response compute the same root
    List<SyncTransactionReceipt> actualSyncReceipts = resultMap.get(blockHeader);
    Hash actualRoot = BodyValidation.syncReceiptsRoot(actualSyncReceipts);

    assertThat(actualRoot).isEqualTo(expectedRoot);

    // Also verify that fully decoded receipts produce the same root
    Hash fullReceiptsRoot = BodyValidation.receiptsRoot(receipts);
    assertThat(actualRoot).isEqualTo(fullReceiptsRoot);
  }

  private BlockHeader mockBlockHeader(final long blockNumber) {
    BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    Mockito.when(blockHeader.getNumber()).thenReturn(blockNumber);
    // second to last hex digit indicates the blockNumber, last hex digit indicates the usage of
    // the hash
    Mockito.when(blockHeader.getHash())
        .thenReturn(Hash.fromHexString(StringUtils.repeat("00", 31) + blockNumber + "1"));

    return blockHeader;
  }

  private EthPeer mockPeer(final long chainHeight) {
    EthPeer ethPeer = Mockito.mock(EthPeer.class);
    ChainState chainState = Mockito.mock(ChainState.class);

    Mockito.when(ethPeer.chainState()).thenReturn(chainState);
    Mockito.when(chainState.getEstimatedHeight()).thenReturn(chainHeight);
    Mockito.when(chainState.getEstimatedTotalDifficulty()).thenReturn(Difficulty.of(0));
    Mockito.when(ethPeer.getReputation()).thenReturn(new PeerReputation());
    PeerConnection connection = mock(PeerConnection.class);
    Mockito.when(ethPeer.getConnection()).thenReturn(connection);
    return ethPeer;
  }

  /**
   * Helper method to create a SyncTransactionReceipt from a TransactionReceipt. This mimics what
   * ReceiptsMessage.syncReceipts() does - encodes receipts and then reads them back as
   * SyncTransactionReceipts.
   */
  private SyncTransactionReceipt createSyncReceipt(final TransactionReceipt receipt) {
    // Create a receipts message and read it back as sync receipts
    ReceiptsMessage receiptsMessage =
        ReceiptsMessage.create(
            List.of(List.of(receipt)),
            TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);
    List<List<SyncTransactionReceipt>> syncReceiptsList = receiptsMessage.syncReceipts();
    return syncReceiptsList.getFirst().getFirst();
  }
}
