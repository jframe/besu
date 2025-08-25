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
package org.hyperledger.besu.ethereum.eth.sync.fastsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LoadHeadersStepTest {

  @Mock private Blockchain blockchain;
  private LoadHeadersStep loadHeadersStep;

  @BeforeEach
  void setUp() {
    loadHeadersStep = new LoadHeadersStep(blockchain);
  }

  @Test
  void shouldLoadHeadersSuccessfully() throws ExecutionException, InterruptedException {
    // Given
    final long startBlock = 100L;
    final long endBlock = 105L;
    final SyncTargetNumberRange range = new SyncTargetNumberRange(startBlock, endBlock);

    // Create headers for the range
    for (long i = startBlock; i < endBlock; i++) {
      BlockHeader header = new BlockHeaderTestFixture().number(i).buildHeader();
      when(blockchain.getBlockHeader(i)).thenReturn(Optional.of(header));
    }

    // When
    CompletableFuture<List<BlockHeader>> future = loadHeadersStep.apply(range);
    List<BlockHeader> headers = future.get();

    // Then
    assertThat(headers).hasSize(5);
    assertThat(headers.get(0).getNumber()).isEqualTo(startBlock);
    assertThat(headers.get(4).getNumber()).isEqualTo(endBlock - 1);
  }

  @Test
  void shouldThrowExceptionWhenHeadersAreMissing() {
    // Given
    final long startBlock = 100L;
    final long endBlock = 105L;
    final SyncTargetNumberRange range = new SyncTargetNumberRange(startBlock, endBlock);

    // Only provide some headers, not all
    when(blockchain.getBlockHeader(100L))
        .thenReturn(Optional.of(new BlockHeaderTestFixture().number(100L).buildHeader()));
    when(blockchain.getBlockHeader(101L))
        .thenReturn(Optional.of(new BlockHeaderTestFixture().number(101L).buildHeader()));
    when(blockchain.getBlockHeader(102L)).thenReturn(Optional.empty());
    when(blockchain.getBlockHeader(103L)).thenReturn(Optional.empty());
    when(blockchain.getBlockHeader(104L)).thenReturn(Optional.empty());

    // When/Then
    CompletableFuture<List<BlockHeader>> future = loadHeadersStep.apply(range);
    assertThatThrownBy(future::get)
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Headers not available for range 100 to 105")
        .hasMessageContaining("Expected 5 headers but found 2");
  }

  @Test
  void shouldReturnEmptyListForEmptyRange() throws ExecutionException, InterruptedException {
    // Given
    final long startBlock = 100L;
    final long endBlock = 100L; // Same as start, so range is empty
    final SyncTargetNumberRange range = new SyncTargetNumberRange(startBlock, endBlock);

    // When
    CompletableFuture<List<BlockHeader>> future = loadHeadersStep.apply(range);
    List<BlockHeader> headers = future.get();

    // Then
    assertThat(headers).isEmpty();
  }
}