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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Frame;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link FrameTransactionProcessor}. Exercises real EVM execution with the
 * EIP-8141 opcode set (APPROVE, TXPARAMLOAD, TXPARAMSIZE, TXPARAMCOPY) against an in-memory world
 * state.
 */
class FrameTransactionProcessorTest {

  private static final BigInteger CHAIN_ID = BigInteger.valueOf(8141);
  private static final Address SENDER =
      Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  private static final Address COINBASE =
      Address.fromHexString("0xcafecafecafecafecafecafecafecafecafecafe");
  private static final Address ENTRY_POINT = FrameTransactionProcessor.ENTRY_POINT;

  // A secondary address whose code just STOPs — no APPROVE call
  private static final Address STOP_ONLY =
      Address.fromHexString("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

  // An address acting as a paymaster; its APPROVE bytecode uses scope 1
  private static final Address PAYMASTER =
      Address.fromHexString("0xcccccccccccccccccccccccccccccccccccccccc");

  // PUSH1 0, APPROVE (0xaa), STOP — approves with scope 0 (sender remains fee payer)
  private static final Bytes APPROVE_SCOPE0 = Bytes.fromHexString("0x6000aa00");

  // PUSH1 1, APPROVE (0xaa), STOP — approves with scope 1 (designates target as fee payer)
  private static final Bytes APPROVE_SCOPE1 = Bytes.fromHexString("0x6001aa00");

  private MainnetTransactionProcessor processor;
  private MutableWorldState worldState;
  private ProcessableBlockHeader blockHeader;

  @BeforeEach
  void setUp() {
    final PragueGasCalculator gasCalculator = new PragueGasCalculator();
    final EVM evm = MainnetEVMs.frameTx(gasCalculator, CHAIN_ID, EvmConfiguration.DEFAULT);
    final MessageCallProcessor mcp = new MessageCallProcessor(evm, new PrecompileContractRegistry());
    final ContractCreationProcessor ccp =
        new ContractCreationProcessor(evm, false, List.of(), 0);

    processor =
        MainnetTransactionProcessor.builder()
            .gasCalculator(gasCalculator)
            .transactionValidatorFactory(mock(TransactionValidatorFactory.class))
            .contractCreationProcessor(ccp)
            .messageCallProcessor(mcp)
            .clearEmptyAccounts(false)
            .warmCoinbase(false)
            .maxStackSize(1024)
            .feeMarket(FeeMarket.london(0L))
            .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.eip1559())
            .build();

    worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final WorldUpdater setup = worldState.updater();
    setup.getOrCreate(ENTRY_POINT).setCode(APPROVE_SCOPE0);
    setup.getOrCreate(STOP_ONLY).setCode(Bytes.of((byte) 0x00)); // STOP
    setup.getOrCreate(PAYMASTER).setCode(APPROVE_SCOPE1);
    setup.getOrCreate(SENDER).setBalance(Wei.fromEth(1));
    setup.getOrCreate(PAYMASTER).setBalance(Wei.fromEth(1));
    setup.commit();

    blockHeader =
        new BlockHeaderTestFixture()
            .baseFeePerGas(Wei.of(1_000_000_000L))
            .gasLimit(30_000_000L)
            .buildHeader();
  }

  @Test
  void singleVerifyFrameWithApproveSucceeds() {
    final Frame verifyFrame =
        new Frame(Frame.MODE_VERIFY, Optional.of(ENTRY_POINT), 100_000L, Bytes.EMPTY);

    final TransactionProcessingResult result = process(frameTx(List.of(verifyFrame)));

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getFrameReceipts()).isPresent();
    assertThat(result.getFrameReceipts().get()).hasSize(1);
    // Scope 0: fee payer remains the sender
    assertThat(result.getFeePayerAddress()).contains(SENDER);
  }

  @Test
  void verifyFrameWithoutApproveFailsTransaction() {
    // STOP_ONLY executes STOP without calling APPROVE — the whole tx is rejected
    final Frame verifyFrame =
        new Frame(Frame.MODE_VERIFY, Optional.of(STOP_ONLY), 100_000L, Bytes.EMPTY);

    final TransactionProcessingResult result = process(frameTx(List.of(verifyFrame)));

    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  void verifyThenDefaultFramesBothSucceed() {
    final Frame verifyFrame =
        new Frame(Frame.MODE_VERIFY, Optional.of(ENTRY_POINT), 100_000L, Bytes.EMPTY);
    // DEFAULT frame targets STOP_ONLY — APPROVE inside a non-VERIFY frame would halt, so we avoid
    // it here
    final Frame defaultFrame =
        new Frame(Frame.MODE_DEFAULT, Optional.of(STOP_ONLY), 50_000L, Bytes.EMPTY);

    final TransactionProcessingResult result = process(frameTx(List.of(verifyFrame, defaultFrame)));

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getFrameReceipts()).isPresent();
    assertThat(result.getFrameReceipts().get()).hasSize(2);
    // Scope 0 approval: fee payer is still the sender
    assertThat(result.getFeePayerAddress()).contains(SENDER);
  }

  @Test
  void approveWithScope1DesignatesPaymasterAsFeePayerAddress() {
    // PAYMASTER's bytecode calls APPROVE with scope 1, making itself the fee payer
    final Frame verifyFrame =
        new Frame(Frame.MODE_VERIFY, Optional.of(PAYMASTER), 100_000L, Bytes.EMPTY);

    final TransactionProcessingResult result = process(frameTx(List.of(verifyFrame)));

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getFeePayerAddress()).contains(PAYMASTER);
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }

  // --- helpers ---

  private Transaction frameTx(final List<Frame> frames) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(CHAIN_ID)
        .nonce(0)
        .frameSender(SENDER)
        .frames(frames)
        .maxPriorityFeePerGas(Wei.of(500_000_000L))
        .maxFeePerGas(Wei.of(2_000_000_000L))
        .build();
  }

  private TransactionProcessingResult process(final Transaction tx) {
    return processor.processTransaction(
        worldState.updater(),
        blockHeader,
        tx,
        COINBASE,
        (mf, n) -> Hash.ZERO,
        TransactionValidationParams.processingBlockParams,
        Wei.ZERO);
  }
}
