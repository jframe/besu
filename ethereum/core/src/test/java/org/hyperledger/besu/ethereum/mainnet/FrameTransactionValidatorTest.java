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
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.INVALID_TRANSACTION_FORMAT;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.WRONG_CHAIN_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.Frame;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FrameTransactionValidatorTest {

  private static final BigInteger CHAIN_ID = BigInteger.valueOf(8141);

  @Mock private org.hyperledger.besu.evm.gascalculator.GasCalculator gasCalculator;

  private MainnetTransactionValidator validator;

  @BeforeEach
  void setUp() {
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(21_000L);
    when(gasCalculator.transactionFloorCost(any(), anyLong())).thenReturn(0L);

    validator =
        new MainnetTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(CHAIN_ID),
            Set.of(TransactionType.FRAME),
            Set.of(BlobType.KZG_PROOF),
            Integer.MAX_VALUE);
  }

  @Test
  void validFrameTransactionPassesValidation() {
    final Transaction tx = frameTransaction(CHAIN_ID, List.of(defaultFrame(100_000L)));

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(tx, Optional.empty(), Optional.empty(), processingParams());

    assertThat(result.isValid()).isTrue();
  }

  @Test
  void emptyFrameListIsRejected() {
    final Transaction tx = frameTransaction(CHAIN_ID, List.of());

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(tx, Optional.empty(), Optional.empty(), processingParams());

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason()).isEqualTo(INVALID_TRANSACTION_FORMAT);
    assertThat(result.getErrorMessage()).contains("at least 1 frame");
  }

  @Test
  void tooManyFramesAreRejected() {
    final List<Frame> frames =
        IntStream.range(0, 1001)
            .mapToObj(i -> defaultFrame(21_000L))
            .collect(Collectors.toList());
    final Transaction tx = frameTransaction(CHAIN_ID, frames);

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(tx, Optional.empty(), Optional.empty(), processingParams());

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason()).isEqualTo(INVALID_TRANSACTION_FORMAT);
    assertThat(result.getErrorMessage()).contains("maximum of 1000 frames");
  }

  @Test
  void frameWithZeroGasLimitIsRejected() {
    final Frame zeroGasFrame = new Frame(Frame.MODE_DEFAULT, Optional.empty(), 0L, Bytes.EMPTY);
    final Transaction tx = frameTransaction(CHAIN_ID, List.of(zeroGasFrame));

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(tx, Optional.empty(), Optional.empty(), processingParams());

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason()).isEqualTo(INVALID_TRANSACTION_FORMAT);
    assertThat(result.getErrorMessage()).contains("gas_limit must be > 0");
  }

  @Test
  void wrongChainIdIsRejected() {
    final Transaction tx =
        frameTransaction(BigInteger.valueOf(9999), List.of(defaultFrame(100_000L)));

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(tx, Optional.empty(), Optional.empty(), processingParams());

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason()).isEqualTo(WRONG_CHAIN_ID);
  }

  @Test
  void intrinsicGasExceedingTotalFrameGasIsRejected() {
    // Intrinsic cost 21_000 > total frame gas (sum of frame gas limits = 10_000)
    final Frame smallFrame = new Frame(Frame.MODE_DEFAULT, Optional.empty(), 10_000L, Bytes.EMPTY);
    final Transaction tx = frameTransaction(CHAIN_ID, List.of(smallFrame));

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(tx, Optional.empty(), Optional.empty(), processingParams());

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT);
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }

  // --- helpers ---

  private Transaction frameTransaction(final BigInteger chainId, final List<Frame> frames) {
    final Transaction tx = mock(Transaction.class);
    when(tx.getType()).thenReturn(TransactionType.FRAME);
    when(tx.getChainId()).thenReturn(Optional.of(chainId));
    when(tx.getFrames()).thenReturn(Optional.of(frames));
    when(tx.getNonce()).thenReturn(1L);
    when(tx.getGasLimit()).thenReturn(0L); // FRAME does not use gas_limit field
    when(tx.getAccessList()).thenReturn(Optional.empty());
    when(tx.codeDelegationListSize()).thenReturn(0);
    when(tx.isContractCreation()).thenReturn(false);
    when(tx.getPayload()).thenReturn(Bytes.EMPTY);
    when(tx.getPayloadZeroBytes()).thenReturn(0L);
    when(tx.getMaxFeePerGas()).thenReturn(Optional.of(Wei.of(2_000_000_000L)));
    when(tx.getMaxPriorityFeePerGas()).thenReturn(Optional.of(Wei.of(1_000_000_000L)));
    return tx;
  }

  private static Frame defaultFrame(final long gasLimit) {
    return new Frame(Frame.MODE_DEFAULT, Optional.empty(), gasLimit, Bytes.EMPTY);
  }

  private static TransactionValidationParams processingParams() {
    return TransactionValidationParams.processingBlockParams;
  }
}
