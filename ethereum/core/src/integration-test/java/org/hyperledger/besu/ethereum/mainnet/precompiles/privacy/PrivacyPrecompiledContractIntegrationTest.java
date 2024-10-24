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
package org.hyperledger.besu.ethereum.mainnet.precompiles.privacy;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.enclave.Enclave;
import org.hyperledger.besu.enclave.types.SendRequest;
import org.hyperledger.besu.enclave.types.SendRequestLegacy;
import org.hyperledger.besu.enclave.types.SendResponse;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.WorldUpdater;
import org.hyperledger.besu.ethereum.mainnet.SpuriousDragonGasCalculator;
import org.hyperledger.besu.ethereum.privacy.PrivateTransaction;
import org.hyperledger.besu.ethereum.privacy.PrivateTransactionProcessor;
import org.hyperledger.besu.ethereum.privacy.storage.PrivateStateStorage;
import org.hyperledger.besu.ethereum.vm.BlockHashLookup;
import org.hyperledger.besu.ethereum.vm.MessageFrame;
import org.hyperledger.besu.ethereum.vm.OperationTracer;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.util.bytes.Bytes32;
import org.hyperledger.besu.util.bytes.BytesValue;
import org.hyperledger.besu.util.bytes.BytesValues;
import org.hyperledger.orion.testutil.OrionKeyConfiguration;
import org.hyperledger.orion.testutil.OrionTestHarness;
import org.hyperledger.orion.testutil.OrionTestHarnessFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PrivacyPrecompiledContractIntegrationTest {

  @ClassRule public static final TemporaryFolder folder = new TemporaryFolder();

  private static final byte[] VALID_PRIVATE_TRANSACTION_RLP_BASE64 =
      Base64.getEncoder()
          .encode(
              BytesValue.fromHexString(
                      "0xf90113800182520894095e7baea6a6c7c4c2dfeb977efac326af552d87"
                          + "a0ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                          + "ffff801ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d"
                          + "495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab94"
                          + "9f53faa07bd2c804ac41316156744d784c4355486d425648586f5a7a7a4267"
                          + "5062572f776a3561784470573958386c393153476f3df85aac41316156744d"
                          + "784c4355486d425648586f5a7a7a42675062572f776a356178447057395838"
                          + "6c393153476f3dac4b6f32625671442b6e4e6c4e594c35454537793349644f"
                          + "6e766966746a69697a706a52742b4854754642733d8a726573747269637465"
                          + "64")
                  .extractArray());
  private static final String DEFAULT_OUTPUT = "0x01";

  private static Enclave enclave;
  private static MessageFrame messageFrame;

  private static OrionTestHarness testHarness;
  private static WorldStateArchive worldStateArchive;
  private static PrivateStateStorage privateStateStorage;
  private static PrivateStateStorage.Updater storageUpdater;

  private PrivateTransactionProcessor mockPrivateTxProcessor() {
    final PrivateTransactionProcessor mockPrivateTransactionProcessor =
        mock(PrivateTransactionProcessor.class);
    final PrivateTransactionProcessor.Result result =
        PrivateTransactionProcessor.Result.successful(
            null, 0, BytesValue.fromHexString(DEFAULT_OUTPUT), null);
    when(mockPrivateTransactionProcessor.processTransaction(
            nullable(Blockchain.class),
            nullable(WorldUpdater.class),
            nullable(WorldUpdater.class),
            nullable(ProcessableBlockHeader.class),
            nullable(PrivateTransaction.class),
            nullable(Address.class),
            nullable(OperationTracer.class),
            nullable(BlockHashLookup.class),
            nullable(BytesValue.class)))
        .thenReturn(result);

    return mockPrivateTransactionProcessor;
  }

  @BeforeClass
  public static void setUpOnce() throws Exception {
    folder.create();

    testHarness =
        OrionTestHarnessFactory.create(
            folder.newFolder().toPath(),
            new OrionKeyConfiguration("orion_key_0.pub", "orion_key_1.key"));

    testHarness.start();

    enclave = new Enclave(testHarness.clientUrl());
    messageFrame = mock(MessageFrame.class);

    worldStateArchive = mock(WorldStateArchive.class);
    final MutableWorldState mutableWorldState = mock(MutableWorldState.class);
    when(mutableWorldState.updater()).thenReturn(mock(WorldUpdater.class));
    when(worldStateArchive.getMutable()).thenReturn(mutableWorldState);
    when(worldStateArchive.getMutable(any())).thenReturn(Optional.of(mutableWorldState));

    privateStateStorage = mock(PrivateStateStorage.class);
    storageUpdater = mock(PrivateStateStorage.Updater.class);
    when(storageUpdater.putLatestStateRoot(nullable(Bytes32.class), any()))
        .thenReturn(storageUpdater);
    when(storageUpdater.putTransactionLogs(nullable(Bytes32.class), any()))
        .thenReturn(storageUpdater);
    when(storageUpdater.putTransactionResult(nullable(Bytes32.class), any()))
        .thenReturn(storageUpdater);
    when(privateStateStorage.updater()).thenReturn(storageUpdater);
  }

  @AfterClass
  public static void tearDownOnce() {
    testHarness.getOrion().stop();
  }

  @Test
  public void testUpCheck() throws IOException {
    assertThat(enclave.upCheck()).isTrue();
  }

  @Test
  public void testSendAndReceive() {
    final List<String> publicKeys = testHarness.getPublicKeys();

    final String s = new String(VALID_PRIVATE_TRANSACTION_RLP_BASE64, UTF_8);
    final SendRequest sc =
        new SendRequestLegacy(s, publicKeys.get(0), Lists.newArrayList(publicKeys.get(0)));
    final SendResponse sr = enclave.send(sc);

    final PrivacyPrecompiledContract privacyPrecompiledContract =
        new PrivacyPrecompiledContract(
            new SpuriousDragonGasCalculator(),
            publicKeys.get(0),
            enclave,
            worldStateArchive,
            privateStateStorage);

    privacyPrecompiledContract.setPrivateTransactionProcessor(mockPrivateTxProcessor());

    final BytesValue actual =
        privacyPrecompiledContract.compute(BytesValues.fromBase64(sr.getKey()), messageFrame);

    assertThat(actual).isEqualTo(BytesValue.fromHexString(DEFAULT_OUTPUT));
  }

  @Test
  public void testNoPrivateKeyError() throws RuntimeException {
    final List<String> publicKeys = testHarness.getPublicKeys();
    publicKeys.add("noPrivateKey");

    final String s = new String(VALID_PRIVATE_TRANSACTION_RLP_BASE64, UTF_8);
    final SendRequest sc = new SendRequestLegacy(s, publicKeys.get(0), publicKeys);

    final Throwable thrown = catchThrowable(() -> enclave.send(sc));

    assertThat(thrown).hasMessageContaining("EnclaveDecodePublicKey");
  }

  @Test
  public void testWrongPrivateKeyError() throws RuntimeException {
    final List<String> publicKeys = testHarness.getPublicKeys();
    publicKeys.add("noPrivateKenoPrivateKenoPrivateKenoPrivateK");

    final String s = new String(VALID_PRIVATE_TRANSACTION_RLP_BASE64, UTF_8);
    final SendRequest sc = new SendRequestLegacy(s, publicKeys.get(0), publicKeys);

    final Throwable thrown = catchThrowable(() -> enclave.send(sc));

    assertThat(thrown).hasMessageContaining("NodeMissingPeerUrl");
  }
}
