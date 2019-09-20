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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.api.jsonrpc.BlockchainImporter;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseUtils;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcTestMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionResult;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.testutil.BlockTestUtil;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class EthGetBlockByHashIntegrationTest {

  private Map<String, JsonRpcMethod> methods;
  private static JsonRpcTestMethodsFactory BLOCKCHAIN;
  private final JsonRpcResponseUtils responseUtils = new JsonRpcResponseUtils();
  private final String ETH_METHOD = "eth_getBlockByHash";
  private final String JSON_RPC_VERSION = "2.0";
  private final String ZERO_HASH = String.valueOf(Hash.ZERO);

  @BeforeClass
  public static void setUpOnce() throws Exception {
    final String genesisJson =
        Resources.toString(BlockTestUtil.getTestGenesisUrl(), Charsets.UTF_8);

    BLOCKCHAIN =
        new JsonRpcTestMethodsFactory(
            new BlockchainImporter(BlockTestUtil.getTestBlockchainUrl(), genesisJson));
  }

  @Before
  public void setUp() {
    methods = BLOCKCHAIN.methods();
  }

  @Test
  public void returnCorrectEthMethodName() {
    assertThat(ethGetBlockByHash().getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  public void returnEmptyResponseIfBlockNotFound() {
    final JsonRpcResponse expected = new JsonRpcSuccessResponse(null, null);

    final JsonRpcMethod method = ethGetBlockByHash();
    final JsonRpcResponse actual = method.response(requestWithParams(ZERO_HASH, true));

    assertThat(actual).isEqualToComparingFieldByField(expected);
  }

  @Test
  public void returnFullTransactionIfBlockFound() {
    final Map<JsonRpcResponseKey, String> expectedResult = new EnumMap<>(JsonRpcResponseKey.class);
    expectedResult.put(JsonRpcResponseKey.NUMBER, "0x1");
    expectedResult.put(
        JsonRpcResponseKey.MIX_HASH,
        "0x552dd5ae3ccb7a278c6cedda27e48963101be56526a8ee8d70e8227f2c08edf0");
    expectedResult.put(
        JsonRpcResponseKey.PARENT_HASH,
        "0xf8f01382f5636d02edac7fff679a6feb7a572d37a395daaab77938feb6fe217f");
    expectedResult.put(JsonRpcResponseKey.NONCE, "0xbd2b1f0ebba7b989");
    expectedResult.put(
        JsonRpcResponseKey.OMMERS_HASH,
        "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347");
    expectedResult.put(
        JsonRpcResponseKey.LOGS_BLOOM,
        "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
    expectedResult.put(
        JsonRpcResponseKey.TRANSACTION_ROOT,
        "0x3ccbb984a0a736604acae327d9b643f8e75c7931cb2c6ac10dab4226e2e4c5a3");
    expectedResult.put(
        JsonRpcResponseKey.STATE_ROOT,
        "0xee57559895449b8dbd0a096b2999cf97b517b645ec8db33c7f5934778672263e");
    expectedResult.put(
        JsonRpcResponseKey.RECEIPTS_ROOT,
        "0xa2bd925fcbb8b1ec39612553b17c9265ab198f5af25cc564655114bf5a28c75d");
    expectedResult.put(JsonRpcResponseKey.COINBASE, "0x8888f1f195afa192cfee860698584c030f4c9db1");
    expectedResult.put(JsonRpcResponseKey.DIFFICULTY, "0x20000");
    expectedResult.put(JsonRpcResponseKey.TOTAL_DIFFICULTY, "0x40000");
    expectedResult.put(JsonRpcResponseKey.EXTRA_DATA, "0x");
    expectedResult.put(JsonRpcResponseKey.SIZE, "0x96a");
    expectedResult.put(JsonRpcResponseKey.GAS_LIMIT, "0x2fefd8");
    expectedResult.put(JsonRpcResponseKey.GAS_USED, "0x78674");
    expectedResult.put(JsonRpcResponseKey.TIMESTAMP, "0x561bc2e0");
    final List<TransactionResult> transactions =
        responseUtils.transactions(
            responseUtils.transaction(
                "0x10aaf14a53caf27552325374429d3558398a36d3682ede6603c2c6511896e9f9",
                "0x1",
                "0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b",
                "0x2fefd8",
                "0x1",
                "0x812742182a79a8e67733edc58cfa3767aa2d7ad06439d156ddbbb33e3403b4ed",
                "0x5b5b610705806100106000396000f3006000357c010000000000000000000000000000000000000000000000000000000090048063102accc11461012c57806312a7b9141461013a5780631774e6461461014c5780631e26fd331461015d5780631f9030371461016e578063343a875d1461018057806338cc4831146101955780634e7ad367146101bd57806357cb2fc4146101cb57806365538c73146101e057806368895979146101ee57806376bc21d9146102005780639a19a9531461020e5780639dc2c8f51461021f578063a53b1c1e1461022d578063a67808571461023e578063b61c05031461024c578063c2b12a731461025a578063d2282dc51461026b578063e30081a01461027c578063e8beef5b1461028d578063f38b06001461029b578063f5b53e17146102a9578063fd408767146102bb57005b6101346104d6565b60006000f35b61014261039b565b8060005260206000f35b610157600435610326565b60006000f35b6101686004356102c9565b60006000f35b610176610442565b8060005260206000f35b6101886103d3565b8060ff1660005260206000f35b61019d610413565b8073ffffffffffffffffffffffffffffffffffffffff1660005260206000f35b6101c56104c5565b60006000f35b6101d36103b7565b8060000b60005260206000f35b6101e8610454565b60006000f35b6101f6610401565b8060005260206000f35b61020861051f565b60006000f35b6102196004356102e5565b60006000f35b610227610693565b60006000f35b610238600435610342565b60006000f35b610246610484565b60006000f35b610254610493565b60006000f35b61026560043561038d565b60006000f35b610276600435610350565b60006000f35b61028760043561035e565b60006000f35b6102956105b4565b60006000f35b6102a3610547565b60006000f35b6102b16103ef565b8060005260206000f35b6102c3610600565b60006000f35b80600060006101000a81548160ff021916908302179055505b50565b80600060016101000a81548160ff02191690837f01000000000000000000000000000000000000000000000000000000000000009081020402179055505b50565b80600060026101000a81548160ff021916908302179055505b50565b806001600050819055505b50565b806002600050819055505b50565b80600360006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908302179055505b50565b806004600050819055505b50565b6000600060009054906101000a900460ff1690506103b4565b90565b6000600060019054906101000a900460000b90506103d0565b90565b6000600060029054906101000a900460ff1690506103ec565b90565b600060016000505490506103fe565b90565b60006002600050549050610410565b90565b6000600360009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16905061043f565b90565b60006004600050549050610451565b90565b7f65c9ac8011e286e89d02a269890f41d67ca2cc597b2c76c7c69321ff492be5806000602a81526020016000a15b565b6000602a81526020016000a05b565b60017f81933b308056e7e85668661dcd102b1f22795b4431f9cf4625794f381c271c6b6000602a81526020016000a25b565b60016000602a81526020016000a15b565b3373ffffffffffffffffffffffffffffffffffffffff1660017f0e216b62efbb97e751a2ce09f607048751720397ecfb9eef1e48a6644948985b6000602a81526020016000a35b565b3373ffffffffffffffffffffffffffffffffffffffff1660016000602a81526020016000a25b565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6001023373ffffffffffffffffffffffffffffffffffffffff1660017f317b31292193c2a4f561cc40a95ea0d97a2733f14af6d6d59522473e1f3ae65f6000602a81526020016000a45b565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6001023373ffffffffffffffffffffffffffffffffffffffff1660016000602a81526020016000a35b565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6001023373ffffffffffffffffffffffffffffffffffffffff1660017fd5f0a30e4be0c6be577a71eceb7464245a796a7e6a55c0d971837b250de05f4e60007fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe98152602001602a81526020016000a45b565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6001023373ffffffffffffffffffffffffffffffffffffffff16600160007fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe98152602001602a81526020016000a35b56",
                "0x0",
                null,
                "0x0",
                "0xa",
                "0x1c",
                "0xe439aa8812c1c0a751b0931ea20c5a30cd54fe15cae883c59fd8107e04557679",
                "0x58d025af99b538b778a47da8115c43d5cee564c3cc8d58eb972aaf80ea2c406e"));

    final JsonRpcResponse expected = responseUtils.response(expectedResult, transactions);
    final JsonRpcResponse actual =
        ethGetBlockByHash()
            .response(
                requestWithParams(
                    "0x10aaf14a53caf27552325374429d3558398a36d3682ede6603c2c6511896e9f9", true));

    assertThat(actual).isEqualToComparingFieldByFieldRecursively(expected);
  }

  @Test
  public void returnTransactionHashOnlyIfBlockFound() {
    final Map<JsonRpcResponseKey, String> expectedResult = new EnumMap<>(JsonRpcResponseKey.class);
    expectedResult.put(JsonRpcResponseKey.NUMBER, "0x1");
    expectedResult.put(
        JsonRpcResponseKey.MIX_HASH,
        "0x552dd5ae3ccb7a278c6cedda27e48963101be56526a8ee8d70e8227f2c08edf0");
    expectedResult.put(
        JsonRpcResponseKey.PARENT_HASH,
        "0xf8f01382f5636d02edac7fff679a6feb7a572d37a395daaab77938feb6fe217f");
    expectedResult.put(JsonRpcResponseKey.NONCE, "0xbd2b1f0ebba7b989");
    expectedResult.put(
        JsonRpcResponseKey.OMMERS_HASH,
        "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347");
    expectedResult.put(
        JsonRpcResponseKey.LOGS_BLOOM,
        "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
    expectedResult.put(
        JsonRpcResponseKey.TRANSACTION_ROOT,
        "0x3ccbb984a0a736604acae327d9b643f8e75c7931cb2c6ac10dab4226e2e4c5a3");
    expectedResult.put(
        JsonRpcResponseKey.STATE_ROOT,
        "0xee57559895449b8dbd0a096b2999cf97b517b645ec8db33c7f5934778672263e");
    expectedResult.put(
        JsonRpcResponseKey.RECEIPTS_ROOT,
        "0xa2bd925fcbb8b1ec39612553b17c9265ab198f5af25cc564655114bf5a28c75d");
    expectedResult.put(JsonRpcResponseKey.COINBASE, "0x8888f1f195afa192cfee860698584c030f4c9db1");
    expectedResult.put(JsonRpcResponseKey.DIFFICULTY, "0x20000");
    expectedResult.put(JsonRpcResponseKey.TOTAL_DIFFICULTY, "0x40000");
    expectedResult.put(JsonRpcResponseKey.EXTRA_DATA, "0x");
    expectedResult.put(JsonRpcResponseKey.SIZE, "0x96a");
    expectedResult.put(JsonRpcResponseKey.GAS_LIMIT, "0x2fefd8");
    expectedResult.put(JsonRpcResponseKey.GAS_USED, "0x78674");
    expectedResult.put(JsonRpcResponseKey.TIMESTAMP, "0x561bc2e0");
    final List<TransactionResult> transactions =
        responseUtils.transactions(
            "0x812742182a79a8e67733edc58cfa3767aa2d7ad06439d156ddbbb33e3403b4ed");

    final JsonRpcResponse expected = responseUtils.response(expectedResult, transactions);
    final JsonRpcRequest request =
        requestWithParams(
            "0x10aaf14a53caf27552325374429d3558398a36d3682ede6603c2c6511896e9f9", false);
    final JsonRpcResponse actual = ethGetBlockByHash().response(request);

    assertThat(actual).isEqualToComparingFieldByFieldRecursively(expected);
  }

  private JsonRpcRequest requestWithParams(final Object... params) {
    return new JsonRpcRequest(JSON_RPC_VERSION, ETH_METHOD, params);
  }

  private JsonRpcMethod ethGetBlockByHash() {
    final JsonRpcMethod method = methods.get(ETH_METHOD);
    assertThat(method).isNotNull();
    return method;
  }
}
