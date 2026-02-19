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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiArchiver;

import java.util.Optional;

import org.junit.jupiter.api.Test;

public class DebugTriggerBonsaiArchiverTest {

  @Test
  public void shouldReturnErrorWhenArchiverNotPresent() {
    final DebugTriggerBonsaiArchiver method = new DebugTriggerBonsaiArchiver(Optional.empty());
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_triggerBonsaiArchiver", new Object[] {}));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType()).isEqualTo(RpcErrorType.METHOD_NOT_ENABLED);
  }

  @Test
  public void shouldReturnTrueWhenArchiverPresent() {
    final BonsaiArchiver mockArchiver = mock(BonsaiArchiver.class);
    final DebugTriggerBonsaiArchiver method = new DebugTriggerBonsaiArchiver(Optional.of(mockArchiver));
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_triggerBonsaiArchiver", new Object[] {}));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final JsonRpcSuccessResponse successResponse = (JsonRpcSuccessResponse) response;
    assertThat(successResponse.getResult()).isEqualTo(true);
    verify(mockArchiver).triggerArchiving();
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    final DebugTriggerBonsaiArchiver method = new DebugTriggerBonsaiArchiver(Optional.empty());

    assertThat(method.getName()).isEqualTo("debug_triggerBonsaiArchiver");
  }
}
