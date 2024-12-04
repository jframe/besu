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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class SyncBlockBody {

  private static final SyncBlockBody EMPTY = new SyncBlockBody();

  private final Bytes rawBytesOfWrappedRlpInput;
  private final List<Bytes> transactionBytes;
  private final Bytes ommersListBytes;
  private final Optional<List<Bytes>> withdrawalBytes;

  public SyncBlockBody(
      final RLPInput wrappedBodyRlpInput,
      final List<Bytes> transactionBytes,
      final Bytes ommersListBytes,
      final List<Bytes> withdrawalBytes) {
    this.rawBytesOfWrappedRlpInput = wrappedBodyRlpInput.raw();
    this.transactionBytes = transactionBytes;
    this.ommersListBytes = ommersListBytes;
    this.withdrawalBytes = Optional.ofNullable(withdrawalBytes);
  }

  private SyncBlockBody() {
    this.rawBytesOfWrappedRlpInput = null;
    this.transactionBytes = null;
    this.ommersListBytes = null;
    this.withdrawalBytes = null;
  }

  public static SyncBlockBody empty() {
    return SyncBlockBody.EMPTY;
  }

  public static SyncBlockBody readWrappedBodyFrom(
      final RLPInput input, final BlockHeaderFunctions blockHeaderFunctions) {
    return readWrappedBodyFrom(input, blockHeaderFunctions, false);
  }

  /**
   * Read all fields from the block body expecting a list wrapping them An example of valid body
   * structure that this method would be able to read is: [[txs],[ommers],[withdrawals]] This is
   * used for decoding list of bodies
   *
   * @param input The RLP-encoded input
   * @param blockHeaderFunctions The block header functions used for parsing block headers
   * @param allowEmptyBody A flag indicating whether an empty body is allowed
   * @return the decoded BlockBody from the RLP
   */
  public static SyncBlockBody readWrappedBodyFrom(
      final RLPInput input,
      final BlockHeaderFunctions blockHeaderFunctions,
      final boolean allowEmptyBody) {
    input.enterList();
    if (input.isEndOfCurrentList() && allowEmptyBody) {
      // empty block [] -> Return empty body.
      input.leaveList();
      return empty();
    }
    final SyncBlockBody body = readFrom(input, blockHeaderFunctions);
    input.leaveList();
    return body;
  }

  /**
   * Read all fields from the block body expecting no list wrapping them. An example of a valid body
   * would be: [txs],[ommers],[withdrawals],[requests] this method is called directly when importing
   * a single block
   *
   * @param input The RLP-encoded input
   * @param blockHeaderFunctions The block header functions used for parsing block headers
   * @return the BlockBody decoded from the RLP
   */
  public static SyncBlockBody readFrom(
      final RLPInput input, final BlockHeaderFunctions blockHeaderFunctions) {
    // get a list of Bytes for the transactions
    final ArrayList<Bytes> transactionBytes = new ArrayList<>();
    input.enterList();
    while (!input.isEndOfCurrentList()) {
      transactionBytes.add(input.currentListAsBytesNoCopy());
    }
    input.leaveList();
    // get the Bytes for the ommers
    Bytes ommersListBytes = input.currentListAsBytesNoCopy();
    // get a list of Bytes for the withdrawals
    ArrayList<Bytes> withdrawalBytes = null;
    if (!input.isEndOfCurrentList()) {
      withdrawalBytes = new ArrayList<>();
      input.enterList();
      while (!input.isEndOfCurrentList()) {
        withdrawalBytes.add(input.currentListAsBytesNoCopy());
      }
      input.leaveList();
    }
    return new SyncBlockBody(input, transactionBytes, ommersListBytes, withdrawalBytes);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SyncBlockBody blockBody = (SyncBlockBody) o;
    return Objects.equals(rawBytesOfWrappedRlpInput, blockBody.rawBytesOfWrappedRlpInput);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rawBytesOfWrappedRlpInput);
  }

  public Hash getTransactionsRoot() {
    return getRootFromListOfBytes(transactionBytes);
  }

  public Hash getOmmersHash() {
    return Hash.wrap(org.hyperledger.besu.crypto.Hash.keccak256(ommersListBytes));
  }

  public Hash getWithdrawalsRoot() {
    if (withdrawalBytes.isEmpty()) {
      return null;
    }
    final List<Bytes> bytes = withdrawalBytes.get();
    return getRootFromListOfBytes(bytes);
  }

  private Hash getRootFromListOfBytes(final List<Bytes> bytes) {
    final MerkleTrie<Bytes, Bytes> trie = new SimpleMerklePatriciaTrie<>(b -> b);
    IntStream.range(0, bytes.size())
        .forEach(
            i -> {
              trie.put(indexKey(i), bytes.get(i));
              System.out.println("index: " + indexKey(i) + "  txBytes: " + bytes.get(i));
            });
    return Hash.wrap(trie.getRootHash());
  }

  private static Bytes indexKey(final int i) {
    return RLP.encodeOne(UInt256.valueOf(i).trimLeadingZeros());
  }
}
