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
package org.hyperledger.besu.cli.subcommands.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.CalibrationResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.ChangeCountResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.EntrySizeTable;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.RecordingTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieLogChangeCounter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieNodeDiffCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex.TrieShapeModel;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TrieNodeHistoryEstimateSubCommandTest {

  private static final Address TEST_ADDRESS =
      Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");

  @Test
  void countRangeDecodesStoredTrieLogsAndCountsRootWrites() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final MutableBlockchain blockchain = createInMemoryBlockchain(gen.genesisBlock());
    final SegmentedInMemoryKeyValueStorage trieLogStorage = new SegmentedInMemoryKeyValueStorage();
    appendBlocks(blockchain, gen, 2);
    storeAccountCreationTrieLogs(blockchain, trieLogStorage, 1L, 2L);

    final long[] leafCountByRange = {1L};
    final TrieLogChangeCounter counter = new TrieLogChangeCounter(2, 10, new TrieShapeModel(16));

    final ChangeCountResult result =
        TrieNodeHistoryEstimateSubCommand.countRange(
            blockchain, trieLogStorage, 1L, 3L, counter, leafCountByRange, accountHash -> 0L);

    // Each of the two blocks writes the account-trie root (depth 0), deduped per block.
    assertThat(result.mutationsByDepth()[0]).isGreaterThanOrEqualTo(2L);
    // Both blocks create TEST_ADDRESS (prior == null) → +1 leaf delta each, in range 0.
    assertThat(result.accountDeltaByRange()[0]).isEqualTo(2L);
  }

  @Test
  void startBlockDefaultsToOneSoTrieLogFreeGenesisIsNotScanned() {
    assertThat(new TrieNodeHistoryEstimateSubCommand().startBlock).isEqualTo(1L);
  }

  @Test
  void countRangeFailsFastWhenTrieLogMissing() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final MutableBlockchain blockchain = createInMemoryBlockchain(gen.genesisBlock());
    final SegmentedInMemoryKeyValueStorage trieLogStorage = new SegmentedInMemoryKeyValueStorage();
    appendBlocks(blockchain, gen, 1);
    // Deliberately store no trie log for block 1.

    final long[] leafCountByRange = {1L};
    final TrieLogChangeCounter counter = new TrieLogChangeCounter(2, 10, new TrieShapeModel(16));

    assertThatThrownBy(
            () ->
                TrieNodeHistoryEstimateSubCommand.countRange(
                    blockchain,
                    trieLogStorage,
                    1L,
                    2L,
                    counter,
                    leafCountByRange,
                    accountHash -> 0L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("1");
  }

  @Test
  void resolveEntrySizeTableUsesHoodiDefaultsWhenNoCalibrationFileGiven() {
    final StringWriter sw = new StringWriter();
    final PrintWriter out = new PrintWriter(sw);

    final EntrySizeTable table = TrieNodeHistoryEstimateSubCommand.resolveEntrySizeTable(out, null);

    assertThat(table.fullBytes(0, 1.0)).isEqualTo(EntrySizeTable.hoodiDefaults().fullBytes(0, 1.0));
    assertThat(sw.toString()).containsIgnoringCase("hoodi");
  }

  @Test
  void resolveEntrySizeTableUsesCalibrationDataWhenFileGiven(@TempDir final Path tempDir) {
    final Bytes node = shortNode(Bytes.of(0x01, 0x02, 0x03));
    final int expectedFullSize = TrieNodeDiffCodec.encodeFull(node).size();
    final CalibrationResult calibration = recordOneShortNodeWriteAtDepth(2, node);
    final Path calibrationFile = tempDir.resolve("calibration.json");
    calibration.writeTo(calibrationFile);

    final StringWriter sw = new StringWriter();
    final PrintWriter out = new PrintWriter(sw);

    final EntrySizeTable table =
        TrieNodeHistoryEstimateSubCommand.resolveEntrySizeTable(out, calibrationFile);

    // A single recorded write means the per-depth mean equals that write's own size exactly, and
    // differs from the hoodi fallback constant at the same depth/shape.
    assertThat(table.fullBytes(2, 0.0)).isEqualTo(expectedFullSize);
    assertThat(table.fullBytes(2, 0.0))
        .isNotEqualTo(EntrySizeTable.hoodiDefaults().fullBytes(2, 0.0));
    assertThat(sw.toString()).contains(calibrationFile.toString());
  }

  @Test
  void resolveEntrySizeTableThrowsClearErrorWhenCalibrationFileMissing(
      @TempDir final Path tempDir) {
    final Path missing = tempDir.resolve("does-not-exist.json");
    final PrintWriter out = new PrintWriter(new StringWriter());

    assertThatThrownBy(() -> TrieNodeHistoryEstimateSubCommand.resolveEntrySizeTable(out, missing))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to read calibration file")
        .hasMessageContaining(missing.toString())
        .hasMessageContaining("x-trie-node-history-calibrate");
  }

  @Test
  void resolveEntrySizeTableThrowsClearErrorWhenCalibrationFileMalformed(
      @TempDir final Path tempDir) throws Exception {
    final Path malformed = tempDir.resolve("malformed.json");
    Files.writeString(malformed, "{ not valid calibration json");
    final PrintWriter out = new PrintWriter(new StringWriter());

    assertThatThrownBy(
            () -> TrieNodeHistoryEstimateSubCommand.resolveEntrySizeTable(out, malformed))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to read calibration file")
        .hasMessageContaining(malformed.toString());
  }

  private static CalibrationResult recordOneShortNodeWriteAtDepth(
      final int depth, final Bytes node) {
    final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    final RecordingTrieNodeStrategy recorder =
        new RecordingTrieNodeStrategy(new NoOpTrieNodeStrategy());
    final Bytes location = Bytes.wrap(new byte[depth]);
    recorder.putFlatAccountTrieNode(
        storage, (SegmentedKeyValueStorageTransaction) null, location, Bytes32.ZERO, node);
    return recorder.result();
  }

  private static Bytes shortNode(final Bytes value) {
    return RLP.encode(
        rlpOut -> {
          rlpOut.startList();
          rlpOut.writeBytes(Bytes.of(0x12));
          rlpOut.writeBytes(value);
          rlpOut.endList();
        });
  }

  private static final class NoOpTrieNodeStrategy implements TrieNodeStrategy {
    @Override
    public Optional<Bytes> getFlatAccountTrieNode(
        final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
      return Optional.empty();
    }

    @Override
    public Optional<Bytes> getFlatStorageTrieNode(
        final Hash accountHash,
        final Bytes location,
        final Bytes32 nodeHash,
        final SegmentedKeyValueStorage storage) {
      return Optional.empty();
    }

    @Override
    public void putFlatAccountTrieNode(
        final SegmentedKeyValueStorage storage,
        final SegmentedKeyValueStorageTransaction transaction,
        final Bytes location,
        final Bytes32 nodeHash,
        final Bytes node) {
      // no-op: only the recording decorator's own bookkeeping is under test
    }

    @Override
    public void putFlatStorageTrieNode(
        final SegmentedKeyValueStorage storage,
        final SegmentedKeyValueStorageTransaction transaction,
        final Hash accountHash,
        final Bytes location,
        final Bytes32 nodeHash,
        final Bytes node) {
      // no-op: only the recording decorator's own bookkeeping is under test
    }

    @Override
    public void removeFlatAccountStateTrieNode(
        final SegmentedKeyValueStorage storage,
        final SegmentedKeyValueStorageTransaction transaction,
        final Bytes location) {
      // no-op: only the recording decorator's own bookkeeping is under test
    }
  }

  private static void appendBlocks(
      final MutableBlockchain blockchain, final BlockDataGenerator gen, final int count) {
    final Block head =
        blockchain.getBlockByNumber(blockchain.getChainHeadBlockNumber()).orElseThrow();
    for (final Block block : gen.blockSequence(head, count)) {
      blockchain.appendBlock(block, gen.receipts(block));
    }
  }

  private static void storeAccountCreationTrieLogs(
      final MutableBlockchain blockchain,
      final SegmentedInMemoryKeyValueStorage trieLogStorage,
      final long... blockNumbers) {
    final TrieLogFactoryImpl factory = new TrieLogFactoryImpl();
    final var tx = trieLogStorage.startTransaction();
    for (final long n : blockNumbers) {
      final Hash blockHash = blockchain.getBlockHeader(n).orElseThrow().getHash();
      final TrieLogLayer layer = new TrieLogLayer();
      layer.setBlockHash(blockHash);
      layer.setBlockNumber(n);
      layer.addAccountChange(
          TEST_ADDRESS, null, new PmtStateTrieAccountValue(1, Wei.of(n), Hash.EMPTY, Hash.EMPTY));
      tx.put(TRIE_LOG_STORAGE, blockHash.getBytes().toArrayUnsafe(), factory.serialize(layer));
    }
    tx.commit();
  }
}
