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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.archiveindex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.within;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.TrieNodePathEnumerator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class TrieLogChangeCounterTest {

  private final TrieLogChangeCounter counter =
      new TrieLogChangeCounter(2, 0 /* sample everything */, new TrieShapeModel(16));

  private PmtStateTrieAccountValue acct(final long balance) {
    return new PmtStateTrieAccountValue(0L, Wei.of(balance), Hash.EMPTY, Hash.EMPTY);
  }

  @Test
  void singleAccountChangeRecordsRootToLeafPath() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final TrieLogLayer log = new TrieLogLayer();
    log.addAccountChange(addr, null, acct(1)); // creation

    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    // Small era leaf count → shallow termination cap, so the path is short and assertions are
    // stable.
    counter.countBlock(log, 100L, 4L, out);

    // depth 0 (root) is always present; there should be exactly one root-level mutation.
    assertThat(out.mutationsByDepth()[0]).isEqualTo(1L);
    // creation bumps the account delta for range 0.
    assertThat(out.accountDeltaByRange()[0]).isEqualTo(1L);
    // depth 0 is upper-FULL.
    assertThat(out.upperFullByDepth()[0]).isEqualTo(1L);
  }

  @Test
  void twoAccountsSharingNoPrefixDoubleTheRootMutation() {
    final Address a = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final Address b = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final TrieLogLayer log = new TrieLogLayer();
    log.addAccountChange(a, acct(1), acct(2));
    log.addAccountChange(b, acct(1), acct(2));

    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    counter.countBlock(log, 100L, 4L, out);

    // Both leaves pass through the root, deduped to ONE root node write.
    assertThat(out.mutationsByDepth()[0]).isEqualTo(1L);
  }

  @Test
  void storageChangeUsesAccountHashPrefixedDepth() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(1)); // hashed internally
    final TrieLogLayer log = new TrieLogLayer();
    log.addStorageChange(addr, slot, UInt256.ZERO, UInt256.valueOf(5));

    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    counter.countBlock(log, 100L, 4L, out);

    // Storage-trie root for this account is at depth 0 (after stripping the 32-byte account hash).
    assertThat(out.mutationsByDepth()[0]).isGreaterThanOrEqualTo(1L);
  }

  @Test
  void storageSlotDepthUsesPerContractLeafCountNotGlobalAccountCount() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(7));
    final TrieLogLayer log = new TrieLogLayer();
    log.addStorageChange(addr, slot, UInt256.ZERO, UInt256.valueOf(5));

    // A mainnet-scale account trie, but this contract's storage trie holds a single slot.
    final long largeAccountLeafCount = 50_000_000L;

    // With a per-contract provider reporting a one-slot storage trie, the slot is a lone leaf and
    // terminates at depth 0 (path compaction): only the storage root node is written.
    final ChangeCountResult perContract = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    counter.countBlock(log, 100L, largeAccountLeafCount, accountHash -> 1L, perContract);
    assertThat(perContract.mutationsByDepth()[0]).isEqualTo(1L);
    for (int depth = 1; depth < perContract.mutationsByDepth().length; depth++) {
      assertThat(perContract.mutationsByDepth()[depth])
          .as("a one-slot storage trie must not write nodes below depth 0 (depth %d)", depth)
          .isZero();
    }

    // Regression guard: pricing the same slot against the global account leaf count (the pre-fix
    // behaviour, reproduced by the 4-arg overload) drives it many levels deep.
    final ChangeCountResult global = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    counter.countBlock(log, 100L, largeAccountLeafCount, global);
    int deepest = 0;
    for (int depth = 0; depth < global.mutationsByDepth().length; depth++) {
      if (global.mutationsByDepth()[depth] > 0) {
        deepest = depth;
      }
    }
    assertThat(deepest)
        .as("global-account-count pricing over-expands a single storage slot into a deep path")
        .isGreaterThan(3);
  }

  @Test
  void categoryDiagnosticsSplitAccountAndStorageWritesAndRecordAssumedStorageSize() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(7));
    final TrieLogLayer log = new TrieLogLayer();
    log.addAccountChange(addr, acct(1), acct(2));
    log.addStorageChange(addr, slot, UInt256.ZERO, UInt256.valueOf(5));

    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    // Block 150_000 → era 1; storage priced at 500 slots for this contract.
    counter.countBlock(log, 150_000L, 4L, accountHash -> 500L, out);

    // Every mutation is attributed to exactly one trie: the split sums back to the combined total.
    for (int d = 0; d < out.mutationsByDepth().length; d++) {
      assertThat(out.accountMutationsByDepth()[d] + out.storageMutationsByDepth()[d])
          .as("depth %d account+storage split must equal combined total", d)
          .isEqualTo(out.mutationsByDepth()[d]);
    }
    assertThat(out.accountMutationsByDepth()[0]).isEqualTo(1L); // one account root
    assertThat(out.storageMutationsByDepth()[0]).isEqualTo(1L); // one storage root

    // Per-era diagnostics land in era 1 (block 150_000 / 100_000).
    assertThat(out.accountWritesByRange()[1]).isGreaterThanOrEqualTo(1L);
    assertThat(out.storageWritesByRange()[1]).isGreaterThanOrEqualTo(1L);
    // One contract priced this block; the recorded mean assumed storage size is 500.
    assertThat(out.assumedStorageLeafCountSumByRange()[1]).isEqualTo(500L);
    assertThat(out.storageContractGroupsByRange()[1]).isEqualTo(1L);
  }

  @Test
  void samplingWithNonZeroShiftDoesNotThrowAtShallowDepths() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final TrieLogLayer log = new TrieLogLayer();
    log.addAccountChange(addr, null, acct(1)); // creation touches depth 0 and 1 paths

    final TrieLogChangeCounter shiftedCounter =
        new TrieLogChangeCounter(2, 10, new TrieShapeModel(16));
    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);

    assertThatCode(() -> shiftedCounter.countBlock(log, 100L, 4L, out)).doesNotThrowAnyException();
  }

  @Test
  void accountTrieDepthsUpToTwoAreAlwaysTrackedRegardlessOfSampling() {
    final Address a = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final Address b = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final Address c = Address.fromHexString("0x00000000000000000000000000000000000000cc");
    final List<Address> addresses = List.of(a, b, c);

    // Aggressive sampling: isSampled alone would almost never keep a given depth 0/1/2 path.
    final TrieLogChangeCounter shiftedCounter =
        new TrieLogChangeCounter(2, 10, new TrieShapeModel(16));
    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);

    // Mainnet-scale leaf count: depth <= 2 occupancy is ~100% here (occupancy-aware existence
    // gating only guarantees every key reaches depth 2 when the era is this large), which is
    // exactly the scale the exact-tracking guarantee is meant for.
    final long leafCountForEra = 50_000_000L;
    final int blocks = 3;
    for (long block = 100; block < 100 + blocks; block++) {
      final TrieLogLayer log = new TrieLogLayer();
      for (final Address addr : addresses) {
        log.addAccountChange(addr, acct(1), acct(2));
      }
      shiftedCounter.countBlock(log, block, leafCountForEra, out);
    }

    boolean anyPathNotSampledByHashAlone = false;
    for (final Address addr : addresses) {
      final Bytes nibbles = TrieNodePathEnumerator.toNibbles(addr.addressHash().getBytes());
      for (int depth = 0; depth <= 2; depth++) {
        final Bytes path = nibbles.slice(0, depth);
        if (!shiftedCounter.isSampled(path)) {
          anyPathNotSampledByHashAlone = true;
        }
        final int[] lifetime = out.sampledLifetime().get(path);
        assertThat(lifetime)
            .as("depth %d path for account %s must be exactly tracked", depth, addr)
            .isNotNull();
        assertThat(lifetime[1]).isEqualTo(blocks);
      }
    }
    // Sanity check: the fixture actually exercises the force-inclusion guarantee (i.e. hash
    // sampling alone would have dropped at least one of these paths).
    assertThat(anyPathNotSampledByHashAlone).isTrue();
  }

  @Test
  void deeperAccountPathsAndAllStoragePathsStillDependOnHashSampling() {
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(7));
    final TrieLogChangeCounter shiftedCounter =
        new TrieLogChangeCounter(2, 10, new TrieShapeModel(16));
    final TrieLogLayer log = new TrieLogLayer();
    log.addAccountChange(addr, acct(1), acct(2));
    log.addStorageChange(addr, slot, UInt256.ZERO, UInt256.valueOf(5));

    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);
    // Large leaf count → deep termination cap, so depth-3 account paths are expanded.
    shiftedCounter.countBlock(log, 100L, 1_000_000L, out);

    final Bytes accountNibbles = TrieNodePathEnumerator.toNibbles(addr.addressHash().getBytes());
    final Bytes depth3AccountPath = accountNibbles.slice(0, 3);
    assertThat(out.sampledLifetime().containsKey(depth3AccountPath))
        .isEqualTo(shiftedCounter.isSampled(depth3AccountPath));

    final Bytes accountHash = addr.addressHash().getBytes();
    final Bytes storageNibbles = TrieNodePathEnumerator.toNibbles(slot.getSlotHash().getBytes());
    final Bytes storageRootPath = Bytes.concatenate(accountHash, storageNibbles.slice(0, 0));
    assertThat(out.sampledLifetime().containsKey(storageRootPath))
        .isEqualTo(shiftedCounter.isSampled(storageRootPath));
  }

  @Test
  void toyScaleSingleAccountOnlyTouchesRootAcrossBlocks() {
    // Real migrator ground truth (Task 8): 1 account, 3 blocks -> exactly 1 entry/block (root
    // only). Besu's real trie uses extension-node compaction, so a node only exists at a key's
    // own termination depth, not at every depth up to a shared cap. leafCountForEra=1 is the
    // shapeModel's own special case (100% mass at depth 0), so every key must terminate at depth
    // 0 here.
    final Address addr = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final ChangeCountResult out = new ChangeCountResult(ChangeCountResult.MAX_DEPTH);

    final int blocks = 3;
    for (long block = 100; block < 100 + blocks; block++) {
      final TrieLogLayer log = new TrieLogLayer();
      log.addAccountChange(addr, acct(1), acct(2));
      counter.countBlock(log, block, 1L, out);
    }

    assertThat(out.mutationsByDepth()[0]).isEqualTo((long) blocks);
    for (int depth = 1; depth < out.mutationsByDepth().length; depth++) {
      assertThat(out.mutationsByDepth()[depth]).as("depth %d must be zero", depth).isEqualTo(0L);
    }
  }

  @Test
  void sampledTerminationDepthIsDeterministic() {
    final Bytes hash =
        Address.fromHexString("0x00000000000000000000000000000000000000aa")
            .addressHash()
            .getBytes();

    final int first = counter.sampledTerminationDepth(hash, 10_000L, 20);
    final int second = counter.sampledTerminationDepth(hash, 10_000L, 20);

    assertThat(second).isEqualTo(first);
  }

  @Test
  void sampledTerminationDepthHistogramTracksModelShape() {
    final long leafCount = 10_000L;
    final int maxDepth = 20;
    final int keyCount = 2_000;
    final long[] histogram = new long[maxDepth + 1];

    for (int i = 0; i < keyCount; i++) {
      final Bytes hash = Hash.hash(UInt256.valueOf(i)).getBytes();
      final int depth = counter.sampledTerminationDepth(hash, leafCount, maxDepth);
      histogram[depth]++;
    }

    final TrieShapeModel model = new TrieShapeModel(16);
    final double expectedPeak = model.expectedLeafDepth(leafCount);

    int peakDepth = 0;
    for (int d = 1; d <= maxDepth; d++) {
      if (histogram[d] > histogram[peakDepth]) {
        peakDepth = d;
      }
    }
    assertThat((double) peakDepth).isCloseTo(expectedPeak, within(2.0));

    long tailBeyondTen = 0;
    for (int d = 11; d <= maxDepth; d++) {
      tailBeyondTen += histogram[d];
    }
    assertThat((double) tailBeyondTen / keyCount).isLessThan(0.01);
  }
}
