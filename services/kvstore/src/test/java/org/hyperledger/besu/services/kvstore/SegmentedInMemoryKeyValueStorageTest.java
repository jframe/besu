package org.hyperledger.besu.services.kvstore;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class SegmentedInMemoryKeyValueStorageTest {

  private enum TestSegment implements SegmentIdentifier {
    FOO(new byte[] {1});

    private final byte[] id;

    TestSegment(final byte[] id) {
      this.id = id;
    }

    @Override
    public String getName() {
      return name();
    }

    @Override
    public byte[] getId() {
      return id;
    }

    @Override
    public boolean containsStaticData() {
      return false;
    }

    @Override
    public boolean isEligibleToHighSpecFlag() {
      return false;
    }
  }

  @Test
  void mergeConcatenatesOntoEmptyKey() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final SegmentedKeyValueStorageTransaction tx = kv.startTransaction();
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {1, 2, 3});
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {4, 5, 6});
    tx.commit();

    assertThat(kv.get(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8)))
        .contains(new byte[] {1, 2, 3, 4, 5, 6});
  }

  @Test
  void mergeConcatenatesOntoExistingPutValue() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final SegmentedKeyValueStorageTransaction tx1 = kv.startTransaction();
    tx1.put(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {(byte) 0xAA});
    tx1.commit();

    final SegmentedKeyValueStorageTransaction tx2 = kv.startTransaction();
    tx2.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {1, 2, 3});
    tx2.commit();

    assertThat(kv.get(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8)))
        .contains(new byte[] {(byte) 0xAA, 1, 2, 3});
  }

  @Test
  void mergeWithinSameUncommittedTransactionAccumulates() {
    final SegmentedInMemoryKeyValueStorage kv = new SegmentedInMemoryKeyValueStorage();
    final SegmentedKeyValueStorageTransaction tx = kv.startTransaction();
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {1});
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {2});
    tx.merge(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8), new byte[] {3});
    tx.commit();

    assertThat(kv.get(TestSegment.FOO, "k".getBytes(StandardCharsets.UTF_8)))
        .contains(new byte[] {1, 2, 3});
  }
}
