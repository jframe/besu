package org.hyperledger.besu.services.kvstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class SegmentedKeyValueStorageTransactionValidatorDecoratorTest {

  /** Records every call made to it; used in place of a mock since this module has no Mockito. */
  private static final class RecordingTransaction implements SegmentedKeyValueStorageTransaction {
    final List<String> calls = new ArrayList<>();

    @Override
    public void put(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      calls.add("put");
    }

    @Override
    public void merge(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      calls.add("merge");
    }

    @Override
    public void remove(final SegmentIdentifier segmentId, final byte[] key) {
      calls.add("remove");
    }

    @Override
    public void commit() throws StorageException {
      calls.add("commit");
    }

    @Override
    public void rollback() {
      calls.add("rollback");
    }

    @Override
    public void close() {
      calls.add("close");
    }
  }

  private static final SegmentIdentifier TEST_SEGMENT =
      new SegmentIdentifier() {
        @Override
        public String getName() {
          return "TEST";
        }

        @Override
        public byte[] getId() {
          return new byte[] {1};
        }

        @Override
        public boolean containsStaticData() {
          return false;
        }

        @Override
        public boolean isEligibleToHighSpecFlag() {
          return false;
        }
      };

  @Test
  void mergeDelegatesWhenActiveAndOpen() {
    final RecordingTransaction delegate = new RecordingTransaction();
    final SegmentedKeyValueStorageTransactionValidatorDecorator decorator =
        new SegmentedKeyValueStorageTransactionValidatorDecorator(delegate, () -> false);

    decorator.merge(TEST_SEGMENT, new byte[] {1}, new byte[] {2});

    assertThat(delegate.calls).containsExactly("merge");
  }

  @Test
  void mergeThrowsAfterCommit() {
    final RecordingTransaction delegate = new RecordingTransaction();
    final SegmentedKeyValueStorageTransactionValidatorDecorator decorator =
        new SegmentedKeyValueStorageTransactionValidatorDecorator(delegate, () -> false);

    decorator.commit();

    assertThatThrownBy(() -> decorator.merge(TEST_SEGMENT, new byte[] {1}, new byte[] {2}))
        .isInstanceOf(IllegalStateException.class);
  }
}
