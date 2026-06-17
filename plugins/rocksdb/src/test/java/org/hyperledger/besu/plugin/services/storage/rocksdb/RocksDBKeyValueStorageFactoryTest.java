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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.hyperledger.besu.plugin.services.storage.DataStorageFormat.BONSAI;
import static org.hyperledger.besu.plugin.services.storage.DataStorageFormat.FOREST;
import static org.hyperledger.besu.plugin.services.storage.DataStorageFormat.X_BONSAI_ARCHIVE;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.RocksDBColumnarKeyValueStorageTest.TestSegment;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.TransactionDBRocksDBColumnarKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RocksDBKeyValueStorageFactoryTest {

  @Mock private RocksDBFactoryConfiguration rocksDbConfiguration;
  @Mock private BesuConfiguration commonConfiguration;
  @Mock private DataStorageConfiguration dataStorageConfiguration;
  @TempDir public Path temporaryFolder;
  private final ObservableMetricsSystem metricsSystem = new NoOpMetricsSystem();
  private final SegmentIdentifier segment = TestSegment.FOO;
  private final List<SegmentIdentifier> segments = List.of(TestSegment.DEFAULT, segment);

  @ParameterizedTest
  @EnumSource(DataStorageFormat.class)
  public void shouldCreateCorrectMetadataFileForLatestVersionForNewDb(
      final DataStorageFormat dataStorageFormat) throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, dataStorageFormat);

    final RocksDBKeyValueStorageFactory storageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    try (final var storage = storageFactory.create(segment, commonConfiguration, metricsSystem)) {
      // Side effect is creation of the Metadata version file
      final BaseVersionedStorageFormat expectedVersion =
          dataStorageFormat == BONSAI
              ? BaseVersionedStorageFormat.BONSAI_WITH_RECEIPT_COMPACTION
              : (dataStorageFormat == X_BONSAI_ARCHIVE
                  ? BaseVersionedStorageFormat.BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION
                  : BaseVersionedStorageFormat.FOREST_WITH_RECEIPT_COMPACTION);
      assertThat(DatabaseMetadata.lookUpFrom(tempDataDir).getVersionedStorageFormat())
          .isEqualTo(expectedVersion);
    }
  }

  @ParameterizedTest
  @EnumSource(DataStorageFormat.class)
  public void shouldCreateCorrectMetadataFileForLatestVersionForNewDbWithReceiptCompaction(
      final DataStorageFormat dataStorageFormat) throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, dataStorageFormat);

    final RocksDBKeyValueStorageFactory storageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    try (final var storage = storageFactory.create(segment, commonConfiguration, metricsSystem)) {
      // Side effect is creation of the Metadata version file
      final BaseVersionedStorageFormat expectedVersion =
          dataStorageFormat == BONSAI
              ? BaseVersionedStorageFormat.BONSAI_WITH_RECEIPT_COMPACTION
              : (dataStorageFormat == X_BONSAI_ARCHIVE
                  ? BaseVersionedStorageFormat.BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION
                  : BaseVersionedStorageFormat.FOREST_WITH_RECEIPT_COMPACTION);
      assertThat(DatabaseMetadata.lookUpFrom(tempDataDir).getVersionedStorageFormat())
          .isEqualTo(expectedVersion);
    }
  }

  @Test
  public void shouldFailIfDbExistsAndNoMetadataFileFound() throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    Files.createDirectories(tempDatabaseDir);
    Files.createDirectories(tempDataDir);
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, FOREST);

    final RocksDBKeyValueStorageFactory storageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    try (final var storage = storageFactory.create(segment, commonConfiguration, metricsSystem)) {
      fail("Must fail if db is present but metadata is not");
    } catch (StorageException se) {
      assertThat(se)
          .hasMessage(
              "Database exists but metadata file not found, without it there is no safe way to open the database");
    }
  }

  @Test
  public void shouldDetectCorrectMetadataV1AndUpgrade() throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    Files.createDirectories(tempDataDir);
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, BONSAI);

    Utils.createDatabaseMetadataV1(tempDataDir, BONSAI);

    final RocksDBKeyValueStorageFactory storageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    try (final var storage = storageFactory.create(segment, commonConfiguration, metricsSystem)) {
      assertThat(DatabaseMetadata.lookUpFrom(tempDataDir).getVersionedStorageFormat())
          .isEqualTo(BaseVersionedStorageFormat.BONSAI_WITH_RECEIPT_COMPACTION);
      assertThat(storageFactory.isSegmentIsolationSupported()).isTrue();
    }
  }

  @Test
  public void shouldFailInCaseOfUnmanagedRollback() throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    Files.createDirectories(tempDatabaseDir);
    Files.createDirectories(tempDataDir);
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, BONSAI);

    Utils.createDatabaseMetadataV1(tempDataDir, BONSAI);

    final RocksDBKeyValueStorageFactory storageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    storageFactory.create(segment, commonConfiguration, metricsSystem);
    storageFactory.close();

    Utils.createDatabaseMetadataV2(tempDataDir, BONSAI, 1);

    final RocksDBKeyValueStorageFactory rolledbackStorageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
    assertThatThrownBy(
            () -> rolledbackStorageFactory.create(segment, commonConfiguration, metricsSystem))
        .isInstanceOf(StorageException.class)
        .hasMessageStartingWith("Database unsafe downgrade detect");
  }

  @Test
  public void shouldThrowExceptionWhenVersionNumberIsInvalid() throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    Files.createDirectories(tempDatabaseDir);
    Files.createDirectories(tempDataDir);
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, FOREST);

    Utils.createDatabaseMetadataV1(tempDataDir, 99);
    assertThatThrownBy(
            () ->
                new RocksDBKeyValueStorageFactory(
                        () -> rocksDbConfiguration,
                        segments,
                        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)
                    .create(segment, commonConfiguration, metricsSystem))
        .isInstanceOf(StorageException.class)
        .hasMessageStartingWith("Unsupported db version");
  }

  @Test
  public void shouldThrowExceptionWhenExistingDatabaseFormatDiffersFromConfig() throws Exception {

    final DataStorageFormat actualDatabaseFormat = FOREST;
    final DataStorageFormat expectedDatabaseFormat = BONSAI;

    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    Files.createDirectories(tempDatabaseDir);
    Files.createDirectories(tempDataDir);
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, BONSAI);

    Utils.createDatabaseMetadataV2(tempDataDir, FOREST, 2);

    assertThatThrownBy(
            () ->
                new RocksDBKeyValueStorageFactory(
                        () -> rocksDbConfiguration,
                        segments,
                        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)
                    .create(segment, commonConfiguration, metricsSystem))
        .isInstanceOf(StorageException.class)
        .hasMessage(
            "Database format mismatch: DB at %s is %s but config expects %s. "
                + "Please check your config.",
            tempDataDir.toAbsolutePath(), actualDatabaseFormat, expectedDatabaseFormat);
  }

  @Test
  public void shouldDetectCorrectMetadataV2AndSetSegmentationFieldDuringCreation()
      throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    Files.createDirectories(tempDatabaseDir);
    Files.createDirectories(tempDataDir);
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, FOREST);

    Utils.createDatabaseMetadataV2(tempDataDir, FOREST, 2);

    final RocksDBKeyValueStorageFactory storageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
    try (final var storage = storageFactory.create(segment, commonConfiguration, metricsSystem)) {
      assertThat(DatabaseMetadata.lookUpFrom(tempDataDir).getVersionedStorageFormat())
          .isEqualTo(BaseVersionedStorageFormat.FOREST_WITH_RECEIPT_COMPACTION);
      assertThatCode(storageFactory::isSegmentIsolationSupported).doesNotThrowAnyException();
    }
  }

  @Test
  public void shouldThrowExceptionWhenMetaDataFileIsCorrupted() throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    Files.createDirectories(tempDatabaseDir);
    Files.createDirectories(tempDataDir);
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, FOREST);

    Utils.createDatabaseMetadataRaw(tempDataDir, "{\"🦄\":1}");

    assertThatThrownBy(
            () ->
                new RocksDBKeyValueStorageFactory(
                        () -> rocksDbConfiguration,
                        segments,
                        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)
                    .create(segment, commonConfiguration, metricsSystem))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Invalid metadata file");
    ;

    Utils.createDatabaseMetadataRaw(tempDataDir, "{\"version\"=1}");

    assertThatThrownBy(
            () ->
                new RocksDBKeyValueStorageFactory(
                        () -> rocksDbConfiguration,
                        segments,
                        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)
                    .create(segment, commonConfiguration, metricsSystem))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Invalid metadata file");
    ;
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  public void shouldCreateDBCorrectlyIfSymlink() throws Exception {
    final Path tempRealDataDir = Files.createDirectories(temporaryFolder.resolve("real-data-dir"));
    final Path tempSymLinkDataDir =
        Files.createSymbolicLink(temporaryFolder.resolve("symlink-data-dir"), tempRealDataDir);
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    mockCommonConfiguration(tempSymLinkDataDir, tempDatabaseDir, FOREST);

    Utils.createDatabaseMetadataV2(tempSymLinkDataDir, FOREST, 2);

    final RocksDBKeyValueStorageFactory storageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    // Ensure that having created everything via a symlink data dir the DB meta-data has been
    // created correctly
    try (final var storage = storageFactory.create(segment, commonConfiguration, metricsSystem)) {
      assertThat(DatabaseMetadata.lookUpFrom(tempRealDataDir).getVersionedStorageFormat())
          .isEqualTo(BaseVersionedStorageFormat.FOREST_WITH_RECEIPT_COMPACTION);
    }
  }

  /**
   * Regression guard for the fresh-Bonsai startup crash: a Bonsai-only segment (analogous to
   * TRIE_BRANCH_STORAGE) must be created and usable on a brand new Bonsai database. Under the buggy
   * state where the active format's own segments were registered as ignorable, this segment would
   * be removed (absent on a fresh DB) and never created, causing "Column handle not found".
   */
  @Test
  public void shouldCreateAndOpenBonsaiOnlySegmentOnFreshBonsaiDb() throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, BONSAI);

    final List<SegmentIdentifier> formatSegments =
        List.of(FormatSegment.DEFAULT, FormatSegment.FOREST_ONLY, FormatSegment.BONSAI_ONLY);

    final RocksDBKeyValueStorageFactory storageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration,
            formatSegments,
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    try (final SegmentedKeyValueStorage storage =
        storageFactory.create(formatSegments, commonConfiguration, metricsSystem)) {
      final byte[] key = {0x01};
      final byte[] value = {0x0f};
      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      tx.put(FormatSegment.BONSAI_ONLY, key, value);
      tx.commit();
      assertThat(storage.get(FormatSegment.BONSAI_ONLY, key)).contains(value);
    }
  }

  /**
   * Cross-format open: a database converted from Bonsai to Forest retains the Bonsai column
   * families on disk. Opening it as Forest must succeed (the leftover Bonsai CF is opened as an
   * existing ignorable segment) and the Forest world-state segment must remain usable.
   *
   * <p>The converted DB is constructed at the lower level (a {@link
   * TransactionDBRocksDBColumnarKeyValueStorage}, which is what the factory uses for FOREST)
   * because the factory cannot physically create a Bonsai-only CF while in FOREST mode (its {@code
   * includeInDatabaseFormat} excludes it), and reopening a Bonsai-metadata DB through the factory
   * as FOREST would trip the format-mismatch guard. We pre-create DEFAULT + FOREST_ONLY +
   * BONSAI_ONLY directly, then write FOREST metadata and reopen through the factory to verify the
   * factory's cross-format ignorable augmentation opens the leftover Bonsai CF.
   */
  @Test
  public void shouldOpenConvertedDbWithLeftoverCrossFormatColumnFamily() throws Exception {
    final Path tempDataDir = temporaryFolder.resolve("data");
    final Path tempDatabaseDir = temporaryFolder.resolve("db");
    Files.createDirectories(tempDataDir);
    mockCommonConfiguration(tempDataDir, tempDatabaseDir, FOREST);

    final byte[] worldStateKey = {0x01};
    final byte[] worldStateValue = {0x0a};

    // Build a database that physically contains the Forest world-state CF AND a leftover Bonsai CF,
    // mirroring a Bonsai->Forest converted database.
    try (final SegmentedKeyValueStorage seedStore =
        new TransactionDBRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder().databaseDir(tempDatabaseDir).build(),
            List.of(FormatSegment.DEFAULT, FormatSegment.FOREST_ONLY, FormatSegment.BONSAI_ONLY),
            List.of(),
            metricsSystem,
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      final SegmentedKeyValueStorageTransaction tx = seedStore.startTransaction();
      tx.put(FormatSegment.FOREST_ONLY, worldStateKey, worldStateValue);
      tx.put(FormatSegment.BONSAI_ONLY, new byte[] {0x02}, new byte[] {0x0b});
      tx.commit();
    }

    // Mark the database as FOREST so the factory opens it in FOREST mode.
    Utils.createDatabaseMetadataV2(tempDataDir, FOREST, 2);

    // Reopen through the factory. segmentsForFormat(FOREST) excludes BONSAI_ONLY, so it lands in
    // the
    // cross-format ignorable set; since it physically exists it must be opened, otherwise RocksDB
    // open fails. The factory must not throw.
    final List<SegmentIdentifier> formatSegments =
        List.of(FormatSegment.DEFAULT, FormatSegment.FOREST_ONLY, FormatSegment.BONSAI_ONLY);
    final RocksDBKeyValueStorageFactory storageFactory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration,
            formatSegments,
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    assertThatCode(
            () -> {
              try (final SegmentedKeyValueStorage storage =
                  storageFactory.create(
                      List.of(FormatSegment.DEFAULT, FormatSegment.FOREST_ONLY),
                      commonConfiguration,
                      metricsSystem)) {
                // The Forest world-state segment is opened normally and its data is readable.
                assertThat(storage.get(FormatSegment.FOREST_ONLY, worldStateKey))
                    .contains(worldStateValue);
              }
            })
        .doesNotThrowAnyException();
  }

  private void mockCommonConfiguration(
      final Path tempDataDir, final Path tempDatabaseDir, final DataStorageFormat format) {
    when(commonConfiguration.getStoragePath()).thenReturn(tempDatabaseDir);
    when(commonConfiguration.getDataPath()).thenReturn(tempDataDir);
    lenient().when(dataStorageConfiguration.getDatabaseFormat()).thenReturn(format);
    lenient()
        .when(commonConfiguration.getDataStorageConfiguration())
        .thenReturn(dataStorageConfiguration);
  }

  /**
   * Format-aware segments used to exercise the factory's cross-format handling. Mirrors the real
   * {@code KeyValueSegmentIdentifier} restrictions: FOREST_ONLY behaves like WORLD_STATE (FOREST
   * only) and BONSAI_ONLY behaves like TRIE_BRANCH_STORAGE (Bonsai/archive only).
   */
  public enum FormatSegment implements SegmentIdentifier {
    DEFAULT("default".getBytes(StandardCharsets.UTF_8), EnumSet.allOf(DataStorageFormat.class)),
    FOREST_ONLY(new byte[] {0x71}, EnumSet.of(FOREST)),
    BONSAI_ONLY(new byte[] {0x72}, EnumSet.of(BONSAI, X_BONSAI_ARCHIVE));

    private final byte[] id;
    private final String nameAsUtf8;
    private final EnumSet<DataStorageFormat> formats;

    FormatSegment(final byte[] id, final EnumSet<DataStorageFormat> formats) {
      this.id = id;
      this.nameAsUtf8 = new String(id, StandardCharsets.UTF_8);
      this.formats = formats;
    }

    @Override
    public String getName() {
      return nameAsUtf8;
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

    @Override
    public boolean includeInDatabaseFormat(final DataStorageFormat format) {
      return formats.contains(format);
    }
  }
}
