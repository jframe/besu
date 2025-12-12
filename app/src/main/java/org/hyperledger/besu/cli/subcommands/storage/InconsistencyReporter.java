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

import org.hyperledger.besu.cli.subcommands.storage.Inconsistency.InconsistencyType;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reports and tracks flat DB consistency check inconsistencies. */
public class InconsistencyReporter implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(InconsistencyReporter.class);

  private final Path outputFile;
  private final PrintWriter writer;
  private final AtomicLong totalInconsistencies = new AtomicLong(0);
  private final Map<InconsistencyType, AtomicLong> countsByType = new HashMap<>();

  /**
   * Creates a new inconsistency reporter.
   *
   * @param outputFile path to the CSV output file
   * @throws IOException if the output file cannot be created
   */
  public InconsistencyReporter(final Path outputFile) throws IOException {
    this.outputFile = outputFile;
    BufferedWriter bufferedWriter = Files.newBufferedWriter(outputFile, UTF_8);
    this.writer = new PrintWriter(bufferedWriter);

    // Initialize counters for all types
    for (InconsistencyType type : InconsistencyType.values()) {
      countsByType.put(type, new AtomicLong(0));
    }

    writeHeader();
  }

  /** Writes the CSV header row. */
  private void writeHeader() {
    writer.println(
        "BlockNumber,BlockHash,Type,Address,StorageKey,Field,Expected,Actual,Description");
  }

  /**
   * Reports an inconsistency by writing it to the CSV file.
   *
   * @param inconsistency the inconsistency to report
   */
  public void report(final Inconsistency inconsistency) {
    totalInconsistencies.incrementAndGet();
    countsByType.get(inconsistency.type()).incrementAndGet();

    // Write CSV line with proper escaping
    writer.printf(
        "%d,%s,%s,%s,%s,%s,\"%s\",\"%s\",\"%s\"%n",
        inconsistency.blockNumber(),
        inconsistency.blockHash().toHexString(),
        inconsistency.type(),
        inconsistency.address().toHexString(),
        inconsistency.storageKey().map(Object::toString).orElse(""),
        inconsistency.fieldName(),
        escapeQuotes(inconsistency.expectedValue()),
        escapeQuotes(inconsistency.actualValue()),
        escapeQuotes(inconsistency.description()));

    writer.flush(); // Flush after each write for real-time monitoring
  }

  /**
   * Escapes double quotes in CSV values.
   *
   * @param value the value to escape
   * @return the escaped value
   */
  private String escapeQuotes(final String value) {
    return value.replace("\"", "\"\"");
  }

  /**
   * Gets the total number of inconsistencies found.
   *
   * @return total inconsistency count
   */
  public long getTotalInconsistencies() {
    return totalInconsistencies.get();
  }

  /**
   * Gets the count of inconsistencies for a specific type.
   *
   * @param type the inconsistency type
   * @return count of inconsistencies of that type
   */
  public long getCountForType(final InconsistencyType type) {
    return countsByType.get(type).get();
  }

  /** Prints a summary of all inconsistencies found to the console. */
  public void printSummary() {
    LOG.info("\n=== Flat DB Consistency Check Summary ===");
    LOG.info("Total inconsistencies found: {}", totalInconsistencies.get());

    if (totalInconsistencies.get() > 0) {
      LOG.info("\nBreakdown by type:");
      for (Map.Entry<InconsistencyType, AtomicLong> entry : countsByType.entrySet()) {
        long count = entry.getValue().get();
        if (count > 0) {
          LOG.info("  - {}: {}", entry.getKey(), count);
        }
      }
      LOG.info("\nDetailed output written to: {}", outputFile.toAbsolutePath());
    } else {
      LOG.info("No inconsistencies found! Flat DB is consistent with trielog data.");
    }
  }

  @Override
  public void close() {
    if (writer != null) {
      writer.close();
    }
  }
}
