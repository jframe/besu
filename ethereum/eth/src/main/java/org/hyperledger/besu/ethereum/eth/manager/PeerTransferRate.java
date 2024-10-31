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
package org.hyperledger.besu.ethereum.eth.manager;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerTransferRate implements Comparable<PeerTransferRate> {
  private static final Logger LOG = LoggerFactory.getLogger(PeerReputation.class);
  private static final int RATES_EXPIRY_TIME_LIMIT = 60;
  private final Queue<PeerRate> rates = new ConcurrentLinkedQueue<>();
  private int rate;

  public void recordTransferRate(final Duration duration, final long bytesDownloaded) {
    final Instant currentTime = Instant.now();

    // Remove entries older than 1 minute
    while (!rates.isEmpty()
        && rates.peek().timestamp
            < currentTime.minus(RATES_EXPIRY_TIME_LIMIT, ChronoUnit.SECONDS).toEpochMilli()) {
      rates.poll();
    }

    rates.add(new PeerRate(duration.toMillis(), currentTime.toEpochMilli(), bytesDownloaded));

    final long sumDuration = rates.stream().mapToLong(r -> r.duration).sum();
    final long sumBytesDownloaded = rates.stream().mapToLong(r -> r.bytesDownloaded).sum();
    final int meanTransferRate = (int) (sumBytesDownloaded / sumDuration);

    LOG.debug(
        "Mean transfer rate: {}, previous rate: {}, entries {}, bytesDownloaded: {}, duration: {}, sumDuration: {}, sumBytesDownloaded: {}",
        meanTransferRate,
        rate,
        rates.size(),
        bytesDownloaded,
        duration,
        sumDuration,
        sumBytesDownloaded);
    rate = meanTransferRate;
  }

  public int getRate() {
    return rate;
  }

  public int getCount() {
    return rates.size();
  }

  public int getTotalBytesDownloaded() {
    return rates.stream().mapToInt(r -> (int) r.bytesDownloaded).sum();
  }

  @Override
  public int compareTo(final PeerTransferRate o) {
    return Integer.compare(rate, o.rate);
  }

  record PeerRate(long duration, long timestamp, long bytesDownloaded) {}
}
