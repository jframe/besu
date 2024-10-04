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
  private final Queue<PeerRate> rates = new ConcurrentLinkedQueue<>();
  private int rate;

  public void recordTransferRate(final Duration duration, final long bytesDownloaded) {
    final Instant currentTime = Instant.now();
    final Instant tenMinutesAgo = currentTime.minus(10, ChronoUnit.MINUTES);

    // Remove entries older than 10 minutes
    while (!rates.isEmpty() && rates.peek().timestamp < tenMinutesAgo.toEpochMilli()) {
      rates.poll();
    }

    rates.add(new PeerRate(duration.toMillis(), currentTime.toEpochMilli(), bytesDownloaded));

    // Wait until we have enough data to calculate a mean transfer rate
    boolean hasOneMinutePassed =
        rates.peek() != null
            && rates.peek().timestamp < currentTime.minus(1, ChronoUnit.MINUTES).toEpochMilli();
    if (!hasOneMinutePassed) {
      LOG.info("Not enough data to calculate mean transfer rate");
      return;
    }

    final long sumDuration = rates.stream().mapToLong(r -> r.duration).sum();
    final long sumBytesDownloaded = rates.stream().mapToLong(r -> r.bytesDownloaded).sum();
    final int meanTransferRate = (int) (sumBytesDownloaded / sumDuration);

    LOG.info(
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

  @Override
  public int compareTo(final PeerTransferRate o) {
    return Integer.compare(rate, o.rate);
  }

  record PeerRate(long duration, long timestamp, long bytesDownloaded) {}
}
