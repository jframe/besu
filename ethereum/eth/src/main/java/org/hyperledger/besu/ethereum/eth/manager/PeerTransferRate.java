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
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerTransferRate {
  private static final Logger LOG = LoggerFactory.getLogger(PeerReputation.class);
  private static final int RATES_EXPIRY_TIME_LIMIT = 60;
  private final Map<String, Queue<PeerRate>> rates = new ConcurrentHashMap<>();
  private final Map<String, Integer> rate = new ConcurrentHashMap<>();

  public void recordTransferRate(
      final Duration duration, final long bytesDownloaded, final String messageName) {
    final Instant currentTime = Instant.now();

    final Queue<PeerRate> peerRates =
        rates.computeIfAbsent(messageName, k -> new ConcurrentLinkedQueue<>());

    // Remove entries older than 1 minute
    while (!peerRates.isEmpty()
        && peerRates.peek().timestamp
            < currentTime.minus(RATES_EXPIRY_TIME_LIMIT, ChronoUnit.SECONDS).toEpochMilli()) {
      peerRates.poll();
    }

    peerRates.add(new PeerRate(duration.toMillis(), currentTime.toEpochMilli(), bytesDownloaded));

    final long sumDuration = peerRates.stream().mapToLong(r -> r.duration).sum();
    final long sumBytesDownloaded = peerRates.stream().mapToLong(r -> r.bytesDownloaded).sum();
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
    rate.put(messageName, meanTransferRate);
  }

  public Map<String, Integer> getRates() {
    return rate;
  }

  public int getTotalRate() {
    return rate.values().stream().mapToInt(Integer::intValue).sum();
  }

  public Map<String, Integer> getCounts() {
    return rates.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
  }

  public Map<String, Long> getTotalBytesDownloaded() {
    return rates.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream().mapToLong(PeerRate::bytesDownloaded).sum()));
  }

  record PeerRate(long duration, long timestamp, long bytesDownloaded) {}
}
