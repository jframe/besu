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
package org.hyperledger.besu.ethereum.eth.manager;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerReputation implements Comparable<PeerReputation> {
  private static final Logger LOG = LoggerFactory.getLogger(PeerReputation.class);
  static final long USELESS_RESPONSE_WINDOW_IN_MILLIS =
      TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
  static final int DEFAULT_MAX_SCORE = Integer.MAX_VALUE;
  static final int DEFAULT_INITIAL_SCORE = Integer.MAX_VALUE;
  public static final int TIMEOUT_THRESHOLD = 5;
  public static final int USELESS_RESPONSE_THRESHOLD = 5;

  private final ConcurrentMap<Integer, AtomicInteger> timeoutCountByRequestType =
      new ConcurrentHashMap<>();
  private final Queue<Long> uselessResponseTimes = new ConcurrentLinkedQueue<>();
  private final String id;

  private int score;
  private final int maxScore;
  private final Queue<PeerRate> rates = new ConcurrentLinkedQueue<>();

  public PeerReputation(final String id) {
    this(DEFAULT_INITIAL_SCORE, DEFAULT_MAX_SCORE, id);
  }

  public PeerReputation(final int initialScore, final int maxScore, final String id) {
    checkArgument(
        initialScore <= maxScore, "Initial score must be less than or equal to max score");
    this.maxScore = maxScore;
    this.score = initialScore;
    this.id = id;
  }

  public Optional<DisconnectReason> recordRequestTimeout(
      final int requestCode, final EthPeer peer) {
    final int newTimeoutCount = getOrCreateTimeoutCount(requestCode).incrementAndGet();
    if (newTimeoutCount >= TIMEOUT_THRESHOLD) {
      LOG.debug(
          "Disconnection triggered by {} repeated timeouts for requestCode {} for peer {}",
          newTimeoutCount,
          requestCode,
          peer.getLoggableId());
      score -= getLargeAdjustment();
      return Optional.of(DisconnectReason.TIMEOUT);
    } else {
      score -= getSmallAdjustment();
      return Optional.empty();
    }
  }

  private int getSmallAdjustment() {
    return (int) (score * 0.01); // 1% of the current score
  }

  private int getLargeAdjustment() {
    return (int) (score * 0.1); // 10% of the current score
  }

  public void resetTimeoutCount(final int requestCode) {
    timeoutCountByRequestType.remove(requestCode);
  }

  private AtomicInteger getOrCreateTimeoutCount(final int requestCode) {
    return timeoutCountByRequestType.computeIfAbsent(requestCode, code -> new AtomicInteger());
  }

  public Map<Integer, AtomicInteger> timeoutCounts() {
    return timeoutCountByRequestType;
  }

  public Optional<DisconnectReason> recordUselessResponse(
      final long timestamp, final EthPeer peer) {
    uselessResponseTimes.add(timestamp);
    while (shouldRemove(uselessResponseTimes.peek(), timestamp)) {
      uselessResponseTimes.poll();
    }
    if (uselessResponseTimes.size() >= USELESS_RESPONSE_THRESHOLD) {
      score -= getLargeAdjustment();
      LOG.debug(
          "Disconnection triggered by exceeding useless response threshold for peer {}",
          peer.getLoggableId());
      return Optional.of(DisconnectReason.USELESS_PEER_USELESS_RESPONSES);
    } else {
      score -= getSmallAdjustment();
      return Optional.empty();
    }
  }

  public void recordUsefulResponse() {
    if (score < maxScore) {
      score = Math.min(maxScore, score + getSmallAdjustment());
    }
  }

  private boolean shouldRemove(final Long timestamp, final long currentTimestamp) {
    return timestamp != null && timestamp + USELESS_RESPONSE_WINDOW_IN_MILLIS < currentTimestamp;
  }

  public void recordTransferRate(final Duration duration, final long bytesDownloaded) {
    long currentTime = System.currentTimeMillis();
    long tenMinutesAgo = currentTime - TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    // Remove entries older than 10 minutes
    while (!rates.isEmpty() && rates.peek().timestamp < tenMinutesAgo) {
      rates.poll();
    }

    rates.add(new PeerRate(duration.toNanos(), currentTime, bytesDownloaded));
    double meanDuration = rates.stream().mapToLong(r -> r.duration).sum();
    double meanBytesDownloaded = rates.stream().mapToLong(r -> r.bytesDownloaded).sum();
    int meanTransferRate = (int) (meanBytesDownloaded / meanDuration);

    LOG.info(
        "Mean transfer rate: {}, previous rate: {}, entries {}, id: {}, bytesDownloaded: {}, duration: {}",
        meanTransferRate,
        score,
        rates.size(),
        id,
        bytesDownloaded,
        duration);

    // Update score based on mean transfer rate
    if (meanTransferRate > 0) {
      score = meanTransferRate;
    }
  }

  @Override
  public String toString() {
    return String.format(
        "PeerReputation score: %d, timeouts: %s, useless: %s",
        score, timeoutCounts(), uselessResponseTimes.size());
  }

  @Override
  public int compareTo(final @Nonnull PeerReputation otherReputation) {
    return Integer.compare(this.score, otherReputation.score);
  }

  public int getScore() {
    return score;
  }

  record PeerRate(long duration, long timestamp, long bytesDownloaded) {}
}
