/*
 * Copyright (c) 2021, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.examples.data;

public class MessageLagStats {
    public Long windowStart;
    public Long windowEnd;
    public Long minLagMs;
    public Long maxLagMs;
    public Long avgLagMs;
    public Long sdevLagMs;
    public Long count;

    public MessageLagStats() {}

    public MessageLagStats(Long windowStart, Long windowEnd, Long minLagMs, Long maxLagMs, Long avgLagMs, Long sdevLagMs, Long count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.minLagMs = minLagMs;
        this.maxLagMs = maxLagMs;
        this.avgLagMs = avgLagMs;
        this.sdevLagMs = sdevLagMs;
        this.count = count;
    }

    @Override
    public String toString() {
        return "MessageLagStats{" +
                "windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", minLagMs=" + minLagMs +
                ", maxLagMs=" + maxLagMs +
                ", avgLagMs=" + avgLagMs +
                ", sdevLagMs=" + sdevLagMs +
                ", count=" + count +
                '}';
    }

    public String toJson() {
        return String.format(
                "{" +
                        "\"window_start\":%s, " +
                        "\"window_end\":%s, " +
                        "\"min_lag_ms\":%s, " +
                        "\"max_lag_ms\":%s, " +
                        "\"avg_lag_ms\":%s, " +
                        "\"stddev_lag_ms\":%s, " +
                        "\"count\":%s" +
                        "}",
                windowStart, windowEnd, minLagMs, maxLagMs, avgLagMs, sdevLagMs, count);
    }
}
