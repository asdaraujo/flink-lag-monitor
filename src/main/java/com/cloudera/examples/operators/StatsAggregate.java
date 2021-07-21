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

package com.cloudera.examples.operators;

import com.cloudera.examples.data.MessageLag;
import com.cloudera.examples.data.MessageLagStats;
import org.apache.flink.api.common.functions.AggregateFunction;

public class StatsAggregate implements AggregateFunction<MessageLag, StatsAggregate.MessageAccumulator, MessageLagStats> {

    @Override
    public MessageAccumulator createAccumulator() {
        return new MessageAccumulator(0L, 0L, 0L, Long.MAX_VALUE, 0L);
    }

    @Override
    public MessageAccumulator add(MessageLag messageLag, MessageAccumulator acc) {
        return acc.add(messageLag);
    }

    @Override
    public MessageLagStats getResult(MessageAccumulator acc) {
        return new MessageLagStats(
                null,
                null,
                acc.min,
                acc.max,
                acc.sum/acc.count,
                Math.round(Math.sqrt(acc.sumOfSquares/acc.count - Math.pow(acc.sum/acc.count, 2))),
                acc.count);
    }

    @Override
    public MessageAccumulator merge(MessageAccumulator acc, MessageAccumulator acc1) {
        return acc.add(acc1);
    }

    protected static class MessageAccumulator {
        public long sum;
        public long sumOfSquares;
        public long count;
        public long min;
        public long max;

        public MessageAccumulator(long sum, long sumOfSquares, long count, long min, long max) {
            this.sum = sum;
            this.sumOfSquares = sumOfSquares;
            this.count = count;
            this.min = min;
            this.max = max;
        }

        public MessageAccumulator add(MessageLag messageLag) {
            long lagMs = messageLag.lagMs();
            return new MessageAccumulator(
                    this.sum + lagMs,
                    this.sumOfSquares + (long) Math.pow(lagMs, 2),
                    this.count + 1,
                    Math.min(this.min, lagMs),
                    Math.max(this.max, lagMs)
            );
        }

        public MessageAccumulator add(MessageAccumulator acc) {
            return new MessageAccumulator(
                    this.sum + acc.sum,
                    this.sumOfSquares + acc.sumOfSquares,
                    this.count + acc.count,
                    Math.min(this.min, acc.min),
                    Math.max(this.max, acc.max)
            );
        }
    }
}
