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

public class MessageLag {
    public Integer hash;
    public Long timestamp0;
    public Long timestamp1;

    public MessageLag() {}

    public MessageLag(Integer hash, Long timestamp0, Long timestamp1) {
        this.hash = hash;
        this.timestamp0 = timestamp0;
        this.timestamp1 = timestamp1;
    }

    public Long lagMs() {
        return timestamp1 - timestamp0;
    };

    @Override
    public String toString() {
        return String.format("MessageLag{%s, %s, %s, lag = %s ms}", hash, timestamp0, timestamp1, lagMs());
    }

    public String toJson() {
        return String.format("{\"hash\":%s, \"timestamp0\":%s, \"timestamp1\":%s, \"lag_ms\":%s}", hash, timestamp0, timestamp1, lagMs());
    }
}
