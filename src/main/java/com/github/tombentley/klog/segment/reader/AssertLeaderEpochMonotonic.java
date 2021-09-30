/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.tombentley.klog.segment.reader;

import java.util.function.Function;

import com.github.tombentley.klog.segment.model.Batch;

public class AssertLeaderEpochMonotonic implements Function<Batch, Batch> {
    private long lastLeaderEpoch = -1;

    @Override
    public Batch apply(Batch batch) {
        if (lastLeaderEpoch != -1 && lastLeaderEpoch >= batch.partitionLeaderEpoch()) {
            throw new IllegalStateException(String.format("%s:%d: Batch partition leader epoch %d is >= previous batch partition leader epoch",
                    batch.filename(), batch.line(), batch.partitionLeaderEpoch(), lastLeaderEpoch));
        }
        return batch;
    }
}
