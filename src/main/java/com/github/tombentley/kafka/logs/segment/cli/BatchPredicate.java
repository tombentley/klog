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
package com.github.tombentley.kafka.logs.segment.cli;

import java.util.function.Predicate;

import com.github.tombentley.kafka.logs.segment.model.Batch;

class BatchPredicate {
    static Predicate<Batch> predicate(Integer pid, Integer producerEpoch, Integer leaderEpoch) {
        Predicate<Batch> predicate = null;
        if (pid != null) {
            int pidPrim = pid;
            predicate = batch -> batch.producerId() == pidPrim;
        }
        if (producerEpoch != null) {
            int pe = producerEpoch;
            Predicate<Batch> producerPred = batch -> batch.producerEpoch() == pe;
            predicate = predicate != null ? predicate.and(producerPred) : producerPred;
        }
        if (leaderEpoch != null) {
            int le = leaderEpoch;
            Predicate<Batch> leaderPred = batch -> batch.partitionLeaderEpoch() == le;
            predicate = predicate != null ? predicate.and(leaderPred) : leaderPred;
        }
        return predicate;
    }
}
