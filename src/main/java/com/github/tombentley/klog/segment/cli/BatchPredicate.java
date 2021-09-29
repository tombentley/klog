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
package com.github.tombentley.klog.segment.cli;

import java.util.function.Predicate;

import com.github.tombentley.klog.segment.model.Batch;
import com.github.tombentley.klog.segment.model.TransactionStateChange;
import com.github.tombentley.klog.segment.model.TransactionStateMessage;
import com.github.tombentley.klog.segment.reader.Segment;

class BatchPredicate {
    static Predicate<Batch> predicate(Segment.Type type, Integer pid, Integer producerEpoch, Integer leaderEpoch, String transactionalId) {
        Predicate<Batch> predicate = null;
        if (pid != null) {
            int pidPrim = pid;
            if (type == Segment.Type.DATA) {
                predicate = batch -> batch.producerId() == pidPrim;
            } else if (type == Segment.Type.TRANSACTION_STATE) {
                predicate = batch -> batch.messages().stream().anyMatch(msg -> (msg instanceof TransactionStateChange) && ((TransactionStateChange) msg).producerId() == pidPrim);
            } else {
                throw new RuntimeException();
            }
        }
        if (producerEpoch != null) {
            int pe = producerEpoch;
            Predicate<Batch> producerPred;
            if (type == Segment.Type.DATA) {
                producerPred = batch -> batch.producerEpoch() == pe;
            } else if (type == Segment.Type.TRANSACTION_STATE) {
                producerPred = batch -> batch.messages().stream().anyMatch(msg -> (msg instanceof TransactionStateChange) && ((TransactionStateChange) msg).producerEpoch() == pe);
            } else {
                throw new RuntimeException();
            }
            predicate = predicate != null ? predicate.and(producerPred) : producerPred;
        }
        if (leaderEpoch != null) {
            int le = leaderEpoch;
            Predicate<Batch> leaderPred = batch -> batch.partitionLeaderEpoch() == le;
            predicate = predicate != null ? predicate.and(leaderPred) : leaderPred;
        }
        if (transactionalId != null) {
            if (type != Segment.Type.TRANSACTION_STATE) {
                throw new RuntimeException();
            }
            Predicate<Batch> transactionalPred = batch -> batch.messages().stream().anyMatch(msg -> (msg instanceof TransactionStateMessage) && ((TransactionStateMessage) msg).transactionId().equals(transactionalId));
            predicate = predicate != null ? predicate.and(transactionalPred) : transactionalPred;
        }
        return predicate;
    }
}
