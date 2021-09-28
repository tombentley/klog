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
package com.github.tombentley.kafka.logs.segment.model;

import java.time.Instant;

/**
 * A data record from a {@code __transaction_state} segment with a state change payload
 * kafka-dump-log.sh output is a single line like:
 * <pre>
 * | offset: 63807159 CreateTime: 1630834794895 keysize: 30 valuesize: 37 sequence: -1 headerKeys: []
 * key: transaction_metadata::transactionalId=MY_TXNAL_ID payload: producerId:891464,producerEpoch:1,state=CompleteCommit,
 * partitions=[],txnLastUpdateTimestamp=1630834794890,txnTimeoutMs=120000
 * </pre>
 */
public record TransactionStateChangeMessage(String filename,
                                            int line,
                                            long offset,
                                            long createTime,
                                            int keySize,
                                            int valueSize,
                                            int sequence,
                                            String headerKeys,
                                            String transactionId,
                                            long producerId,
                                            short producerEpoch,
                                            State state,
                                            String partitions,
                                            long txnLastUpdateTimestamp,
                                            long txnTimeoutMs) implements TransactionStateMessage {
    public enum State {
        Ongoing {
            @Override
            public boolean validPrevious(State previous) {
                return previous == Ongoing || previous == CompleteAbort || previous == CompleteCommit || previous == Empty;
            }
        },
        PrepareCommit {
            @Override
            public boolean validPrevious(State previous) {
                return previous == Ongoing;
            }
        },
        PrepareAbort {
            @Override
            public boolean validPrevious(State previous) {
                return previous == Ongoing;
            }
        },
        CompleteCommit {
            @Override
            public boolean validPrevious(State previous) {
                return previous == PrepareCommit;
            }
        },
        CompleteAbort {
            @Override
            public boolean validPrevious(State previous) {
                return previous == PrepareAbort;
            }
        },
        Empty {
            @Override
            public boolean validPrevious(State previous) {
                return previous == CompleteAbort || previous == CompleteCommit || previous == Empty;
            }
        },
        Dead {
            @Override
            public boolean validPrevious(State previous) {
                return previous == Empty || previous == CompleteAbort || previous == CompleteCommit;
            }
        };
        public abstract boolean validPrevious(State previous);
    }

    @Override
    public String toString() {
        return "TransactionStateMessage(" +
                "offset=" + offset +
                ", createTime=" + Instant.ofEpochMilli(createTime) +
                ", keySize=" + keySize +
                ", valueSize=" + valueSize +
                ", sequence=" + sequence +
                ", headerKeys='" + headerKeys + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", state=" + state +
                ", partitions='" + partitions + '\'' +
                ", txnLastUpdateTimestamp=" + Instant.ofEpochMilli(txnLastUpdateTimestamp) +
                ", txnTimeoutMs=" + txnTimeoutMs +
                ')';
    }

    public ProducerSession session() {
        return new ProducerSession(producerId, producerEpoch);
    }
}

