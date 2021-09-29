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
package com.github.tombentley.klog.segment.model;

import java.time.Instant;
import java.util.Objects;

/**
 * A data record from a {@code __transaction_state} segment with a state change payload
 * kafka-dump-log.sh output is a single line like:
 * <pre>
 * | offset: 63807159 CreateTime: 1630834794895 keysize: 30 valuesize: 37 sequence: -1 headerKeys: []
 * key: transaction_metadata::transactionalId=MY_TXNAL_ID payload: producerId:891464,producerEpoch:1,state=CompleteCommit,
 * partitions=[],txnLastUpdateTimestamp=1630834794890,txnTimeoutMs=120000
 * </pre>
 */
public final class TransactionStateChange implements TransactionStateMessage {
    private final String filename;
    private final int line;
    private final long offset;
    private final long createTime;
    private final int keySize;
    private final int valueSize;
    private final int sequence;
    private final String headerKeys;
    private final String transactionId;
    private final long producerId;
    private final short producerEpoch;
    private final State state;
    private final String partitions;
    private final long txnLastUpdateTimestamp;
    private final long txnTimeoutMs;

    /**
     */
    public TransactionStateChange(String filename,
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
                                  long txnTimeoutMs) {
        this.filename = filename;
        this.line = line;
        this.offset = offset;
        this.createTime = createTime;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.sequence = sequence;
        this.headerKeys = headerKeys;
        this.transactionId = transactionId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.state = state;
        this.partitions = partitions;
        this.txnLastUpdateTimestamp = txnLastUpdateTimestamp;
        this.txnTimeoutMs = txnTimeoutMs;
    }

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

    public String filename() {
        return filename;
    }

    public int line() {
        return line;
    }

    public long offset() {
        return offset;
    }

    public long createTime() {
        return createTime;
    }

    public int keySize() {
        return keySize;
    }

    public int valueSize() {
        return valueSize;
    }

    public int sequence() {
        return sequence;
    }

    public String headerKeys() {
        return headerKeys;
    }

    public String transactionId() {
        return transactionId;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public State state() {
        return state;
    }

    public String partitions() {
        return partitions;
    }

    public long txnLastUpdateTimestamp() {
        return txnLastUpdateTimestamp;
    }

    public long txnTimeoutMs() {
        return txnTimeoutMs;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (TransactionStateChange) obj;
        return Objects.equals(this.filename, that.filename) &&
                this.line == that.line &&
                this.offset == that.offset &&
                this.createTime == that.createTime &&
                this.keySize == that.keySize &&
                this.valueSize == that.valueSize &&
                this.sequence == that.sequence &&
                Objects.equals(this.headerKeys, that.headerKeys) &&
                Objects.equals(this.transactionId, that.transactionId) &&
                this.producerId == that.producerId &&
                this.producerEpoch == that.producerEpoch &&
                Objects.equals(this.state, that.state) &&
                Objects.equals(this.partitions, that.partitions) &&
                this.txnLastUpdateTimestamp == that.txnLastUpdateTimestamp &&
                this.txnTimeoutMs == that.txnTimeoutMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, line, offset, createTime, keySize, valueSize, sequence, headerKeys, transactionId, producerId, producerEpoch, state, partitions, txnLastUpdateTimestamp, txnTimeoutMs);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.stateChange(this);
    }
}

