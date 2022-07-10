package com.github.tombentley.klog.snapshot.model;/*
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
import com.github.tombentley.klog.snapshot.model.Located;
import com.github.tombentley.klog.snapshot.model.Visitor;
import java.util.Objects;

/**
 * A producer state record from a snapshot.
 * <br/><br/>
 * The {@code producerId}, {@code producerEpoch} and {@code coordinatorEpoch} can be used to rollback a hanging
 * transactions using the {@code kafka-transactions.sh}. The {@code lastTimestamp} can be used to approximately
 * detect when a transaction has been left hanging on a partition.
 * <br/><br/>
 * Sample output of {@code kafka-dump-log.sh}:
 * <pre>
 * producerId: 172079 producerEpoch: 15 coordinatorEpoch: 44 currentTxnFirstOffset: None lastTimestamp: 1655723423705
 * firstSequence: 0 lastSequence: 0 lastOffset: 933319222 offsetDelta: 0 timestamp: 1655723423705
 * </pre>
 */
public final class ProducerState implements Located {
    private final String filename;
    private final int line;

    // producer state
    private final long producerId; // producer ID
    private final short producerEpoch; // current producer epoch
    private final int coordinatorEpoch; // epoch of the last TX coordinator to send an end transaction marker
    private final long currentTxnFirstOffset; // first offset of the on-going transaction
    private final long lastTimestamp; // last timestamp from the oldest transaction

    // batch metadata
    private final int firstSequence; // first written sequence
    private final int lastSequence; // last written sequence
    private final long lastOffset; // last written offset
    private final int offsetDelta; // difference between last sequence and first sequence in last batch
    private final long timestamp; // max timestamp from the last written entry

    public ProducerState(String filename,
                         int line,
                         long producerId,
                         short producerEpoch,
                         int coordinatorEpoch,
                         long currentTxnFirstOffset,
                         long lastTimestamp,
                         int firstSequence,
                         int lastSequence,
                         long lastOffset,
                         int offsetDelta,
                         long timestamp) {
        this.filename = filename;
        this.line = line;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.coordinatorEpoch = coordinatorEpoch;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        this.lastTimestamp = lastTimestamp;
        this.firstSequence = firstSequence;
        this.lastSequence = lastSequence;
        this.lastOffset = lastOffset;
        this.offsetDelta = offsetDelta;
        this.timestamp = timestamp;
    }

    @Override
    public String filename() {
        return filename;
    }

    @Override
    public int line() {
        return line;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    public long currentTxnFirstOffset() {
        return currentTxnFirstOffset;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    public int firstSequence() {
        return firstSequence;
    }

    public int lastSequence() {
        return lastSequence;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public int offsetDelta() {
        return offsetDelta;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducerState that = (ProducerState) o;
        return line == that.line &&
                producerId == that.producerId &&
                producerEpoch == that.producerEpoch &&
                coordinatorEpoch == that.coordinatorEpoch &&
                currentTxnFirstOffset == that.currentTxnFirstOffset &&
                lastTimestamp == that.lastTimestamp &&
                firstSequence == that.firstSequence &&
                lastSequence == that.lastSequence &&
                lastOffset == that.lastOffset &&
                offsetDelta == that.offsetDelta &&
                timestamp == that.timestamp &&
                Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, line, producerId, producerEpoch, coordinatorEpoch, currentTxnFirstOffset,
                lastTimestamp, firstSequence, lastSequence, lastOffset, offsetDelta, timestamp);
    }

    @Override
    public String toString() {
        return "ProducerSnapshot{" +
                "producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", coordinatorEpoch=" + coordinatorEpoch +
                ", currentTxnFirstOffset=" + currentTxnFirstOffset +
                ", lastTimestamp=" + lastTimestamp +
                ", firstSequence=" + firstSequence +
                ", lastSequence=" + lastSequence +
                ", lastOffset=" + lastOffset +
                ", offsetDelta=" + offsetDelta +
                ", timestamp=" + timestamp +
                '}';
    }

    public void accept(Visitor visitor) {
        visitor.pidSnapshot(this);
    }
}
