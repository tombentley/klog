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
import java.util.List;
import java.util.Objects;

/**
 * A batch of messages.
 * kafka-dump-log.sh output is a single line like:
 * <pre>
 * baseOffset: 2250123 lastOffset: 2250123 count: 1 baseSequence: 756 lastSequence: 756 producerId: 891414
 * producerEpoch: 14 partitionLeaderEpoch: 533 isTransactional: true isControl: false position: 32947275
 * CreateTime: 1631107105272 size: 1105 magic: 2 compresscodec: ZSTD crc: 1835494165 isvalid: true
 * </pre>
 */
public final class Batch implements Located {
    private final String filename;
    private final int line;
    private final long baseOffset;
    private final long lastOffset;
    private final int count;
    private final int baseSequence;
    private final int lastSequence;
    private final long producerId;
    private final short producerEpoch;
    private final int partitionLeaderEpoch;
    private final boolean isTransactional;
    private final boolean isControl;
    private final long position;
    private final long createTime;
    private final int size;
    private final byte magic;
    private final String compressCodec;
    private final int crc;
    private final boolean isValid;
    private final List<BaseMessage> messages;

    /**
     */
    public Batch(String filename,
                 int line,
                 long baseOffset,
                 long lastOffset,
                 int count,
                 int baseSequence,
                 int lastSequence,
                 long producerId,
                 short producerEpoch,
                 int partitionLeaderEpoch,
                 boolean isTransactional,
                 boolean isControl,
                 long position,
                 long createTime,
                 int size,
                 byte magic,
                 String compressCodec,
                 int crc,
                 boolean isValid,
                 List<BaseMessage> messages) {
        this.filename = filename;
        this.line = line;
        this.baseOffset = baseOffset;
        this.lastOffset = lastOffset;
        this.count = count;
        this.baseSequence = baseSequence;
        this.lastSequence = lastSequence;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.isTransactional = isTransactional;
        this.isControl = isControl;
        this.position = position;
        this.createTime = createTime;
        this.size = size;
        this.magic = magic;
        this.compressCodec = compressCodec;
        this.crc = crc;
        this.isValid = isValid;
        this.messages = messages;
    }

    @Override
    public String toString() {
        return "Batch(" +
                "baseOffset=" + baseOffset +
                ", lastOffset=" + lastOffset +
                ", count=" + count +
                ", baseSequence=" + baseSequence +
                ", lastSequence=" + lastSequence +
                ", producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", partitionLeaderEpoch=" + partitionLeaderEpoch +
                ", isTransactional=" + isTransactional +
                ", isControl=" + isControl +
                ", position=" + position +
                ", createTime=" + Instant.ofEpochMilli(createTime) +
                ", size=" + size +
                ", magic=" + magic +
                ", compressCodec='" + compressCodec + '\'' +
                ", crc=" + crc +
                ", isValid=" + isValid +
                ')';
    }

    public ProducerSession session() {
        return new ProducerSession(producerId(), producerEpoch());
    }

    public String filename() {
        return filename;
    }

    public int line() {
        return line;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public int count() {
        return count;
    }

    public int baseSequence() {
        return baseSequence;
    }

    public int lastSequence() {
        return lastSequence;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public int partitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    public boolean isControl() {
        return isControl;
    }

    public long position() {
        return position;
    }

    public long createTime() {
        return createTime;
    }

    public int size() {
        return size;
    }

    public byte magic() {
        return magic;
    }

    public String compressCodec() {
        return compressCodec;
    }

    public int crc() {
        return crc;
    }

    public boolean isValid() {
        return isValid;
    }

    public List<BaseMessage> messages() {
        return messages;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Batch) obj;
        return Objects.equals(this.filename, that.filename) &&
                this.line == that.line &&
                this.baseOffset == that.baseOffset &&
                this.lastOffset == that.lastOffset &&
                this.count == that.count &&
                this.baseSequence == that.baseSequence &&
                this.lastSequence == that.lastSequence &&
                this.producerId == that.producerId &&
                this.producerEpoch == that.producerEpoch &&
                this.partitionLeaderEpoch == that.partitionLeaderEpoch &&
                this.isTransactional == that.isTransactional &&
                this.isControl == that.isControl &&
                this.position == that.position &&
                this.createTime == that.createTime &&
                this.size == that.size &&
                this.magic == that.magic &&
                Objects.equals(this.compressCodec, that.compressCodec) &&
                this.crc == that.crc &&
                this.isValid == that.isValid &&
                Objects.equals(this.messages, that.messages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, line, baseOffset, lastOffset, count, baseSequence, lastSequence, producerId, producerEpoch, partitionLeaderEpoch, isTransactional, isControl, position, createTime, size, magic, compressCodec, crc, isValid, messages);
    }

    public void accept(Visitor visitor) {
        visitor.batch(this);
        for (var msg : messages()) {
            msg.accept(visitor);
        }
    }

}
