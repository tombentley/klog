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
import java.util.List;

/**
 * A batch of messages.
 * kafka-dump-log.sh output is a single line like:
 * <pre>
 * baseOffset: 2250123 lastOffset: 2250123 count: 1 baseSequence: 756 lastSequence: 756 producerId: 891414
 * producerEpoch: 14 partitionLeaderEpoch: 533 isTransactional: true isControl: false position: 32947275
 * CreateTime: 1631107105272 size: 1105 magic: 2 compresscodec: ZSTD crc: 1835494165 isvalid: true
 * </pre>
 */
public record Batch(String filename,
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
                    List<BaseMessage> messages) implements Located {

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
}
