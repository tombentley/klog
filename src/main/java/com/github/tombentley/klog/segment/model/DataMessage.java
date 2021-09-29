package com.github.tombentley.klog.segment.model;/*
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

import java.time.Instant;
import java.util.Objects;

/**
 * A regular data record (in a batch)
 * kafka-dump-log.sh output is a single line like:
 * <pre>
 * | offset: 2250123 CreateTime: 1631107105272 keysize: 52 valuesize: 2470 sequence: 756 headerKeys: [SOME_HEADER,SOME_OTHER_HEADER]
 * </pre>
 */
public final class DataMessage implements BaseMessage {
    private final String filename;
    private final int line;
    private final long offset;
    private final long createTime;
    private final int keySize;
    private final int valueSize;
    private final int sequence;
    private final String headerKeys;

    /**
     */
    public DataMessage(String filename,
                       int line,
                       long offset,
                       long createTime,
                       int keySize,
                       int valueSize,
                       int sequence,
                       String headerKeys) {
        this.filename = filename;
        this.line = line;
        this.offset = offset;
        this.createTime = createTime;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.sequence = sequence;
        this.headerKeys = headerKeys;
    }

    @Override
    public String toString() {
        return "DataMessage(" +
                "offset=" + offset +
                ", createTime=" + Instant.ofEpochMilli(createTime) +
                ", keySize=" + keySize +
                ", valueSize=" + valueSize +
                ", sequence=" + sequence +
                ", headerKeys='" + headerKeys + '\'' +
                ')';
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

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (DataMessage) obj;
        return Objects.equals(this.filename, that.filename) &&
                this.line == that.line &&
                this.offset == that.offset &&
                this.createTime == that.createTime &&
                this.keySize == that.keySize &&
                this.valueSize == that.valueSize &&
                this.sequence == that.sequence &&
                Objects.equals(this.headerKeys, that.headerKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, line, offset, createTime, keySize, valueSize, sequence, headerKeys);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.dataMessage(this);
    }

}
