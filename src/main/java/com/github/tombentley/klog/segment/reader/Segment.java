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

import java.util.stream.Stream;

import com.github.tombentley.klog.segment.model.Batch;

public class Segment {

    ;

    public static enum Type {
        DATA(null),
        TRANSACTION_STATE("__transaction_state"),
        CONSUMER_OFFSETS("__consumer_offsets"); // TODO support this
        public final String topicName;
        Type(String topicName) {
            this.topicName = topicName;
        }
    }

    private final String dumpFileName;
    private final Type type;
    private final String topicName;
    private final boolean deepIteration;
    private final Stream<Batch> batches;

    public Segment(String dumpFileName, Type type, String topicName, boolean deepIteration, Stream<Batch> batches) {
        this.dumpFileName = dumpFileName;
        this.type = type;
        this.topicName = topicName;
        this.deepIteration = deepIteration;
        this.batches = batches;
    }

    /**
     * The name of the dumped file
     */
    public String dumpFileName() {
        return dumpFileName;
    }

    public Type type() {
        return type;
    }

    public String topicName() {
        return topicName;
    }

    /**
     * Whether the dump has message-level info (was dumped using --deep-iteration).
     */
    public boolean deepIteration() {
        return deepIteration;
    }

    /**
     * The batches in the dump.
     */
    public Stream<Batch> batches() {
        return batches;
    }

}
