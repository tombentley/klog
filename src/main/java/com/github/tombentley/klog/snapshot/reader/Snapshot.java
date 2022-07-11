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
package com.github.tombentley.klog.snapshot.reader;

import com.github.tombentley.klog.snapshot.model.ProducerState;
import java.util.stream.Stream;

public class Snapshot {
    public enum Type {
        /*
         * For each topic partition, the broker maintains an in-memory mapping from the PID to the epoch,
         * sequence number, the last offset successfully written to the log, and the coordinator epoch
         * from each transactional producer. This is periodically stored in a {@code .snapshot} file.
         * The broker keeps the two most recent snapshots. If there is no snapshot when the broker
         * restarts, it rebuilds the map by scanning the full log.
         */
        PRODUCER_SNAPSHOT(null);
        public final String topicName;
        Type(String topicName) {
            this.topicName = topicName;
        }
    }

    private final String dumpFileName;
    private final Type type;
    private final String topicName;
    private final Stream<ProducerState> states;

    public Snapshot(String dumpFileName,
                    Type type,
                    String topicName,
                    Stream<ProducerState> states) {
        this.dumpFileName = dumpFileName;
        this.type = type;
        this.topicName = topicName;
        this.states = states;
    }

    public String dumpFileName() {
        return dumpFileName;
    }

    public Type type() {
        return type;
    }

    public String topicName() {
        return topicName;
    }

    public Stream<ProducerState> states() {
        return states;
    }
}
