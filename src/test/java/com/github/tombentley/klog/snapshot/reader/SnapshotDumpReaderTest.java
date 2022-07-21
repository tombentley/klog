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
import java.util.List;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class SnapshotDumpReaderTest {
    @Test
    public void readSnapshotFromKafka2Dump() {
        var content = "Dumping 00000000000933607637.snapshot\n" +
                      "producerId: 171101 producerEpoch: 12 coordinatorEpoch: 54 currentTxnFirstOffset: None firstSequence: 0 lastSequence: 0 lastOffset: 932442537 offsetDelta: 0 timestamp: 1655628030307\n" +
                      "producerId: 199398 producerEpoch: 0 coordinatorEpoch: 57 currentTxnFirstOffset: Some(933607621) firstSequence: 0 lastSequence: 0 lastOffset: 933607621 offsetDelta: 0 timestamp: 1655761267833\n" +
                      "producerId: 173102 producerEpoch: 16 coordinatorEpoch: 59 currentTxnFirstOffset: None firstSequence: 0 lastSequence: 0 lastOffset: 933203854 offsetDelta: 0 timestamp: 1655704254875\n";
        Snapshot snapshot = new SnapshotDumpReader()
                .readSnapshot("<test-input>", content.lines());
        assertEquals(null, snapshot.topicName());
        assertEquals("<test-input>", snapshot.dumpFileName());
        assertEquals(Snapshot.Type.PRODUCER, snapshot.type());
        List<ProducerState> states = snapshot.states().collect(Collectors.toList());
        assertEquals(3, states.size());
        assertEquals(57, states.stream()
                .filter(s -> s.producerId() == 199398).findFirst().get().coordinatorEpoch());
    }

    @Test
    public void readSnapshotFromKafka3Dump() {
        var content = "Dumping 00000000000933607637.snapshot\n" +
                "producerId: 171101 producerEpoch: 12 coordinatorEpoch: 54 currentTxnFirstOffset: None lastTimestamp: 1655628030307 firstSequence: 0 lastSequence: 0 lastOffset: 932442537 offsetDelta: 0 timestamp: 1655628030307\n" +
                "producerId: 199398 producerEpoch: 0 coordinatorEpoch: 57 currentTxnFirstOffset: Some(933607621) lastTimestamp: 1655761267833 firstSequence: 0 lastSequence: 0 lastOffset: 933607621 offsetDelta: 0 timestamp: 1655761267833\n" +
                "producerId: 173102 producerEpoch: 16 coordinatorEpoch: 59 currentTxnFirstOffset: None lastTimestamp: 1655704254875 firstSequence: 0 lastSequence: 0 lastOffset: 933203854 offsetDelta: 0 timestamp: 1655704254875\n";
        Snapshot snapshot = new SnapshotDumpReader()
                .readSnapshot("<test-input>", content.lines());
        assertEquals(null, snapshot.topicName());
        assertEquals("<test-input>", snapshot.dumpFileName());
        assertEquals(Snapshot.Type.PRODUCER, snapshot.type());
        List<ProducerState> states = snapshot.states().collect(Collectors.toList());
        assertEquals(3, states.size());
        assertEquals(57, states.stream()
                .filter(s -> s.producerId() == 199398).findFirst().get().coordinatorEpoch());
    }
}
