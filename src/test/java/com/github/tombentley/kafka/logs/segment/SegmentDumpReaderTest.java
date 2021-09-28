package com.github.tombentley.kafka.logs.segment;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
class SegmentDumpReaderTest {

    /** Without --deep-iteration */
    @Test
    public void testWithoutDeepIteration() {
        var content = """
                Dumping ./00000000000000000000.log
                Starting offset: 0
                baseOffset: 0 lastOffset: 1 count: 2 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 0 CreateTime: 1632815304456 size: 88 magic: 2 compresscodec: none crc: 873053997 isvalid: true
                baseOffset: 2 lastOffset: 2 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 88 CreateTime: 1632815305550 size: 75 magic: 2 compresscodec: none crc: 945198711 isvalid: true
                baseOffset: 3 lastOffset: 3 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 163 CreateTime: 1632815307188 size: 79 magic: 2 compresscodec: none crc: 757930674 isvalid: true""";
        SegmentInfo segmentInfo = new SegmentDumpReader().readSegment("<test-input>", content.lines());
        assertEquals(0, segmentInfo.firstBatch().baseOffset());
        assertEquals(1, segmentInfo.firstBatch().lastOffset());
        assertFalse(segmentInfo.firstBatch().isTransactional());
        assertEquals(3, segmentInfo.lastBatch().baseOffset());
        assertEquals(3, segmentInfo.lastBatch().lastOffset());
        assertFalse(segmentInfo.lastBatch().isTransactional());
        assertEquals(0, segmentInfo.aborted());
        assertEquals(0, segmentInfo.committed());
        assertTrue(segmentInfo.emptyTransactions().isEmpty());
        assertTrue(segmentInfo.openTransactions().isEmpty());
        assertEquals(0, segmentInfo.txnSizeStats().getCount());
        assertEquals(0, segmentInfo.txnDurationStats().getCount());
    }

    /** --deep-iteration */
    @Test
    public void testWithDeepIteration() {
        var content = """
               Dumping /tmp/kafka-logs/foo-0/00000000000000000000.log
               Starting offset: 0
               baseOffset: 0 lastOffset: 1 count: 2 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 0 CreateTime: 1632815304456 size: 88 magic: 2 compresscodec: none crc: 873053997 isvalid: true
               | offset: 0 CreateTime: 1632815303637 keySize: -1 valueSize: 7 sequence: -1 headerKeys: []
               | offset: 1 CreateTime: 1632815304456 keySize: -1 valueSize: 5 sequence: -1 headerKeys: []
               baseOffset: 2 lastOffset: 2 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 88 CreateTime: 1632815305550 size: 75 magic: 2 compresscodec: none crc: 945198711 isvalid: true
               | offset: 2 CreateTime: 1632815305550 keySize: -1 valueSize: 7 sequence: -1 headerKeys: []
               baseOffset: 3 lastOffset: 3 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 163 CreateTime: 1632815307188 size: 79 magic: 2 compresscodec: none crc: 757930674 isvalid: true
               | offset: 3 CreateTime: 1632815307188 keySize: -1 valueSize: 11 sequence: -1 headerKeys: []
               """;
        SegmentInfo segmentInfo = new SegmentDumpReader().readSegment("<test-input>", content.lines());
        assertEquals(0, segmentInfo.firstBatch().baseOffset());
        assertEquals(1, segmentInfo.firstBatch().lastOffset());
        assertFalse(segmentInfo.firstBatch().isTransactional());
        assertEquals(3, segmentInfo.lastBatch().baseOffset());
        assertEquals(3, segmentInfo.lastBatch().lastOffset());
        assertFalse(segmentInfo.lastBatch().isTransactional());
        assertEquals(0, segmentInfo.aborted());
        assertEquals(0, segmentInfo.committed());
        assertTrue(segmentInfo.emptyTransactions().isEmpty());
        assertTrue(segmentInfo.openTransactions().isEmpty());
        assertEquals(0, segmentInfo.txnSizeStats().getCount());
        assertEquals(0, segmentInfo.txnDurationStats().getCount());
    }

    /** --deep-iteration --print-data-log */
    @Test
    public void testWithDeepIterationAndPayload() {
        var content = """
                Dumping /tmp/kafka-logs/foo-0/00000000000000000000.log
                Starting offset: 0
                baseOffset: 0 lastOffset: 1 count: 2 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 0 CreateTime: 1632815304456 size: 88 magic: 2 compresscodec: none crc: 873053997 isvalid: true
                | offset: 0 CreateTime: 1632815303637 keySize: -1 valueSize: 7 sequence: -1 headerKeys: [] payload: drfverv
                | offset: 1 CreateTime: 1632815304456 keySize: -1 valueSize: 5 sequence: -1 headerKeys: [] payload: rberb
                baseOffset: 2 lastOffset: 2 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 88 CreateTime: 1632815305550 size: 75 magic: 2 compresscodec: none crc: 945198711 isvalid: true
                | offset: 2 CreateTime: 1632815305550 keySize: -1 valueSize: 7 sequence: -1 headerKeys: [] payload: trnnrtn
                baseOffset: 3 lastOffset: 3 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 163 CreateTime: 1632815307188 size: 79 magic: 2 compresscodec: none crc: 757930674 isvalid: true
                | offset: 3 CreateTime: 1632815307188 keySize: -1 valueSize: 11 sequence: -1 headerKeys: [] payload: 65u5k6uk,yj
                """;
        SegmentInfo segmentInfo = new SegmentDumpReader().readSegment("<test-input>", content.lines());
        assertEquals(0, segmentInfo.firstBatch().baseOffset());
        assertEquals(1, segmentInfo.firstBatch().lastOffset());
        assertFalse(segmentInfo.firstBatch().isTransactional());
        assertEquals(3, segmentInfo.lastBatch().baseOffset());
        assertEquals(3, segmentInfo.lastBatch().lastOffset());
        assertFalse(segmentInfo.lastBatch().isTransactional());
        assertEquals(0, segmentInfo.aborted());
        assertEquals(0, segmentInfo.committed());
        assertTrue(segmentInfo.emptyTransactions().isEmpty());
        assertTrue(segmentInfo.openTransactions().isEmpty());
        assertEquals(0, segmentInfo.txnSizeStats().getCount());
        assertEquals(0, segmentInfo.txnDurationStats().getCount());
    }

}