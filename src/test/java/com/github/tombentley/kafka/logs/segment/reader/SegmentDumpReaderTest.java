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
package com.github.tombentley.kafka.logs.segment.reader;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        SegmentInfo segmentInfo = new SegmentDumpReader()
                .readSegment("<test-input>", content.lines())
                .batches().collect(SegmentInfoCollector.collector());
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
        SegmentInfo segmentInfo = new SegmentDumpReader().readSegment("<test-input>", content.lines())
                .batches()
                .collect(SegmentInfoCollector.collector());
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
        SegmentInfo segmentInfo = new SegmentDumpReader().readSegment("<test-input>", content.lines())
                .batches()
                .collect(SegmentInfoCollector.collector());
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
    public void testWithDeepIterationWithControlRecords() {
        var content = """
                Dumping /tmp/kafka-0-logs/transactional-foo-0/00000000000000000000.log
                Starting offset: 0
                baseOffset: 0 lastOffset: 1 count: 2 baseSequence: 0 lastSequence: 1 producerId: 0 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: true isControl: false position: 0 CreateTime: 1632840910502 size: 95 magic: 2 compresscodec: none crc: 3463992817 isvalid: true
                | offset: 0 CreateTime: 1632840910484 keySize: -1 valueSize: 10 sequence: 0 headerKeys: []
                | offset: 1 CreateTime: 1632840910502 keySize: -1 valueSize: 10 sequence: 1 headerKeys: []
                baseOffset: 2 lastOffset: 2 count: 1 baseSequence: 2 lastSequence: 2 producerId: 0 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: true isControl: false position: 95 CreateTime: 1632840911002 size: 78 magic: 2 compresscodec: none crc: 3470306477 isvalid: true
                | offset: 2 CreateTime: 1632840911002 keySize: -1 valueSize: 10 sequence: 2 headerKeys: []
                baseOffset: 3 lastOffset: 3 count: 1 baseSequence: 3 lastSequence: 3 producerId: 0 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: true isControl: false position: 173 CreateTime: 1632840911503 size: 78 magic: 2 compresscodec: none crc: 244140094 isvalid: true
                | offset: 3 CreateTime: 1632840911503 keySize: -1 valueSize: 10 sequence: 3 headerKeys: []
                baseOffset: 4 lastOffset: 4 count: 1 baseSequence: -1 lastSequence: -1 producerId: 0 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: true isControl: true position: 251 CreateTime: 1632840911601 size: 78 magic: 2 compresscodec: none crc: 4234329125 isvalid: true
                | offset: 4 CreateTime: 1632840911601 keySize: 4 valueSize: 6 sequence: -1 headerKeys: [] endTxnMarker: COMMIT coordinatorEpoch: 4
                baseOffset: 5 lastOffset: 5 count: 1 baseSequence: 4 lastSequence: 4 producerId: 0 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: true isControl: false position: 329 CreateTime: 1632840912091 size: 78 magic: 2 compresscodec: none crc: 3445037521 isvalid: true
                | offset: 5 CreateTime: 1632840912091 keySize: -1 valueSize: 10 sequence: 4 headerKeys: []
                baseOffset: 6 lastOffset: 6 count: 1 baseSequence: -1 lastSequence: -1 producerId: 0 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: true isControl: true position: 407 CreateTime: 1632840912595 size: 78 magic: 2 compresscodec: none crc: 1079808135 isvalid: true
                | offset: 6 CreateTime: 1632840912595 keySize: 4 valueSize: 6 sequence: -1 headerKeys: [] endTxnMarker: COMMIT coordinatorEpoch: 4
                """;
        SegmentInfo segmentInfo = new SegmentDumpReader().readSegment("<test-input>", content.lines())
                .batches()
                .collect(SegmentInfoCollector.collector());
        assertEquals(0, segmentInfo.firstBatch().baseOffset());
        assertEquals(1, segmentInfo.firstBatch().lastOffset());
        assertTrue(segmentInfo.firstBatch().isTransactional());
        assertFalse(segmentInfo.firstBatch().isControl());
        assertEquals(6, segmentInfo.lastBatch().baseOffset());
        assertEquals(6, segmentInfo.lastBatch().lastOffset());
        assertTrue(segmentInfo.lastBatch().isTransactional());
        assertTrue(segmentInfo.lastBatch().isControl());
        assertEquals(0, segmentInfo.aborted());
        assertEquals(2, segmentInfo.committed());
        assertTrue(segmentInfo.emptyTransactions().isEmpty());
        assertTrue(segmentInfo.openTransactions().isEmpty());
        assertEquals(2, segmentInfo.txnSizeStats().getCount());
        assertEquals(2, segmentInfo.txnDurationStats().getCount());
    }

    /** --transaction-log-decoder on a segment from __transaction_state */
    @Test
    public void testWithTransactionLogDecoder() {
        var content = """
                Dumping /tmp/kafka-0-logs/__transaction_state-4/00000000000000000000.log
                Starting offset: 0
                baseOffset: 0 lastOffset: 0 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 0 CreateTime: 1632840910297 size: 120 magic: 2 compresscodec: none crc: 2207277534 isvalid: true
                | offset: 0 CreateTime: 1632840910297 keySize: 15 valueSize: 37 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=Empty,partitions=[],txnLastUpdateTimestamp=1632840910282,txnTimeoutMs=60000
                baseOffset: 1 lastOffset: 1 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 120 CreateTime: 1632840910511 size: 149 magic: 2 compresscodec: none crc: 2028590545 isvalid: true
                | offset: 1 CreateTime: 1632840910511 keySize: 15 valueSize: 64 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=Ongoing,partitions=[transactional-foo-0],txnLastUpdateTimestamp=1632840910510,txnTimeoutMs=60000
                baseOffset: 2 lastOffset: 2 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 269 CreateTime: 1632840911586 size: 149 magic: 2 compresscodec: none crc: 3719422551 isvalid: true
                | offset: 2 CreateTime: 1632840911586 keySize: 15 valueSize: 64 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=PrepareCommit,partitions=[transactional-foo-0],txnLastUpdateTimestamp=1632840911585,txnTimeoutMs=60000
                baseOffset: 3 lastOffset: 3 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 418 CreateTime: 1632840911620 size: 120 magic: 2 compresscodec: none crc: 3726340669 isvalid: true
                | offset: 3 CreateTime: 1632840911620 keySize: 15 valueSize: 37 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=CompleteCommit,partitions=[],txnLastUpdateTimestamp=1632840911588,txnTimeoutMs=60000
                baseOffset: 4 lastOffset: 4 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 538 CreateTime: 1632840912092 size: 149 magic: 2 compresscodec: none crc: 3298507796 isvalid: true
                | offset: 4 CreateTime: 1632840912092 keySize: 15 valueSize: 64 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=Ongoing,partitions=[transactional-foo-0],txnLastUpdateTimestamp=1632840912092,txnTimeoutMs=60000
                baseOffset: 5 lastOffset: 5 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 687 CreateTime: 1632840912592 size: 149 magic: 2 compresscodec: none crc: 764186261 isvalid: true
                | offset: 5 CreateTime: 1632840912592 keySize: 15 valueSize: 64 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=PrepareCommit,partitions=[transactional-foo-0],txnLastUpdateTimestamp=1632840912592,txnTimeoutMs=60000
                baseOffset: 6 lastOffset: 6 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 836 CreateTime: 1632840912607 size: 120 magic: 2 compresscodec: none crc: 1098902730 isvalid: true
                | offset: 6 CreateTime: 1632840912607 keySize: 15 valueSize: 37 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=CompleteCommit,partitions=[],txnLastUpdateTimestamp=1632840912593,txnTimeoutMs=60000
                """;
        SegmentInfo segmentInfo = new SegmentDumpReader()
                .readSegment("<test-input>", content.lines())
                .batches()
                .collect(SegmentInfoCollector.collector());
        assertEquals(0, segmentInfo.firstBatch().baseOffset());
        assertEquals(0, segmentInfo.firstBatch().lastOffset());
        assertFalse(segmentInfo.firstBatch().isTransactional());
        assertEquals(6, segmentInfo.lastBatch().baseOffset());
        assertEquals(6, segmentInfo.lastBatch().lastOffset());
        assertFalse(segmentInfo.lastBatch().isTransactional());
        assertEquals(0, segmentInfo.aborted());
        assertEquals(0, segmentInfo.committed());
        assertTrue(segmentInfo.emptyTransactions().isEmpty());
        assertTrue(segmentInfo.openTransactions().isEmpty());
        assertEquals(0, segmentInfo.txnSizeStats().getCount());
        assertEquals(0, segmentInfo.txnDurationStats().getCount());
    }

    // TODO simulate hanging transaction
    // TODO test for the txn state machine

}