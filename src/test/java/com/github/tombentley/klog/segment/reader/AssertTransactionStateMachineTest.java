package com.github.tombentley.klog.segment.reader;

import java.util.List;
import java.util.stream.Collectors;

import com.github.tombentley.klog.segment.model.Batch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
class AssertTransactionStateMachineTest {

    /** --transaction-log-decoder on a segment from __transaction_state */
    @Test
    public void testInvalidStateTransition() {
        var content = "Dumping /tmp/kafka-0-logs/__transaction_state-4/00000000000000000000.log\n" +
                      "Starting offset: 0\n" +
                      "baseOffset: 0 lastOffset: 0 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 0 CreateTime: 1632840910297 size: 120 magic: 2 compresscodec: none crc: 2207277534 isvalid: true\n" +
                      "| offset: 0 CreateTime: 1632840910297 keySize: 15 valueSize: 37 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=Empty,partitions=[],txnLastUpdateTimestamp=1632840910282,txnTimeoutMs=60000\n" +
                      "baseOffset: 1 lastOffset: 1 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 120 CreateTime: 1632840910511 size: 149 magic: 2 compresscodec: none crc: 2028590545 isvalid: true\n" +
                      "| offset: 1 CreateTime: 1632840910511 keySize: 15 valueSize: 64 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=Ongoing,partitions=[transactional-foo-0],txnLastUpdateTimestamp=1632840910510,txnTimeoutMs=60000\n" +
                      "baseOffset: 2 lastOffset: 2 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 269 CreateTime: 1632840911586 size: 149 magic: 2 compresscodec: none crc: 3719422551 isvalid: true\n" +
                      "| offset: 2 CreateTime: 1632840911586 keySize: 15 valueSize: 64 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=PrepareCommit,partitions=[transactional-foo-0],txnLastUpdateTimestamp=1632840911585,txnTimeoutMs=60000\n" +
                      "baseOffset: 3 lastOffset: 3 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 4 isTransactional: false isControl: false position: 418 CreateTime: 1632840911620 size: 120 magic: 2 compresscodec: none crc: 3726340669 isvalid: true\n" +
                      "| offset: 3 CreateTime: 1632840911620 keySize: 15 valueSize: 37 sequence: -1 headerKeys: [] key: transaction_metadata::transactionalId=my-txnal-id payload: producerId:0,producerEpoch:0,state=CompleteAbort,partitions=[],txnLastUpdateTimestamp=1632840911588,txnTimeoutMs=60000\n";
        var e = assertThrows(IllegalStateException.class, () -> new SegmentDumpReader()
                    .readSegment("<test-input>", content.lines())
                    .batches()
                    .collect(Collectors.toList()));
        assertEquals("<test-input>:10: Illegal state change from PrepareCommit to CompleteAbort", e.getMessage());
    }
}