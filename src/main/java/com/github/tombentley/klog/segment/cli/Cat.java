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
package com.github.tombentley.klog.segment.cli;

import java.io.File;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.github.tombentley.klog.segment.model.Batch;
import com.github.tombentley.klog.segment.model.ControlMessage;
import com.github.tombentley.klog.segment.model.DataMessage;
import com.github.tombentley.klog.segment.model.TransactionStateChange;
import com.github.tombentley.klog.segment.model.TransactionStateDeletion;
import com.github.tombentley.klog.segment.model.Visitor;
import com.github.tombentley.klog.segment.reader.Segment;
import com.github.tombentley.klog.segment.reader.SegmentDumpReader;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@CommandLine.Command(
        name = "cat",
        description = "Print segment dumps previously produced by kafka-dump-logs.sh." +
                      "This is slightly more useful that just looking at the dumps directly, because it converts" +
                      "the timestamps into something human readable."
)
public class Cat implements Runnable {
    @Option(names = {"--pid"},
            description = "Select only records with the given producer id.")
    Integer pid;

    @Option(names = {"--producer-epoch"},
            description = "Select only records with the given producer epoch.")
    Integer producerEpoch;

    @Option(names = {"--leader-epoch"},
            description = "Select only records appended with the given leader epoch.")
    Integer leaderEpoch;

    @Option(names = {"--transactional-id"},
            description = "Select only records (of some __transaction_state dump) which apply to the given transactional id.")
    String transactionalId;

    @Parameters(index = "0..*", arity = "1..",
            description = "Segment dumps produced by `kafka-dump-logs.sh`.")
    List<File> dumpFiles;

    @Override
    public void run() {
        Visitor visitor = new OutputVisitor();

        SegmentDumpReader segmentDumpReader = new SegmentDumpReader();
        // Sort to get into offset order
        dumpFiles.stream().sorted(Comparator.comparing(File::getName)).forEach(dumpFile -> {
            Segment segment = segmentDumpReader.readSegment(dumpFile);
            if (transactionalId != null && segment.type() != Segment.Type.TRANSACTION_STATE) {
                throw new RuntimeException("--transactional-id can only be used on partitions of " + Segment.Type.TRANSACTION_STATE.topicName);
            }
            Predicate<Batch> predicate = BatchPredicate.predicate(segment.type(), pid, producerEpoch, leaderEpoch, transactionalId);
            Stream<Batch> locatedStream = segment.batches();
            if (predicate != null) {
                locatedStream = locatedStream.filter(predicate);
            }
            locatedStream.forEach(batch -> {
                batch.accept(visitor);
            });
        });
    }

    private static class OutputVisitor implements Visitor {
        @Override
        public void batch(Batch batch) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string(
                    "@|bold Batch(" +
                    "baseOffset=" + batch.baseOffset() +
                    ", lastOffset=" + batch.lastOffset() +
                    ", count=" + batch.count() +
                    ", baseSequence=" + batch.baseSequence() +
                    ", lastSequence=" + batch.lastSequence() +
                    ", producerId=" + batch.producerId() +
                    ", producerEpoch=" + batch.producerEpoch() +
                    ", partitionLeaderEpoch=" + batch.partitionLeaderEpoch() +
                    ", isTransactional=" + batch.isTransactional() +
                    ", isControl=" + batch.isControl() +
                    ", position=" + batch.position() +
                    ", createTime=" + Instant.ofEpochMilli(batch.createTime()) +
                    ", size=" + batch.size() +
                    ", magic=" + batch.magic() +
                    ", compressCodec='" + batch.compressCodec() + '\'' +
                    ", crc=" + batch.crc() +
                    ", isValid=|@" + (batch.isValid() ? "@|green,bold false|@" : "@|red,bold false|@") + "@|bold " +
                    ")|@"));
        }

        @Override
        public void controlMessage(ControlMessage msg) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("  ControlMessage(" +
                               "offset=" + msg.offset() +
                               ", createTime=" + Instant.ofEpochMilli(msg.createTime()) +
                               ", keySize=" + msg.keySize() +
                               ", valueSize=" + msg.valueSize() +
                               ", sequence=" + msg.sequence() +
                               ", headers='" + msg.headerKeys() + '\'' +
                               ", commit=" + (msg.commit() ? "@|green,bold commit|@" : "@|red,bold abort|@") +
                               ", coordinatorEpoch=" + msg.coordinatorEpoch() +
                               ')'));
        }

        @Override
        public void stateChangeDeletion(TransactionStateDeletion msg) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("  TransactionStateDeletion(" +
                               "offset=" + msg.offset() +
                               ", createTime=" + Instant.ofEpochMilli(msg.createTime()) +
                               ", keySize=" + msg.keySize() +
                               ", valueSize=" + msg.valueSize() +
                               ", sequence=" + msg.sequence() +
                               ", headerKeys='" + msg.headerKeys() + '\'' +
                               ", transactionId='" + msg.transactionId() + '\'' +
                               ')'));
        }

        @Override
        public void stateChange(TransactionStateChange msg) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("  TransactionStateMessage(" +
                               "offset=" + msg.offset() +
                               ", createTime=" + Instant.ofEpochMilli(msg.createTime()) +
                               ", keySize=" + msg.keySize() +
                               ", valueSize=" + msg.valueSize() +
                               ", sequence=" + msg.sequence() +
                               ", headerKeys='" + msg.headerKeys() + '\'' +
                               ", transactionId='" + msg.transactionId() + '\'' +
                               ", producerId=" + msg.producerId() +
                               ", producerEpoch=" + msg.producerEpoch() +
                               ", state=@|blue " + msg.state() + "|@" +
                               ", partitions='" + msg.partitions() + '\'' +
                               ", txnLastUpdateTimestamp=" + Instant.ofEpochMilli(msg.txnLastUpdateTimestamp()) +
                               ", txnTimeoutMs=" + msg.txnTimeoutMs() +
                               ')'));
        }

        @Override
        public void dataMessage(DataMessage msg) {
            System.out.println("  DataMessage(" +
                               "offset=" + msg.offset() +
                               ", createTime=" + Instant.ofEpochMilli(msg.createTime()) +
                               ", keySize=" + msg.keySize() +
                               ", valueSize=" + msg.valueSize() +
                               ", sequence=" + msg.sequence() +
                               ", headerKeys='" + msg.headerKeys() + '\'' +
                               ')');
        }
    }
}
