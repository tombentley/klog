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

import com.github.tombentley.klog.common.Located;
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
import com.github.tombentley.klog.segment.model.SegmentVisitor;
import com.github.tombentley.klog.segment.reader.Segment;
import com.github.tombentley.klog.segment.reader.SegmentDumpReader;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@CommandLine.Command(
        name = "cat",
        description = "Print segment dumps previously produced by kafka-dump-logs.sh. " +
                      "This is slightly more useful that just looking at the dumps directly, because it converts" +
                      "the timestamps into something human readable."
)
public class Cat implements Runnable {
    @Option(names = {"--line-numbers", "-l"},
            description = "Include line numbers in the output",
            defaultValue = "false")
    Boolean lineNumbers;

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
            description = "Segment dumps produced by kafka-dump-logs.sh.")
    List<File> dumpFiles;

    @Override
    public void run() {
        SegmentVisitor visitor = new OutputVisitor(dumpFiles.size() > 1, lineNumbers);
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

    private static class OutputVisitor implements SegmentVisitor {
        private final boolean lineNumbers;
        private final boolean fileName;

        public OutputVisitor(boolean fileName, boolean lineNumbers) {
            this.fileName = fileName;
            this.lineNumbers = lineNumbers;
        }

        @Override
        public void batch(Batch batch) {
            location(batch);
            System.out.println(CommandLine.Help.Ansi.AUTO.string(
                    String.format("@|bold Batch(baseOffset=%d, lastOffset=%d, count=%d, baseSequence=%d, " +
                                  "lastSequence=%d, producerId=%d, producerEpoch=%s, partitionLeaderEpoch=%d, " +
                                  "isTransactional=%s, isControl=%s, position=%d, createTime=%s, size=%d, " +
                                  "magic=%s, compressCodec='%s', crc=%d, isValid=|@%s@|bold )|@",
                            batch.baseOffset(), batch.lastOffset(), batch.count(), batch.baseSequence(),
                            batch.lastSequence(), batch.producerId(), batch.producerEpoch(), batch.partitionLeaderEpoch(),
                            batch.isTransactional(), batch.isControl(), batch.position(), Instant.ofEpochMilli(batch.createTime()), batch.size(),
                            batch.magic(), batch.compressCodec(), batch.crc(), batch.isValid())));
        }

        private void location(Located batch) {
            if (fileName) {
                System.out.printf("%s:", batch.filename());
            }
            if (lineNumbers) {
                System.out.printf("%d: ", batch.line());
            }
        }

        @Override
        public void controlMessage(ControlMessage msg) {
            location(msg);
            System.out.println(CommandLine.Help.Ansi.AUTO.string(
                    String.format("  ControlMessage(offset=%d, createTime=%s, keySize=%d, valueSize=%d, sequence=%d, " +
                                  "headers='%s', commit=%s, coordinatorEpoch=%d)",
                            msg.offset(), Instant.ofEpochMilli(msg.createTime()), msg.keySize(), msg.valueSize(), msg.sequence(),
                            msg.headerKeys(), msg.commit() ? "@|green,bold commit|@" : "@|red,bold abort|@", msg.coordinatorEpoch())));
        }

        @Override
        public void stateChangeDeletion(TransactionStateDeletion msg) {
            location(msg);
            System.out.println(CommandLine.Help.Ansi.AUTO.string(
                    String.format("  TransactionStateDeletion(offset=%d, createTime=%s, keySize=%d, valueSize=%d, " +
                                  "sequence=%d, headerKeys='%s', transactionId='%s')",
                            msg.offset(), Instant.ofEpochMilli(msg.createTime()), msg.keySize(), msg.valueSize(),
                            msg.sequence(), msg.headerKeys(), msg.transactionId())));
        }

        @Override
        public void stateChange(TransactionStateChange msg) {
            location(msg);
            System.out.println(CommandLine.Help.Ansi.AUTO.string(
                    String.format("  TransactionStateMessage(offset=%d, createTime=%s, keySize=%d, valueSize=%d, " +
                                  "sequence=%d, headerKeys='%s', transactionId='%s', producerId=%d, producerEpoch=%s, " +
                                  "state=@|blue %s|@, partitions='%s', txnLastUpdateTimestamp=%s, txnTimeoutMs=%d)",
                            msg.offset(), Instant.ofEpochMilli(msg.createTime()), msg.keySize(), msg.valueSize(),
                            msg.sequence(), msg.headerKeys(), msg.transactionId(), msg.producerId(), msg.producerEpoch(),
                            msg.state(), msg.partitions(), Instant.ofEpochMilli(msg.txnLastUpdateTimestamp()), msg.txnTimeoutMs())));
        }

        @Override
        public void dataMessage(DataMessage msg) {
            location(msg);
            System.out.printf("  DataMessage(offset=%d, createTime=%s, keySize=%d, valueSize=%d, sequence=%d, headerKeys='%s')%n",
                    msg.offset(), Instant.ofEpochMilli(msg.createTime()), msg.keySize(), msg.valueSize(), msg.sequence(), msg.headerKeys());
        }
    }
}
