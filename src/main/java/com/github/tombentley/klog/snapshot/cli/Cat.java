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
package com.github.tombentley.klog.snapshot.cli;

import com.github.tombentley.klog.snapshot.model.Located;
import com.github.tombentley.klog.snapshot.model.ProducerState;
import com.github.tombentley.klog.snapshot.model.Visitor;
import com.github.tombentley.klog.snapshot.reader.Snapshot;
import com.github.tombentley.klog.snapshot.reader.SnapshotDumpReader;
import java.io.File;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(
        name = "cat",
        description = "Print snapshot dumps previously produced by kafka-dump-logs.sh." +
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

    @Option(names = {"--abort-command", "-a"},
            description = "Print abort commands instead of producer state",
            defaultValue = "false")
    static Boolean abortCommand;

    @CommandLine.Parameters(index = "0..*", arity = "1..",
            description = "Snapshot dumps produced by `kafka-dump-logs.sh`.")
    List<File> dumpFiles;

    @Override
    public void run() {
        Visitor visitor = new OutputVisitor(dumpFiles.size() > 1, lineNumbers);
        SnapshotDumpReader snapshotDumpReader = new SnapshotDumpReader();
        // Sort to get into offset order
        dumpFiles.stream().sorted(Comparator.comparing(File::getName)).forEach(dumpFile -> {
            Snapshot snaphost = snapshotDumpReader.readSnapshot(dumpFile);
            Predicate<ProducerState> predicate = SnapshotPredicate.predicate(snaphost.type(), pid, producerEpoch);
            Stream<ProducerState> locatedStream = snaphost.states();
            if (predicate != null) {
                locatedStream = locatedStream.filter(predicate);
            }
            locatedStream.forEach(batch -> {
                batch.accept(visitor);
            });
        });
    }

    private static class OutputVisitor implements Visitor {
        private final boolean lineNumbers;
        private final boolean fileName;

        public OutputVisitor(boolean fileName, boolean lineNumbers) {
            this.fileName = fileName;
            this.lineNumbers = lineNumbers;
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
        public void pidSnapshot(ProducerState msg) {
            location(msg);
            if (abortCommand) {
                System.out.println(CommandLine.Help.Ansi.AUTO.string(
                        String.format("$KAFKA_HOME/bin/kafka-transactions.sh --bootstrap-server $BOOTSTRAP_URL abort --topic $TOPIC_NAME " +
                                        "--partition $PART_NUM --producer-id %d --producer-epoch %d --coordinator-epoch %d",
                                msg.producerId(), msg.producerEpoch(), msg.coordinatorEpoch())));
            } else {
                System.out.println(CommandLine.Help.Ansi.AUTO.string(
                        String.format("ProducerSnapshot(producerId=@|blue,bold %d|@, producerEpoch=@|blue,bold %d|@, coordinatorEpoch=@|blue,bold %d|@, " +
                                        "currentTxnFirstOffset=%d%s, firstSequence=%d, lastSequence=%d, lastOffset=%d, offsetDelta=%d, timestamp=%s)",
                                msg.producerId(), msg.producerEpoch(), msg.coordinatorEpoch(), msg.currentTxnFirstOffset(),
                                ", lastTimestamp=" + Instant.ofEpochMilli(msg.lastTimestamp()), msg.firstSequence(), msg.lastSequence(),
                                msg.lastOffset(), msg.offsetDelta(), Instant.ofEpochMilli(msg.timestamp()))));
            }
        }
    }
}
