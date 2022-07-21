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

import com.github.tombentley.klog.common.Located;
import com.github.tombentley.klog.snapshot.model.ProducerState;
import com.github.tombentley.klog.snapshot.model.SnapshotVisitor;
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
        description = "Print producer snapshot dumps previously obtained using kafka-dump-logs.sh."
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

    @CommandLine.Parameters(index = "0..*", arity = "1..",
            description = "Snapshot dumps produced by kafka-dump-logs.sh.")
    List<File> dumpFiles;

    @Override
    public void run() {
        SnapshotVisitor visitor = new OutputVisitor(dumpFiles.size() > 1, lineNumbers);
        SnapshotDumpReader snapshotDumpReader = new SnapshotDumpReader();
        // Sort to get into offset order
        dumpFiles.stream().sorted(Comparator.comparing(File::getName)).forEach(dumpFile -> {
            Snapshot snapshot = snapshotDumpReader.readSnapshot(dumpFile);
            Predicate<ProducerState> predicate = SnapshotPredicate.predicate(snapshot.type(), pid, producerEpoch);
            Stream<ProducerState> locatedStream = snapshot.states();
            if (predicate != null) {
                locatedStream = locatedStream.filter(predicate);
            }
            locatedStream.forEach(batch -> {
                batch.accept(visitor);
            });
        });
    }

    private static class OutputVisitor implements SnapshotVisitor {
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
        public void producerSnapshot(ProducerState msg) {
            location(msg);
            System.out.println(CommandLine.Help.Ansi.AUTO.string(
                    String.format("ProducerState(producerId=@|blue,bold %d|@, producerEpoch=@|blue,bold %d|@, coordinatorEpoch=@|blue,bold %d|@, " +
                                    "currentTxnFirstOffset=%d%s, firstSequence=%d, lastSequence=%d, lastOffset=%d, offsetDelta=%d, timestamp=%s)",
                            msg.producerId(), msg.producerEpoch(), msg.coordinatorEpoch(), msg.currentTxnFirstOffset(),
                            ", lastTimestamp=" + Instant.ofEpochMilli(msg.lastTimestamp()), msg.firstSequence(), msg.lastSequence(),
                            msg.lastOffset(), msg.offsetDelta(), Instant.ofEpochMilli(msg.timestamp()))));
        }
    }
}
