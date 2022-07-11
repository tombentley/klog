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

import com.github.tombentley.klog.snapshot.model.ProducerState;
import com.github.tombentley.klog.snapshot.model.SnapshotVisitor;
import com.github.tombentley.klog.snapshot.reader.Snapshot;
import com.github.tombentley.klog.snapshot.reader.SnapshotDumpReader;
import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(
        name = "abort-cmd",
        description = "Print abort commands for a given producer ID and epoch."
)
public class AbortCmd implements Runnable {
    @Option(names = {"--pid"},
            description = "Select only records with the given producer id.",
            required = true)
    Integer pid;

    @Option(names = {"--producer-epoch"},
            description = "Select only records with the given producer epoch.",
            required = true)
    Integer producerEpoch;

    @CommandLine.Parameters(index = "0..*", arity = "1..",
            description = "Snapshot dumps produced by kafka-dump-logs.sh.")
    List<File> dumpFiles;

    @Override
    public void run() {
        SnapshotVisitor visitor = new OutputVisitor();
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
        @Override
        public void producerSnapshot(ProducerState msg) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string(
                    String.format("$KAFKA_HOME/bin/kafka-transactions.sh --bootstrap-server $BOOTSTRAP_URL abort --topic $TOPIC_NAME " +
                                    "--partition $PART_NUM --producer-id %d --producer-epoch %d --coordinator-epoch %d",
                            msg.producerId(), msg.producerEpoch(), msg.coordinatorEpoch())));
        }
    }
}
