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
package com.github.tombentley.kafka.logs.segment.cli;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.github.tombentley.kafka.logs.segment.model.Batch;
import com.github.tombentley.kafka.logs.segment.reader.SegmentDumpReader;
import com.github.tombentley.kafka.logs.segment.reader.SegmentInfoCollector;
import picocli.CommandLine;

@CommandLine.Command(
        name = "stat",
        description = "Get statistics from some segment dumps"
)
public class Stat implements Runnable {
    @CommandLine.Option(names = {"--pid"},
            description = "Select only records with the given producer id")
    Integer pid;

    @CommandLine.Option(names = {"--producer-epoch"},
            description = "Select only records with the given producer epoch")
    Integer producerEpoch;

    @CommandLine.Option(names = {"--leader-epoch"},
            description = "Select only records appended with the given leader epoch")
    Integer leaderEpoch;

    @CommandLine.Parameters(index = "0..*", arity = "1..",
            description = "Segment dumps produced by kafka-dump-logs.sh")
    List<File> dumpFiles;

    @Override
    public void run() {
        Predicate<Batch> predicate = BatchPredicate.predicate(pid, producerEpoch, leaderEpoch);

        SegmentDumpReader segmentDumpReader = new SegmentDumpReader();
        // Sort to get into offset order
        dumpFiles.stream().sorted(Comparator.comparing(File::getName)).map(dumpFile -> {
            Stream<Batch> locatedStream = segmentDumpReader.readSegment(dumpFile).batches();
            if (predicate != null) {
                locatedStream = locatedStream.filter(predicate);
            }
            return locatedStream.collect(SegmentInfoCollector.collector());
        }).forEach(batch -> {
            System.out.println("First batch: " + batch.firstBatch());
            batch.emptyTransactions().forEach(txn -> System.out.println("Empty txn: " + txn));
            System.out.println("Last batch: " + batch.lastBatch());
            batch.openTransactions().forEach((sess, txn) -> System.out.println("Open transaction: " + sess + "->" + txn));
            System.out.println("#committed: " + batch.committed());
            System.out.println("#aborted: " + batch.aborted());
            System.out.println("Txn sizes: " + batch.txnSizeStats());
            System.out.println("Txn durations(ms): " + batch.txnDurationStats());
        });
    }

}
