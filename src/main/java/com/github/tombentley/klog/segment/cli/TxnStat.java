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
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

import com.github.tombentley.klog.segment.model.Batch;
import com.github.tombentley.klog.segment.reader.Segment;
import com.github.tombentley.klog.segment.reader.SegmentDumpReader;
import com.github.tombentley.klog.segment.reader.SegmentInfoCollector;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "txn-stat",
        description = "Get statistics about transactions from some segment dumps."
)
public class TxnStat implements Runnable {
    @Option(names = {"--pid"},
            description = "Select only records with the given producer id.")
    Integer pid;

    @Option(names = {"--producer-epoch"},
            description = "Select only records with the given producer epoch.")
    Integer producerEpoch;

    @Option(names = {"--leader-epoch"},
            description = "Select only records appended with the given leader epoch.")
    Integer leaderEpoch;

    @Parameters(index = "0..*", arity = "1..",
            description = "Segment dumps produced by `kafka-dump-logs.sh`.")
    List<File> dumpFiles;

    @Override
    public void run() {

        SegmentDumpReader segmentDumpReader = new SegmentDumpReader();
        // Sort to get into offset order
        var segmentInfo = dumpFiles.stream().sorted(Comparator.comparing(File::getName))
                .map(dumpFile -> {
                    Segment segment = segmentDumpReader.readSegment(dumpFile);
                    if (segment.type() != Segment.Type.DATA) {
                        throw new RuntimeException(segment.type().topicName + " partitions do not contain transactional messages");
                    }
                    // TODO assert that they're all the same topic
                    // TODO assert that they're in the right order
                    return segment;
                })
                .flatMap(segment -> {
                    Predicate<Batch> predicate = BatchPredicate.predicate(segment.type(), pid, producerEpoch, leaderEpoch, null);
                    var locatedStream = segment.batches();
                    if (predicate != null) {
                        locatedStream = locatedStream.filter(predicate);
                    }
                    return locatedStream;
                })
                .collect(SegmentInfoCollector.collector());

        System.out.println("num_committed: " + segmentInfo.numTransactionalCommit());
        System.out.println("num_aborted: " + segmentInfo.numTransactionalAbort());
        System.out.println("txn_size_stats: " + segmentInfo.txnSizeStats());
        System.out.println("txn_duration_stats_ms: " + segmentInfo.txnDurationStats());
        segmentInfo.emptyTransactions().forEach(txn -> System.out.println("empty_txn: " + txn));
        segmentInfo.openTransactions().forEach((sess, txn) -> System.out.println("open_txn: " + sess + "->" + txn));
    }

}
