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
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.github.tombentley.klog.segment.model.Batch;
import com.github.tombentley.klog.segment.model.ProducerSession;
import com.github.tombentley.klog.segment.reader.EmptyTransaction;
import com.github.tombentley.klog.segment.reader.FirstBatchInTxn;
import com.github.tombentley.klog.segment.reader.Segment;
import com.github.tombentley.klog.segment.reader.SegmentDumpReader;
import com.github.tombentley.klog.segment.reader.TransactionalInfoCollector;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "txn-stat",
        description = "Get statistics about transactions from some segment dumps."
)
public class TxnStat implements Runnable {
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

    @Parameters(index = "0..*", arity = "1..",
            description = "Segment dumps produced by `kafka-dump-logs.sh`.")
    List<File> dumpFiles;

    @Override
    public void run() {

        SegmentDumpReader segmentDumpReader = new SegmentDumpReader();
        // Sort to get into offset order
        List<Segment> segments = dumpFiles.stream().sorted(Comparator.comparing(File::getName))
                .map(dumpFile -> {
                    Segment segment = segmentDumpReader.readSegment(dumpFile);
                    if (segment.type() != Segment.Type.DATA) {
                        throw new RuntimeException(segment.type().topicName + " partitions do not contain transactional messages");
                    }

                    return segment;
                }).collect(Collectors.toList());
        Set<String> topics = segments.stream().map(Segment::topicName).collect(Collectors.toSet());
        if (topics.size() > 1) {
            throw new RuntimeException("Segment dumps come from multiple different topics " + topics);
        }
        // TODO assert that they're in the right order (they should be because we sorted the files)
        var segmentInfo = segments.stream()
                .flatMap(segment -> {
                    Predicate<Batch> predicate = BatchPredicate.predicate(segment.type(), pid, producerEpoch, leaderEpoch, null);
                    var locatedStream = segment.batches();
                    if (predicate != null) {
                        locatedStream = locatedStream.filter(predicate);
                    }
                    return locatedStream;
                })
                .collect(TransactionalInfoCollector.collector());

        System.out.printf("num_committed: %d%n", segmentInfo.numTransactionalCommit());
        System.out.printf("num_aborted: %d%n", segmentInfo.numTransactionalAbort());
        System.out.printf("txn_size_stats: %s%n", segmentInfo.txnSizeStats());
        System.out.printf("txn_duration_stats_ms: %s%n", segmentInfo.txnDurationStats());
        for (EmptyTransaction emptyTransaction : segmentInfo.emptyTransactions()) {
            printEmpty(segments.size() > 1, emptyTransaction);
        }
        for (Map.Entry<ProducerSession, FirstBatchInTxn> entry : segmentInfo.openTransactions().entrySet()) {
            printOpen(segments.size() > 1, entry.getKey(), entry.getValue());
        }
    }

    private void printEmpty(boolean filenames, EmptyTransaction txn) {
        System.out.print("empty_txn:");
        if (filenames) {
            System.out.printf("%s:", txn.controlMessage().filename());
        }
        if (lineNumbers) {
            System.out.printf("%d:", txn.controlMessage().line());
        }
        System.out.printf(" %s%n", txn);
    }

    private void printOpen(boolean filenames, ProducerSession sess, FirstBatchInTxn txn) {
        System.out.print("open_txn:");
        if (filenames) {
            System.out.printf("%s:", txn.firstBatchInTxn().filename());
        }
        if (lineNumbers) {
            System.out.printf("%d:", txn.firstBatchInTxn().line());
        }
        System.out.printf(" %s->%s%n", sess, txn);
    }

}
