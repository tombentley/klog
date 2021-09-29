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
import picocli.CommandLine;

@CommandLine.Command(
        name = "cat",
        description = """
                Print the log dumps
                
                This is slightly more useful that just looking at the dumps directly, because it converts
                the timestamps into something human readable.
                """
)
public class Cat implements Runnable {
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
        dumpFiles.stream().sorted(Comparator.comparing(File::getName)).forEach(dumpFile -> {
            Stream<Batch> locatedStream = segmentDumpReader.readSegment(dumpFile).batches();
            if (predicate != null) {
                locatedStream = locatedStream.filter(predicate);
            }
            locatedStream.forEach(batch -> {
                System.out.println(batch);
                for (var msg : batch.messages()) {
                    System.out.println(msg);
                }
            });
        });
    }

}
