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
package com.github.tombentley.klog.snapshot.reader;

import com.github.tombentley.klog.snapshot.model.ProducerState;
import com.github.tombentley.klog.segment.reader.UnexpectedFileContent;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Short.parseShort;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SnapshotDumpReader {
    private final static Pattern DATA_RECORD_PATTERN = Pattern.compile("producerId: (?<producerId>[0-9]+) " +
            "producerEpoch: (?<producerEpoch>[0-9]+) " +
            "coordinatorEpoch: (?<coordinatorEpoch>[0-9]+) " +
            "currentTxnFirstOffset: (?<currentTxnFirstOffset>None|Some\\([0-9]+\\))" +
            "( lastTimestamp: (?<lastTimestamp>[0-9]+))? " +
            "firstSequence: (?<firstSequence>[0-9]+) " +
            "lastSequence: (?<lastSequence>[0-9]+) " +
            "lastOffset: (?<lastOffset>[0-9]+) " +
            "offsetDelta: (?<offsetDelta>[0-9]+) " +
            "timestamp: (?<timestamp>[0-9]+)");

    public Snapshot readSnapshot(File dumpFile) {
        Stream<String> lines;
        try {
            lines = Files.lines(dumpFile.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return readSnapshot(dumpFile.getName(), lines);
    }

    public Snapshot readSnapshot(String dumpFileName, Stream<String> lines) {
        File[] dumpFile = {null};
        int[] lineNumber = {0};
        Spliterator<String> spliterator = lines.spliterator();
        // read header line
        if (!spliterator.tryAdvance(line -> {
            lineNumber[0]++;
            dumpFile[0] = this.readDumpingLine(line);
        })) {
            throw new UnexpectedFileContent("Expected > 0 lines");
        }
        // read content lines
        Snapshot.Type type = Snapshot.Type.PRODUCER;
        Stream<ProducerState> states = producerStates(dumpFileName, lineNumber[0],
                type, StreamSupport.stream(spliterator, false));
        return new Snapshot(dumpFileName, type, topicName(dumpFile[0]), states);
    }

    private File readDumpingLine(String line) {
        File file;
        String dumpingPattern = "^Dumping (.*)$";
        var dumpingMatcher = Pattern.compile(dumpingPattern).matcher(line);
        if (!dumpingMatcher.matches()) {
            throw new UnexpectedFileContent("Expected first line to match " + dumpingPattern);
        } else {
            file = new File(dumpingMatcher.group(1));
        }
        return file;
    }

    private Stream<ProducerState> producerStates(String dumpFileName, int startLineNumber, Snapshot.Type type, Stream<String> lines) {
        Stream<ProducerState> outputStream = lines.flatMap(new Function<String, Stream<? extends ProducerState>>() {
            int lineNumber = startLineNumber;

            @Override
            public Stream<? extends ProducerState> apply(String line) {
                lineNumber++;
                return Stream.of(parseData(line, dumpFileName));
            }

            private ProducerState parseData(String line,
                                            String dumpFilename) {
                Matcher matcher = DATA_RECORD_PATTERN.matcher(line);
                if (!matcher.matches()) {
                    throw new IllegalStateException("This doesn't look like a data record");
                }
                String currentTxnFirstOffset = matcher.group("currentTxnFirstOffset")
                        .replaceAll("None", "0")
                        .replaceAll("[^\\d]", "");
                String lastTimestamp = matcher.group("lastTimestamp") != null
                        ? matcher.group("lastTimestamp") : "0";
                return new ProducerState(dumpFilename, lineNumber,
                        parseLong(matcher.group("producerId")),
                        parseShort(matcher.group("producerEpoch")),
                        parseInt(matcher.group("coordinatorEpoch")),
                        parseLong(currentTxnFirstOffset),
                        parseLong(lastTimestamp),
                        parseInt(matcher.group("firstSequence")),
                        parseInt(matcher.group("lastSequence")),
                        parseLong(matcher.group("lastOffset")),
                        parseInt(matcher.group("offsetDelta")),
                        parseLong(matcher.group("timestamp")));
            }
        });

        outputStream = outputStream.map(new AssertTransactionalProducer());
        return outputStream;
    }

    private String topicName(File file) {
        File parent = file.getParentFile();
        if (parent != null && parent.getName().matches("[a-zA-Z0-9_.-]+-[0-9]+")) {
            return parent.getName().substring(parent.getName().lastIndexOf("-"));
        }
        return null;
    }

    /*
    mvn clean compile exec:java \
      -Dexec.mainClass="com.github.tombentley.klog.snapshot.reader.SnapshotDumpReader" \
      -Dexec.args="/home/fvaleri/Documents/setup/tmp/ENTMQST-4101/partitions/__consumer_offsets-27/00000000000933607637.snapshot.dump-2.7"
    */
    public static void main(String[] args) {
        System.out.println();
        SnapshotDumpReader dumpReader = new SnapshotDumpReader();
        // Sort to get into offset order
        Arrays.stream(args).map(File::new).flatMap(dumpFile ->
                dumpReader.readSnapshot(dumpFile).states()).forEach(System.out::println);
    }
}
