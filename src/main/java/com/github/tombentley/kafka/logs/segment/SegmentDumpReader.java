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
package com.github.tombentley.kafka.logs.segment;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.github.tombentley.kafka.logs.segment.model.BaseMessage;
import com.github.tombentley.kafka.logs.segment.model.Batch;
import com.github.tombentley.kafka.logs.segment.model.ControlMessage;
import com.github.tombentley.kafka.logs.segment.model.DataMessage;
import com.github.tombentley.kafka.logs.segment.model.Located;
import com.github.tombentley.kafka.logs.segment.model.ProducerSession;
import com.github.tombentley.kafka.logs.segment.model.TransactionStateChangeMessage;
import com.github.tombentley.kafka.logs.segment.model.TransactionStateDeletion;
import com.github.tombentley.kafka.logs.segment.model.TransactionStateMessage;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Short.parseShort;

public class SegmentDumpReader {

    private final static Pattern MESSAGE_SET_PATTERN = Pattern.compile("baseOffset: (?<baseOffset>[0-9]+) " +
            "lastOffset: (?<lastOffset>[0-9]+) " +
            "count: (?<count>[0-9]+) " +
            "baseSequence: (?<baseSequence>-?[0-9]+) " +
            "lastSequence: (?<lastSequence>-?[0-9]+) " +
            "producerId: (?<producerId>-?[0-9]+) " +
            "producerEpoch: (?<producerEpoch>-?[0-9]+) " +
            "partitionLeaderEpoch: (?<partitionLeaderEpoch>[0-9]+) " +
            "isTransactional: (?<isTransactional>true|false) " +
            "isControl: (?<isControl>true|false) " +
            "position: (?<position>[0-9]+) " +
            "CreateTime: (?<createTime>[0-9]+) " +
            "size: (?<size>[0-9]+) " +
            "magic: (?<magic>[0-9]+) " +
            "compresscodec: (?<compressCodec>[A-Z]+) " +
            "crc: (?<crc>[0-9]+) " +
            "isvalid: (?<isValid>true|false)");

    private final static Pattern DATA_RECORD_PATTERN = Pattern.compile("\\| offset: (?<offset>[0-9]+) " +
            "CreateTime: (?<createTime>[0-9]+) " +
            "keysize: (?<keySize>[0-9]+) " +
            "valuesize: (?<valueSize>-?[0-9]+) " +
            "sequence: (?<sequence>-?[0-9]+) " +
            "headerKeys: \\[(?<headerKeys>.*)\\]");

    private final static Pattern CONTROL_RECORD_PATTERN = Pattern.compile(DATA_RECORD_PATTERN.pattern() + " " +
            "endTxnMarker: (?<endTxnMarker>COMMIT|ABORT) " +
            "coordinatorEpoch: (?<coordinatorEpoch>[0-9]+)");

    private final static Pattern TRANSACTIONAL_RECORD_PATTERN = Pattern.compile(DATA_RECORD_PATTERN.pattern() + " " +
            "key: transaction_metadata::transactionalId=(?<transactionalId>.*) " +
            "payload: (?<payload>.*)");
    private final static Pattern TRANSACTIONAL_PAYLOAD_PATTERN = Pattern.compile("producerId:(?<producerId>[0-9]+)," +
            "producerEpoch:(?<producerEpoch>[0-9]+)," +
            "state=(?<state>Ongoing|PrepareCommit|PrepareAbort|CompleteCommit|CompleteAbort|Empty|Dead)," +
            "partitions=\\[(?<partitions>.*)\\]," +
            "txnLastUpdateTimestamp=(?<txnLastUpdateTimestamp>[0-9]+)," +
            "txnTimeoutMs=(?<txnTimeoutMs>[0-9]+)");

    Stream<Located> readSegment2(File dumpFile) throws IOException {
        File[] segmentFile = {null};
        Spliterator<String> spliterator = Files.lines(dumpFile.toPath()).spliterator();
        if (!spliterator.tryAdvance(line -> segmentFile[0] = this.readDumpingLine(line))) {
            throw new UnexpectedFileContent("Expected > 1 lines");
        }
        SegmentType segmentType = segmentType(dumpFile, segmentFile[0]);
        if (!spliterator.tryAdvance(line -> this.readStartingOffsetLine(segmentFile[0], line))) {
            throw new UnexpectedFileContent("Expected > 2 lines");
        }
        int[] expect = {0};
        return StreamSupport.stream(spliterator, false).map(line -> {
            if (expect[0] == 0) {
                var batch = parseBatch(segmentType, line, dumpFile.getName(), -1);
                if (batch.isControl()) {
                    expect[0] = -batch.count();
                } else {
                    expect[0] = batch.count();
                }
                return batch;
            } else if (expect[0] > 0) {
                BaseMessage result;
                if (segmentType == SegmentType.TRANSACTION_STATE) {
                    result = parseTransactionState(expect[0], line, dumpFile.getName(), -1);
                } else {
                    result = parseData(expect[0], line, dumpFile.getName(), -1);
                }
                expect[0]--;
                return result;
            } else {
                var control = parseControl(expect[0], line, dumpFile.getName(), -1);
                expect[0]++;
                return control;
            }
        });
    }

//    SegmentInfo readSegment(File file, InputStream in, BiConsumer<SegmentType, Batch> batchConsumer, BiConsumer<SegmentType, BaseMessage> messageConsumer) throws IOException {
//        String filename = file.getName();
//        LineNumberReader reader = new LineNumberReader(new InputStreamReader(in));
//        // e.g. Dumping /var/lib/kafka/data-0/kafka-vol-1/my-topic-1/00000000000002226094.log
//
//        String line = reader.readLine();
//        File segmentFile = readDumpingLine(line);
//        SegmentType segmentType = segmentType(file, segmentFile);
//
//        // e.g. Starting offset: 2226094
//        line = reader.readLine();
//        long startingOffset = readStartingOffsetLine(segmentFile, line);
//        var openTransactions = new HashMap<ProducerSession, FirstBatchInTxn>();
//        var emptyTransactions = new ArrayList<EmptyTransaction>();
//        Batch firstBatch = null;
//        Batch batch = null;
//        int expect = 0;
//        // 0 => expecting a batch line, > 0 => expecting that $expect data message, < 0 expecting -$expect many control messages
//
//        while (true) {
//            line = reader.readLine();
//            if (line == null) {
//                break;
//            }
//            try {
//                if (expect == 0) {
//                    batch = parseBatch(segmentType, line, filename, reader.getLineNumber());
//                    if (firstBatch == null) {
//                        firstBatch = batch;
//                        // This only works for topics with cleanup.policy: delete
////                        if (startingOffset != firstBatch.baseOffset()) {
////                            throw new UnexpectedFileContent("The 2nd line of the file claims the starting offset is " + startingOffset + ", but first message has base offset " + firstBatch.baseOffset());
////                        }
//                        // TODO support inferring whether any compaction has taken place
//                    }
//                    if (batch.isControl()) {
//                        expect = -batch.count();
//                    } else {
//                        expect = batch.count();
//                    }
//                    if (batch.isTransactional()) {
//                        ProducerSession session = new ProducerSession(batch.producerId(), batch.producerEpoch());
//                        if (batch.isControl()) {
//                            if (batch.count() != 1) {
//                                throw new UnexpectedFileContent("Transactional data batch with >1 control records");
//                            }
//                            // Defer removal from openTransactions till we've seen the control record
//                        } else {
//                            var firstInBatch = openTransactions.get(session);
//                            if (firstInBatch == null) {
//                                openTransactions.put(session, new FirstBatchInTxn(filename, reader.getLineNumber(), batch, new AtomicInteger(1)));
//                            } else {
//                                firstInBatch.numDataBatches().incrementAndGet();
//                            }
//                        }
//                    }
//                } else if (expect > 0) {
//                    if (segmentType == SegmentType.TRANSACTION_STATE) {
//                        var txn = parseTransactionState(expect, line, filename, reader.getLineNumber());
//                        messageConsumer.accept(segmentType, txn);
//                    } else {
//                        var data = parseData(expect, line, filename, reader.getLineNumber());
//                        messageConsumer.accept(segmentType, data);
//                    }
//                    expect--;
//                } else {
//                    var control = parseControl(expect, line, filename, reader.getLineNumber());
//                    var removed = openTransactions.remove(batch.session());
//                    if (removed == null) {
//                        emptyTransactions.add(new EmptyTransaction(filename, reader.getLineNumber(), batch, control));
//                    }
//                    expect++;
//                }
//            } catch (Exception e) {
//                throw new RuntimeException(String.format("%s:%d:", file, reader.getLineNumber()), e);
//            }
//        }
//        return new SegmentInfo(file, segmentType, openTransactions, firstBatch, batch, emptyTransactions);
//    }

    private void checkBatch(SegmentType segmentType, Batch batch) {
        if (segmentType == SegmentType.TRANSACTION_STATE) {
            if (batch.producerId() != -1) {
                throw new UnexpectedFileContent("Segment of __transaction_state with producerId != -1");
            } else if (batch.producerEpoch() != -1) {
                throw new UnexpectedFileContent("Segment of __transaction_state with producerEpoch != -1");
            }
        } else if (segmentType == SegmentType.DATA) {
            if (batch.isTransactional()) {
                if (batch.producerId() == -1) {
                    throw new UnexpectedFileContent("Transactional batch with producerId == -1");
                } else if (batch.producerEpoch() == -1) {
                    throw new UnexpectedFileContent("Transactional batch with producerEpoch == -1");
                }
            }
        }
    }

    private SegmentType segmentType(File dumpFile, File segmentFile) {
        SegmentType segmentType;
        File parent = segmentFile.getParentFile();
        if (parent != null) {
            String name = parent.getName();
            segmentType = name.matches(SegmentType.TRANSACTION_STATE.topicName + "-[0-9]+") ? SegmentType.TRANSACTION_STATE :
                          name.matches(SegmentType.CONSUMER_OFFSETS.topicName + "-[0-9]+") ? SegmentType.CONSUMER_OFFSETS : SegmentType.DATA;
        } else {
            // Can happen if kafka-dump-log.sh run from the directory containing the segment
            System.err.printf("%s: Don't know original segment file name, assuming a normal segment", dumpFile);
            segmentType = SegmentType.DATA;
        }
        return segmentType;
    }

    private long filenameOffset(File segmentFile) {
        String pattern = "[0-9]+\\.log";
        if (!segmentFile.getName().matches(pattern)) {
            throw new UnexpectedFileContent("Expected FILE in first line to match " + pattern);
        }
        return parseLong(segmentFile.getName().substring(0, segmentFile.getName().indexOf('.')));
    }

    private File readDumpingLine(String line) {
        File segmentFile;
        String dumpingPattern = "^Dumping (.*)$";
        var dumpingMatcher = Pattern.compile(dumpingPattern).matcher(line);
        if (!dumpingMatcher.matches()) {
            throw new UnexpectedFileContent("Expected first line to match " + dumpingPattern);
        } else {
            segmentFile = new File(dumpingMatcher.group(1));
        }
        return segmentFile;
    }

    private long readStartingOffsetLine(File segmentFile, String line) {
        String startingOffsetPattern = "^Starting offset: ([0-9]+)$";
        var offsetMatcher = Pattern.compile(startingOffsetPattern).matcher(line);
        if (!offsetMatcher.matches()) {
            throw new UnexpectedFileContent("Expected second line to match " + startingOffsetPattern);
        }
        long startingOffset = parseLong(offsetMatcher.group(1));
        long filenameOffset = filenameOffset(segmentFile);
        if (filenameOffset != startingOffset) {
            throw new UnexpectedFileContent("Segment file name " + segmentFile + " implies starting offset of " + filenameOffset + " but 2nd line says offset is " + startingOffset);
        }
        return startingOffset;
    }

    private DataMessage parseData(int expect, String line, String filename, int lineNumber) {
        Matcher matcher = DATA_RECORD_PATTERN.matcher(line);
        if (!matcher.matches()) {
            throw new IllegalStateException("Expected " + (expect) + " data records in batch, but this doesn't look like a data record");
        }
        return new DataMessage(filename, lineNumber,
                parseLong(matcher.group("offset")),
                parseLong(matcher.group("createTime")),
                parseInt(matcher.group("keySize")),
                parseInt(matcher.group("valueSize")),
                parseInt(matcher.group("sequence")),
                matcher.group("headerKeys"));
    }

    private TransactionStateMessage parseTransactionState(int expect, String line, String filename, int lineNumber) {
        Matcher matcher = TRANSACTIONAL_RECORD_PATTERN.matcher(line);
        if (!matcher.matches()) {
            throw new IllegalStateException("Expected " + (expect) + " txn records in batch, but this doesn't look like a txn record");
        }
        String payload = matcher.group("payload");
        if (payload.equals("<DELETE>")) {
            return null;
        } else {
            Matcher payloadMatcher = TRANSACTIONAL_PAYLOAD_PATTERN.matcher(payload);
            if (!payloadMatcher.matches()) {
                return new TransactionStateDeletion(filename, lineNumber,
                        parseLong(matcher.group("offset")),
                        parseLong(matcher.group("createTime")),
                        parseInt(matcher.group("keySize")),
                        parseInt(matcher.group("valueSize")),
                        parseInt(matcher.group("sequence")),
                        matcher.group("headerKeys"),
                        matcher.group("transactionalId"));
            }
            return new TransactionStateChangeMessage(filename, lineNumber,
                    parseLong(matcher.group("offset")),
                    parseLong(matcher.group("createTime")),
                    parseInt(matcher.group("keySize")),
                    parseInt(matcher.group("valueSize")),
                    parseInt(matcher.group("sequence")),
                    matcher.group("headerKeys"),
                    matcher.group("transactionalId"),
                    parseLong(payloadMatcher.group("producerId")),
                    parseShort(payloadMatcher.group("producerEpoch")),
                    TransactionStateChangeMessage.State.valueOf(payloadMatcher.group("state")),
                    payloadMatcher.group("partitions"),
                    parseLong(payloadMatcher.group("txnLastUpdateTimestamp")),
                    parseLong(payloadMatcher.group("txnTimeoutMs")));
        }
    }

    private ControlMessage parseControl(int expect, String line, String filename, int lineNumber) {
        Matcher matcher = CONTROL_RECORD_PATTERN.matcher(line);
        if (!matcher.matches()) {
            throw new IllegalStateException("Expected " + (-expect) + " control records in batch, but this doesn't look like a control record");
        }
        return new ControlMessage(filename, lineNumber,
                parseLong(matcher.group("offset")),
                parseLong(matcher.group("createTime")),
                parseInt(matcher.group("keySize")),
                parseInt(matcher.group("valueSize")),
                parseInt(matcher.group("sequence")),
                matcher.group("headerKeys"),
                matcher.group("endTxnMarker").equals("COMMIT"),
                parseInt(matcher.group("coordinatorEpoch")));
    }

    private Batch parseBatch(SegmentType segmentType, String line, String filename, int lineNumber) {
        Batch currentBatch;
        Matcher matcher = MESSAGE_SET_PATTERN.matcher(line);
        if (!matcher.matches()) {
            throw new IllegalStateException("Expected a message batch");
        }
        currentBatch = new Batch(filename, lineNumber,
                parseLong(matcher.group("baseOffset")),
                parseLong(matcher.group("lastOffset")),
                parseInt(matcher.group("count")),
                parseInt(matcher.group("baseSequence")),
                parseInt(matcher.group("lastSequence")),
                parseLong(matcher.group("producerId")),
                parseShort(matcher.group("producerEpoch")),
                parseInt(matcher.group("partitionLeaderEpoch")),
                parseBoolean(matcher.group("isTransactional")),
                parseBoolean(matcher.group("isControl")),
                parseLong(matcher.group("position")),
                parseLong(matcher.group("createTime")),
                parseInt(matcher.group("size")),
                Byte.parseByte(matcher.group("magic")),
                matcher.group("compressCodec"),
                Integer.parseUnsignedInt(matcher.group("crc")),
                parseBoolean(matcher.group("isValid")));
        checkBatch(segmentType, currentBatch);
        return currentBatch;
    }
//
//    private void readSegments(List<File> args) {
//        List<File> sortedFiles = args.stream().sorted().collect(Collectors.toList());
//        SegmentInfo prevInfo = null;
//        for (var file : sortedFiles) {
//            try {
//                try (var in = new FileInputStream(file)) {
//                    var info = readSegment(file, in,
//                            (type, batch) -> {},
//                            (type, message) -> {
//                                if (message instanceof TransactionStateChangeMessage
//                                        && ((TransactionStateChangeMessage) message).producerEpoch() == 0
//                                        && ((TransactionStateChangeMessage) message).producerId() == 961000) {
//                                    //System.err.println(message);
//                                }
//                            });
//                    if (prevInfo != null &&
//                            info.firstBatch().baseOffset() <= prevInfo.lastBatch().lastOffset()) {
//                        throw new UnexpectedFileContent(prevInfo.dumpFile() + " offset incompatible with " + info.dumpFile());
//                    }
//                    // TODO carry open transactions over between segments
//                    System.err.println("First batch " + info.firstBatch());
//                    info.emptyTransactions().forEach(txn -> System.err.println("Empty transaction " + txn));
//                    System.err.println("Last batch " + info.lastBatch());
//                    info.openTransactions().forEach((session, firstBatch) -> {
//                        System.err.println("Still open transaction: " + session + " -> " + firstBatch);
//                    });
//                    prevInfo = info;
//                }
//            } catch (IOException e) {
//                throw new UncheckedIOException(e);
//            }
//        }
//    }

    static class SegmentInfoCollector {

        private Batch currentBatch;
        private Map<ProducerSession, FirstBatchInTxn> openTransactions = new HashMap<>();
        private Batch firstBatch;
        private List<EmptyTransaction> emptyTransactions = new ArrayList<>();
        private IntSummaryStatistics txnSizeStats = new IntSummaryStatistics();
        private IntSummaryStatistics txnDurationStats = new IntSummaryStatistics();
        private long committed = 0;
        private long aborted = 0;
        private Map<ProducerSession, TransactionStateChangeMessage.State> transactions = new HashMap<>();

        public SegmentInfoCollector() {
        }

        public static Collector<Located, SegmentInfoCollector, SegmentInfo> collector() {
            return Collector.of(SegmentInfoCollector::new,
                    SegmentInfoCollector::accumulator,
                    SegmentInfoCollector::combiner,
                    SegmentInfoCollector::finisher);
        }

        public void accumulator(Located x) {
            if (x instanceof Batch batch) {
                if (firstBatch == null) {
                    firstBatch = batch;
                }
                currentBatch = batch;
                if (batch.isTransactional()) {
                    ProducerSession session = new ProducerSession(batch.producerId(), batch.producerEpoch());
                    if (batch.isControl()) {
                        if (batch.count() != 1) {
                            throw new UnexpectedFileContent("Transactional data batch with >1 control records");
                        }
                        // Defer removal from openTransactions till we've seen the control record
                    } else {
                        var firstInBatch = openTransactions.get(session);
                        if (firstInBatch == null) {
                            openTransactions.put(session, new FirstBatchInTxn(batch, new AtomicInteger(1)));
                        } else {
                            firstInBatch.numDataBatches().incrementAndGet();
                        }
                    }
                }
            } else {
                var message = (BaseMessage) x;
                if (message instanceof DataMessage) {

                } else if (message instanceof ControlMessage control) {
                    if (control.commit()) {
                        committed++;
                    } else {
                        aborted++;
                    }
                    var firstBatchInTxn = openTransactions.remove(currentBatch.session());
                    if (firstBatchInTxn == null) {
                        emptyTransactions.add(new EmptyTransaction(currentBatch, control));
                    } else {
                        txnSizeStats.accept(firstBatchInTxn.numDataBatches().get());
                        txnDurationStats.accept((int) (currentBatch.createTime() - firstBatchInTxn.firstBatchInTxn().createTime()));
                    }
                } else if (message instanceof TransactionStateChangeMessage stateChange) {
                    validateStateTransition(message, stateChange);
                } else if (message instanceof TransactionStateDeletion deletion) {

                }
            }
        }

        private void validateStateTransition(BaseMessage message, TransactionStateChangeMessage stateChange) {
            TransactionStateChangeMessage.State state = transactions.get(stateChange.session());
            if (state != null && !stateChange.state().validPrevious(state)) {
                throw new RuntimeException(message.filename() + ": " + message.line() + ": Illegal state change from " + state + " to " + stateChange.state());
            }
            transactions.put(stateChange.session(), stateChange.state());
        }

        public SegmentInfoCollector combiner(SegmentInfoCollector b) {
            return null;// TODO
        }

        public SegmentInfo finisher() {
            return new SegmentInfo(openTransactions, firstBatch, currentBatch, emptyTransactions,
                    committed, aborted,
                    txnSizeStats, txnDurationStats);
        }

    }

    public static void main(String[] args) {
        SegmentDumpReader segmentDumpReader = new SegmentDumpReader();
        // Sort to get into offset order
        //segmentDumpReader.readSegments(Arrays.stream(args).map(File::new).collect(Collectors.toList()));
        Arrays.stream(args).map(File::new).map(dumpFile -> {
            try {
                return segmentDumpReader.readSegment2(dumpFile).collect(SegmentInfoCollector.collector());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).forEach(x -> {
            System.out.println("First batch: " + x.firstBatch());
            x.emptyTransactions().forEach(txn -> System.out.println("Empty txn: " + txn));
            System.out.println("Last batch: " + x.lastBatch());
            x.openTransactions().forEach((sess, txn) -> System.out.println("Open transaction: " + sess + "->" + txn));
            System.out.println("#committed: " + x.committed());
            System.out.println("#aborted: " + x.aborted());
            System.out.println("Txn sizes: " + x.txnSizeStats());
            System.out.println("Txn durations(ms): " + x.txnDurationStats());
        });
    }

}
