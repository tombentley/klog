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
package com.github.tombentley.klog.segment.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;

import com.github.tombentley.klog.segment.model.BaseMessage;
import com.github.tombentley.klog.segment.model.Batch;
import com.github.tombentley.klog.segment.model.ControlMessage;
import com.github.tombentley.klog.segment.model.DataMessage;
import com.github.tombentley.klog.segment.model.ProducerSession;
import com.github.tombentley.klog.segment.model.TransactionStateChange;
import com.github.tombentley.klog.segment.model.TransactionStateDeletion;

public class TransactionalInfoCollector {

    private Batch currentBatch;
    private final Map<ProducerSession, FirstBatchInTxn> openTransactions = new HashMap<>();
    private Batch firstBatch;
    private final Map<ProducerSession, EmptyTransaction> emptyTransactions = new HashMap<>();
    private final IntSummaryStatistics txnSizeStats = new IntSummaryStatistics();
    private final IntSummaryStatistics txnDurationStats = new IntSummaryStatistics();
    private long numTransactionalCommit = 0;
    private long numTransactionalAbort = 0;

    public TransactionalInfoCollector() {
    }

    public static Collector<Batch, TransactionalInfoCollector, TransactionalInfo> collector() {
        return Collector.of(TransactionalInfoCollector::new,
                TransactionalInfoCollector::accumulator,
                TransactionalInfoCollector::combiner,
                TransactionalInfoCollector::finisher);
    }

    public void accumulator(Batch batch) {

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

        for (var message : batch.messages()) {
            if (message instanceof DataMessage) {

            } else if (message instanceof ControlMessage) {
                var control = (ControlMessage) message;
                if (control.commit()) {
                    numTransactionalCommit++;
                } else {
                    numTransactionalAbort++;
                }
                var firstBatchInTxn = openTransactions.remove(currentBatch.session());
                if (firstBatchInTxn == null) {
                    emptyTransactions.put(currentBatch.session(), new EmptyTransaction(currentBatch, control));
                } else {
                    txnSizeStats.accept(firstBatchInTxn.numDataBatches().get());
                    txnDurationStats.accept((int) (currentBatch.createTime() - firstBatchInTxn.firstBatchInTxn().createTime()));
                }
            } else if (message instanceof TransactionStateChange) {

            } else if (message instanceof TransactionStateDeletion) {

            }
        }
    }


    public TransactionalInfoCollector combiner(TransactionalInfoCollector b) {
        return null;// TODO
    }

    public TransactionalInfo finisher() {
        return new TransactionalInfo(openTransactions, firstBatch, currentBatch, emptyTransactions,
                numTransactionalCommit, numTransactionalAbort,
                txnSizeStats, txnDurationStats);
    }

}
