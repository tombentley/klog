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

import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.github.tombentley.klog.segment.model.Batch;
import com.github.tombentley.klog.segment.model.ProducerSession;

public final class SegmentInfo {
    private final Map<ProducerSession, FirstBatchInTxn> openTransactions;
    private final Batch firstBatch;
    private final Batch lastBatch;
    private final List<EmptyTransaction> emptyTransactions;
    private final long numTransactionalCommit;
    private final long numTransactionalAbort;
    private final IntSummaryStatistics txnSizeStats;
    private final IntSummaryStatistics txnDurationStats;

    public SegmentInfo(Map<ProducerSession, FirstBatchInTxn> openTransactions,
                       Batch firstBatch,
                       Batch lastBatch,
                       List<EmptyTransaction> emptyTransactions,
                       long numTransactionalCommit, long numTransactionalAbort,
                       IntSummaryStatistics txnSizeStats,
                       IntSummaryStatistics txnDurationStats) {
        this.openTransactions = openTransactions;
        this.firstBatch = firstBatch;
        this.lastBatch = lastBatch;
        this.emptyTransactions = emptyTransactions;
        this.numTransactionalCommit = numTransactionalCommit;
        this.numTransactionalAbort = numTransactionalAbort;
        this.txnSizeStats = txnSizeStats;
        this.txnDurationStats = txnDurationStats;
    }

    public Map<ProducerSession, FirstBatchInTxn> openTransactions() {
        return openTransactions;
    }

    public Batch firstBatch() {
        return firstBatch;
    }

    public Batch lastBatch() {
        return lastBatch;
    }

    public List<EmptyTransaction> emptyTransactions() {
        return emptyTransactions;
    }

    public long numTransactionalCommit() {
        return numTransactionalCommit;
    }

    public long numTransactionalAbort() {
        return numTransactionalAbort;
    }

    public IntSummaryStatistics txnSizeStats() {
        return txnSizeStats;
    }

    public IntSummaryStatistics txnDurationStats() {
        return txnDurationStats;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SegmentInfo) obj;
        return Objects.equals(this.openTransactions, that.openTransactions) &&
               Objects.equals(this.firstBatch, that.firstBatch) &&
               Objects.equals(this.lastBatch, that.lastBatch) &&
               Objects.equals(this.emptyTransactions, that.emptyTransactions) &&
               this.numTransactionalCommit == that.numTransactionalCommit &&
               this.numTransactionalAbort == that.numTransactionalAbort &&
               Objects.equals(this.txnSizeStats, that.txnSizeStats) &&
               Objects.equals(this.txnDurationStats, that.txnDurationStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(openTransactions, firstBatch, lastBatch, emptyTransactions, numTransactionalCommit, numTransactionalAbort, txnSizeStats, txnDurationStats);
    }

    @Override
    public String toString() {
        return "SegmentInfo[" +
               "openTransactions=" + openTransactions + ", " +
               "firstBatch=" + firstBatch + ", " +
               "lastBatch=" + lastBatch + ", " +
               "emptyTransactions=" + emptyTransactions + ", " +
               "committed=" + numTransactionalCommit + ", " +
               "aborted=" + numTransactionalAbort + ", " +
               "txnSizeStats=" + txnSizeStats + ", " +
               "txnDurationStats=" + txnDurationStats + ']';
    }


}
