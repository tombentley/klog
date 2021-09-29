package com.github.tombentley.klog.segment.reader;/*
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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.tombentley.klog.segment.model.Batch;

/**
 * The first batch in a transaction.
 */
public final class FirstBatchInTxn {
    private final Batch firstBatchInTxn;
    private final AtomicInteger numDataBatches;

    /**
     */
    public FirstBatchInTxn(Batch firstBatchInTxn, AtomicInteger numDataBatches) {
        this.firstBatchInTxn = firstBatchInTxn;
        this.numDataBatches = numDataBatches;
    }

    public Batch firstBatchInTxn() {
        return firstBatchInTxn;
    }

    public AtomicInteger numDataBatches() {
        return numDataBatches;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (FirstBatchInTxn) obj;
        return Objects.equals(this.firstBatchInTxn, that.firstBatchInTxn) &&
                Objects.equals(this.numDataBatches, that.numDataBatches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstBatchInTxn, numDataBatches);
    }

    @Override
    public String toString() {
        return "FirstBatchInTxn[" +
                "firstBatchInTxn=" + firstBatchInTxn + ", " +
                "numDataBatches=" + numDataBatches + ']';
    }
}
