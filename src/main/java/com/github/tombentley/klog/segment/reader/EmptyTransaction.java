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

import java.util.Objects;

import com.github.tombentley.klog.segment.model.Batch;
import com.github.tombentley.klog.segment.model.ControlMessage;

/**
 * Represents a transaction that was committed/aborted without any data.
 * I.e. there's a control record for a given {@code <PID, epoch>} pair, but either:
 * <ul>
 * <li>no data records since the previous control record for that {@code <PID, epoch>} pair, or,</li>
 * <li>no previous control or data records for that {@code <PID, epoch>} pair, or,</li>
 * </ul>
 * This could happen if a client crashes after sending {@code ADD_PARTITIONS_TO_TXN} but before {@code PRODUCE}.
 * In that case we'd expect the new instance of the client to get a new producer epoch and the transaction started
 * will eventually time out, so the coordinator will {@code WRITE_TXN_MARKERS} for abort.
 *
 * And empty committed transaction is a bug.
 */
public final class EmptyTransaction {
    private final Batch closingBatch;
    private final ControlMessage controlMessage;

    /**
     */
    public EmptyTransaction(Batch closingBatch, ControlMessage controlMessage) {
        this.closingBatch = closingBatch;
        this.controlMessage = controlMessage;
    }

    public Batch closingBatch() {
        return closingBatch;
    }

    public ControlMessage controlMessage() {
        return controlMessage;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (EmptyTransaction) obj;
        return Objects.equals(this.closingBatch, that.closingBatch) &&
                Objects.equals(this.controlMessage, that.controlMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(closingBatch, controlMessage);
    }

    @Override
    public String toString() {
        return "EmptyTransaction[" +
                "closingBatch=" + closingBatch + ", " +
                "controlMessage=" + controlMessage + ']';
    }
}
