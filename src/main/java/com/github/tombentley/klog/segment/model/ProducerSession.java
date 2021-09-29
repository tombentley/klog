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
package com.github.tombentley.klog.segment.model;

import java.util.Objects;

/**
 * A {@code <PID, epoch>} pair, identifying a producer session.
 */
public final class ProducerSession {
    private final long producerId;
    private final short producerEpoch;

    /**
     */
    public ProducerSession(long producerId, short producerEpoch) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ProducerSession) obj;
        return this.producerId == that.producerId &&
                this.producerEpoch == that.producerEpoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, producerEpoch);
    }

    @Override
    public String toString() {
        return "ProducerSession[" +
                "producerId=" + producerId + ", " +
                "producerEpoch=" + producerEpoch + ']';
    }

}
