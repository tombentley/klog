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
package com.github.tombentley.kafka.logs.segment.model;

import java.time.Instant;

/**
 * A data record from a {@code __transaction_state} segment with an empty payload ({@code payload: <DELETE>}).
 * kafka-dump-log.sh output is a single line like:
 * <pre>
 * | offset: 62414131 CreateTime: 1621429407409 keysize: 54 valuesize: -1 sequence: -1 headerKeys: []
 * key: transaction_metadata::transactionalId=MY_TXNAL_ID payload: <DELETE>
 * </pre>
 */
public record TransactionStateDeletion(long offset,
                                       long createTime,
                                       int keySize,
                                       int valueSize,
                                       int sequence,
                                       String headerKeys,
                                       String transactionId) implements TransactionStateMessage {
    @Override
    public String toString() {
        return "TransactionStateDeletion(" +
                "offset=" + offset +
                ", createTime=" + Instant.ofEpochMilli(createTime) +
                ", keySize=" + keySize +
                ", valueSize=" + valueSize +
                ", sequence=" + sequence +
                ", headerKeys='" + headerKeys + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ')';
    }
}
