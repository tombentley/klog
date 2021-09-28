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

import com.github.tombentley.kafka.logs.segment.model.Batch;
import com.github.tombentley.kafka.logs.segment.model.ControlMessage;

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
public record EmptyTransaction(String file, int line, Batch closingBatch, ControlMessage controlMessage) { }
