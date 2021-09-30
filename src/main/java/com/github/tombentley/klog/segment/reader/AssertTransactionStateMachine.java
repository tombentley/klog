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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.github.tombentley.klog.segment.model.BaseMessage;
import com.github.tombentley.klog.segment.model.Batch;
import com.github.tombentley.klog.segment.model.ProducerSession;
import com.github.tombentley.klog.segment.model.TransactionStateChange;

/**
 * A function to be included in a stream pipeline which validates the state transitions in __transaction_state dumps.
 */
public class AssertTransactionStateMachine implements Function<Batch, Batch> {
    private final Map<ProducerSession, TransactionStateChange.State> transactions = new HashMap<>();

    @Override
    public Batch apply(Batch batch) {
        for (var message : batch.messages()) {
            if (message instanceof TransactionStateChange) {
                var stateChange = (TransactionStateChange) message;
                validateStateTransition(message, stateChange);
            }
        }
        return batch;
    }

    private void validateStateTransition(BaseMessage message, TransactionStateChange stateChange) {
        TransactionStateChange.State state = transactions.get(stateChange.session());
        if (state != null && !stateChange.state().validPrevious(state)) {
            throw new IllegalStateException(String.format("%s:%d: Illegal state change from %s to %s",
                    message.filename(), message.line(), state, stateChange.state()));
        }
        transactions.put(stateChange.session(), stateChange.state());
    }
}


