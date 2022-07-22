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
package com.github.tombentley.klog.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name="txn-coordinating-partition",
    description = "Determine the coordinating partition of __transaction_state for a given transactional.id"
)
public class TxnCoordinatingPartition implements Runnable {

    @Parameters(arity = "1", index = "0",
            description = "The transactional.id")
    String id;

    @Option(names = "num-partitions",
        description = "The number of partitions of __transaction_state",
    defaultValue = "50")
    int partitions;

    @Override
    public void run() {
        System.out.println(abs(id.hashCode()) % partitions);
    }

    private int abs(int n) {
        return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
    }
}
