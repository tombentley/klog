# `klog` -- analyse kafka segment dumps

## What is it?

A tool to analyse dumps of Kafka log segments, as produced by `kafka-dump-logs.sh`.

## Building

You can build a native executable using GraalVM:

```shell
./mvnw package -Pnative
```

If you already have Docker installed, then you can build the native image without GraalVM:

```shell
mvn clean package -Pnative -Dquarkus.native.container-build=true
```

If you only have Minikube, then you can build it too:

```shell
minikube start
eval $(minikube -p minikube docker-env)
mvn clean package -Pnative -Dquarkus.native.remote-container-build=true
```

## Installing

You can copy the executable to some directory in your `$PATH`, for example:

```shell
sudo cp target/klog-*-SNAPSHOT-runner ~/usr/bin/klog
```

Alternatively you can use alias in your shell session:

```shell
alias klog=target/klog-*-SNAPSHOT-runner
```

Or add it to your `.bashrc` to make it permanent: 

```shell
echo alias klog=target/klog-*-runner >> ~/.bashrc
```

## Usage

Currently there's only the `segment` subcommand which itself takes two subcommands


### `klog segment cat`

This will echo a dumped segment file (or files) to standard output

```shell
klog segment cat 00000000000002226093.log
```

Snippet of example output:

```
Batch(baseOffset=2253037, lastOffset=2253037, count=1, baseSequence=143, lastSequence=143, producerId=895510, producerEpoch=1, partitionLeaderEpoch=539, isTransactional=true, isControl=false, position=37567204, createTime=2021-09-09T08:45:55.998Z, size=4526, magic=2, compressCodec='ZSTD', crc=-1607451119, isValid=false)
  DataMessage(offset=2253037, createTime=2021-09-09T08:45:55.998Z, keySize=52, valueSize=184452, sequence=143, headerKeys='foo,bar')
Batch(baseOffset=2253038, lastOffset=2253038, count=1, baseSequence=-1, lastSequence=-1, producerId=895510, producerEpoch=1, partitionLeaderEpoch=539, isTransactional=true, isControl=true, position=37571730, createTime=2021-09-09T08:45:56.044Z, size=78, magic=2, compressCodec='NONE', crc=-170113429, isValid=false)
  ControlMessage(offset=2253038, createTime=2021-09-09T08:45:56.044Z, keySize=4, valueSize=6, sequence=-1, headers='', commit=true, coordinatorEpoch=887)
```


There's not much value in this above regular `cat` except it will interpret all timestamps in a human readable way. (Plain segment dump represent these as a millisecond offset since the UNIX epoch) and colourise the output.

Filtering options are supported: `--pid`, `--producer-epoch`, `leader-epoch` and, for `__transaction_state` dumps, `--transactional-id`. When multiple options are present a batch or message must support all the filters to be included in the output.

### `klog segment txn-stat`

This will report a statistics transactions in the given segment dumps of normal partitions.

```shell
klog segment txn-stat 00000000000002226093.log
```

Example output:
```
num_committed: 12683
num_aborted: 2
txn_size_stats: IntSummaryStatistics{count=12683, sum=12772, min=1, average=1.007017, max=6}
txn_duration_stats_ms: IntSummaryStatistics{count=12683, sum=643672, min=11, average=50.750769, max=32189}
empty_txn: EmptyTransaction[closingBatch=Batch(baseOffset=2241851, lastOffset=2241851, count=1, baseSequence=-1, lastSequence=-1, producerId=895428, producerEpoch=12, partitionLeaderEpoch=531, isTransactional=true, isControl=true, position=19143380, createTime=2021-09-06T07:47:42.540Z, size=78, magic=2, compressCodec='NONE', crc=-1540206536, isValid=true), controlMessage=ControlMessage(offset=2241851, createTime=2021-09-06T07:47:42.540Z, keySize=4, valueSize=6, sequence=-1, headers='', commit=false, coordinatorEpoch=612)]
empty_txn: EmptyTransaction[closingBatch=Batch(baseOffset=2250125, lastOffset=2250125, count=1, baseSequence=-1, lastSequence=-1, producerId=894436, producerEpoch=4, partitionLeaderEpoch=534, isTransactional=true, isControl=true, position=32948458, createTime=2021-09-08T13:22:27.087Z, size=78, magic=2, compressCodec='NONE', crc=448547950, isValid=true), controlMessage=ControlMessage(offset=2250125, createTime=2021-09-08T13:22:27.087Z, keySize=4, valueSize=6, sequence=-1, headers='', commit=false, coordinatorEpoch=911)]
open_txn: ProducerSession[producerId=894436, producerEpoch=4]->FirstBatchInTxn[firstBatchInTxn=Batch(baseOffset=2250126, lastOffset=2250126, count=1, baseSequence=660, lastSequence=660, producerId=894436, producerEpoch=4, partitionLeaderEpoch=534, isTransactional=true, isControl=false, position=32948536, createTime=2021-09-08T13:20:26.964Z, size=6764, magic=2, compressCodec='ZSTD', crc=-1999558231, isValid=true), numDataBatches=1]
```

Currently, this includes:

* `num_committed` The number of transactional commits
* `num_aborted` The number of transactional aborts
* `txn_size_stats` Some stats about the number of data batches in each transaction
* `txn_duraction_stats_ms` Some stats about the duration of transactions
* `empty_txn` (multiple occurrences) Info about each empty transaction in the log. An empty transaction is one where, for a given producer session (identified by a PID and producer epoch), a transaction is ended by a control batch without the previous batch for that session being a data batch. 
* `open_txn` (multiple occurrences) Info about any open transactions in the log. An open transaction is one where there's a data batch for a given producer session that's not followed by a control batch.

As for `klog segment cat` filtering options are supported: `--pid`, `--producer-epoch`, `leader-epoch`

## Example use case: Post-mortem investigation of hanging transactions

Investigate hanging transactions by `klog segment txn-stat` on segment dumps of the partition with the hanging transaction to find the PID and producer epoch of the producer session lacking a control record. Then use `klog segment cat` to find the transactional id. 

## Development

This project uses [Quarkus](https://quarkus.io/).

### Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./mvnw compile quarkus:dev
```

To seed the command line arguments, pass the `-Dquarkus.args` option:

```shell script
./mvnw compile quarkus:dev -Dquarkus.args='patch get connectors'
```

In dev mode, remote debuggers can connect to the running application on port 5005.
In order to wait for a debugger to connect, pass the `-Dsuspend` option.

### Packaging and running the application

The application can be packaged using:

```shell script
./mvnw package
```

It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.