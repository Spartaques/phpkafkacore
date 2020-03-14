# phpkafkacore

This library is a *pure PHP* wrapper for https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/book.rdkafka.html. It's been tested against https://kafka.apache.org/.

The library was used for the PHP projects to simplify work and handle common use cases.

## Project Maintainers

https://github.com/Spartaques

## Supported Kafka Versions

Kafka version >= [0.9.0.0 ](https://github.com/apache/kafka/releases/tag/0.9.0.0) 

## Setup

Ensure you have [composer](http://getcomposer.org/) installed, then run the following command:

```php
composer require spartaques/phpcorekafka
```



# Topics

There are some things, that you should know before creating a topic for your application.

1. Message ordering
2. Replication factor
3. Count of partitions

About ordering: **any events that need to stay in a fixed order must go in the same topic** (and they must also use the same partitioning key). So, as a rule of thumb, we could say that all events about the same entity need to go in the same topic. Partition key described below.

Replication factor should be used for achieving fault tolerance. 

Partitions is scaling unit in kafka, so we must know our data and calculate proper number.

# Producing

The most important things to know about producing is:

1. mode (sync, async, fire & forget)?
2. configurations
3. messages order
4.  payload schema.

1. Because kafka works in async mode by default, we can loose some messages if smth went wrong with broker. To avoid this, we can simply wait for response from broker.

examples/producesync.php describes sync producing. We simply use timeout for produce message, that means we wait for response from server.

examples/produce_async.php describes async producing. We just use 0 as timeout value for poll.

Dont forget to call flush() to be sure that all events are published when using async mode. It's related to php because php dies after each request, so some events might lost.

2. We should understand what we need from producer and broker. All things can be configured for best result.

The most important configuration parameters for producer:

***acks** - The acks parameter controls how many partition replicas must receive the record before the producer can consider the write successful. This option has a significant impact on how likely messages are to be lost.* 

***buffer.memory** - This sets the amount of memory the producer will use to buffer messages waiting to be sent to brokers.*

***compression.type** - By default, messages are sent uncompressed. This parameter can be set to snappy, gzip, or lz4, in which case the corresponding compression algorithms will be used to compress the data before sending it to the brokers.*

***retries** - When the producer receives an error message from the server, the error could be transient (e.g., a lack of leader for a partition). In this case, the value of the retries parameter will control how many times the producer will retry sending the message before giving up and notifying the client of an issue.*

*etc...*

3. Kafka use ordering strategy (partitioner) to deliver messages to partitions depends on use case. https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md describes partitioner parameter that handle this.

example: examples/produceorder.php

*Always use RD_KAFKA_PARTITION_UA as partition number. Kafka will care about everything.*

In most of the situations, ordering is not important. But if we, for example, use kafka for storing client, fact that client address info came before general info is not appropriate.

So, in such situations, we have 2 choise:

1. Use only 1 partition and 1 consumer, so messages will be handled in one order.
2. Use many partitions and consumers, but messages related to some entity should always go to one partition. Kafka consistent hashing and partitioner handle for us this case by default. (Using Rabbitmq for example, you should write this logic yourself).

4. Message payload schema should be clean and simple, and contain only data that are used.

For more advanced using https://avro.apache.org/ serializer should be used.



# Consuming

1. Committing

2. rebalancing 
3. configurations

1. Depend on data that should be processed, we can choose what behaviour is appropriate for us.

If duplication or loosing is not a problem, automatic commit (that works by default) is appropriate for us.

When we want to avoid such behaviour, we should use manual commit.

Manual commit works only when **enable.auto.commit** is set to false, and have 2 mode:

1) Synchronous - commit last offset after processing message.    example: examples/consumesynccommit.php

2) Async - non-blocking commit last offset after processing message. example: examples/consume_async_commit.php

3) Autocommit (by default) - examples/consumeautocommit.php

2. Rebalancing is a process of reassigning partitions to available consumers. It starts by consumers leader when it not receive heartbeats by one of the consumers after some period. When we use manual commit mode, we should commit offset before rebalancing starts.

Example: src/Common/DefaultCallbacks.php ,  syncRebalance().

3. configurations

most important configuration parameters:

**enable.auto.commit** - This parameter controls whether the consumer will commit offsets automatically, and defaults to true.

**fetch.min.bytes** - This property allows a consumer to specify the minimum amount of data that it wants to receive from the broker when fetching records. 

**fetch.max.wait.ms** - By setting fetch.min.bytes, you tell Kafka to wait until it has enough data to send before responding to the consumer. 

**max.partition.fetch.bytes** - This property controls the maximum number of bytes the server will return per parti‐ tion. The default is 1 MB.

**session.timeout.ms** - The amount of time a consumer can be out of contact with the brokers while still considered alive defaults to 3 seconds.

**auto.offset.reset** - This property controls the behavior of the consumer when it starts reading a partition for which it doesn’t have a committed offset or if the committed offset it has is invalid (usually because the consumer was down for so long that the record with that offset was already aged out of the broker). 

**max.poll.records** - This controls the maximum number of records that a single call to poll() will return.

receive.buffer.bytes and send.buffer.bytes - These are the sizes of the TCP send and receive buffers used by the sockets when writing and reading data.

For more info: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md



# Using in Production

There are 2 problems that should be handled:

1. kafka
2. server where code works.

In 1 situation, we must handle all errors, log and analyse. For this purpose we can register callbacks 

And push some notifications using custom callback. Example: 

src/Common/DefaultCallbacks.php error() method.

In 2 sutiation, we should use some process manager aka **supervisor** for monitoring our processes, and be confidence that our consumers handle signals and exit (close connections) gracefully. 

# Using with frameworks

This library is framework agnostic.

