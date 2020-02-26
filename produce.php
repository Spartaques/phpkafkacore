<?php

use Spartaques\CoreKafka\Produce\ProducerWrapper;
use Spartaques\CoreKafka\Produce\ProducerDataObject;
use Spartaques\CoreKafka\Produce\ProducerParamObject;

require 'vendor/autoload.php';

$producer = new ProducerWrapper();


// producer initialization object
$produceData = new ProducerParamObject(
    'test',
    [
        'metadata.broker.list' => 'kafka:9092',
        'client.id' => 'clientid'
    ],
    []
);

for ($i = 0; $i < 1000; $i++) {
    // produce message using ProducerDataObject
    $producer->init($produceData)->produce(new ProducerDataObject("Message $i", RD_KAFKA_PARTITION_UA));
}

$producer->flush();

