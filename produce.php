<?php

use Spartaques\CoreKafka\Produce\Produce;
use Spartaques\CoreKafka\Produce\ProducerDataObject;
use Spartaques\CoreKafka\Produce\ProducerParamObject;

require 'vendor/autoload.php';

$producer = new Produce();

$produceData = new ProducerParamObject(
    'hell',
    ['metadata.broker.list' => 'kafka:9092'],
    []
);

for ($i = 0; $i < 10000; $i++) {
    $producer->instantiate($produceData)->produce(new ProducerDataObject("Message $i",RD_KAFKA_PARTITION_UA));
}
        $producer->flush(10000);

