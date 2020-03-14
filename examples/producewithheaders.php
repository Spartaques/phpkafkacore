
<?php

use Spartaques\CoreKafka\Produce\ProducerWrapper;
use Spartaques\CoreKafka\Produce\ProducerData;
use Spartaques\CoreKafka\Produce\ProducerProperties;

require 'vendor/autoload.php';

$producer = new ProducerWrapper();


// producer initialization object
$produceData = new ProducerProperties(
    'test123',
    [
        'metadata.broker.list' => 'kafka:9092',
        'client.id' => 'clientid'
    ],
    []
);

$headers = [
    'SomeKey' => 'SomeValue',
    'AnotherKey' => 'AnotherValue',
];

for ($i = 0; $i < 1; $i++) {
    // produce message using ProducerDataObject
    $producer->init($produceData)->produceWithHeaders(new ProducerData("Message $i", RD_KAFKA_PARTITION_UA, 0, null, $headers));
}

$producer->flush();

