<?php

use Spartaques\CoreKafka\Common\CallbacksCollection;
use Spartaques\CoreKafka\Common\ConfigurationCallbacksKeys;
use Spartaques\CoreKafka\Common\DefaultCallbacks;
use Spartaques\CoreKafka\Produce\ProducerWrapper;
use Spartaques\CoreKafka\Produce\ProducerData;
use Spartaques\CoreKafka\Produce\ProducerProperties;

require 'vendor/autoload.php';

$producer = new ProducerWrapper();

$callbacksInstance = new DefaultCallbacks();


$collection = new CallbacksCollection(
    [
        ConfigurationCallbacksKeys::DELIVERY_REPORT => $callbacksInstance->delivery(),
        ConfigurationCallbacksKeys::ERROR => $callbacksInstance->error(),
        ConfigurationCallbacksKeys::LOG => $callbacksInstance->log(),
    ]);

// producer initialization object
$produceData = new ProducerProperties(
    'test123',
    [
        'metadata.broker.list' => 'kafka:9092',
        'client.id' => 'clientid',
    ],
    [
    ],
    $collection
);

for ($i = 0; $i < 1000000; $i++) {
    var_dump($i);

    // produce message using ProducerDataObject
    $producer->init($produceData)->produce(new ProducerData("Message $i", 0));
}

$producer->flush();

