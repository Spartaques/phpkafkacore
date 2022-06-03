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

$json = json_encode([
   'sfqsf' => 'fwqdfwfqwfqwf',
   1 => 'fwqdfwfqwfqwf' ,
   2 => 'fwqdfwfqwfqwf' ,
   3 => 'fwqdfwfqwfqwf' ,
   4 => 'fwqdfwfqwfqwf' ,
   5 => 'fwqdfwfqwfqwf' ,
   6 => 'fwqdfwfqwfqwf' ,
   7 => 'fwqdfwfqwfqwf' ,
   8 => 'fwqdfwfqwfqwf' ,
   9 => 'fwqdfwfqwfqwf'
]);

for ($i = 0; $i < 100; $i++) {
    var_dump($i);

    // produce message using ProducerDataObject
    $producer->init($produceData)->produce(new ProducerData($json, 0));
}

$producer->flush();

