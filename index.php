<?php

use App\Produce\Produce;
use App\Produce\ProducerDataObject;
use App\Produce\ProducerParamObject;

require 'vendor/autoload.php';

$producer = new Produce();

$produceData = new ProducerParamObject(
    'oauthdata',
    ['metadata.broker.list' => 'kafka:9092'],
    [],
);

for ($i = 0; $i < 1000; $i++) {
    $producer->instantiate($produceData)->produce(new ProducerDataObject('{"name":"Andrew"}',0));
}
        $producer->flush();

