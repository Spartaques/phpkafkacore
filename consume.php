<?php

use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerParamObject;

require 'vendor/autoload.php';

$consumer = new ConsumerWrapper();

$consumeDataObject = new ConsumerParamObject(
    ['group.id' => 'test',
        'client.id' => 'test',
        'metadata.broker.list' => 'kafka:9092',
        'auto.offset.reset' => 'smallest',
        ]
);

$consumer->init($consumeDataObject)->consume(['test'],function ($message) {
    var_dump($message);
});
