<?php

use Spartaques\CoreKafka\Consume\HighLevel\Consume;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumeParamObject;

require 'vendor/autoload.php';

$consumer = new Consume();

$consumeDataObject = new ConsumeParamObject(
    ['group.id' => 'test',
        'metadata.broker.list' => 'kafka:9092',
        'auto.offset.reset' => 'latest',
        ]
);

($consumer->instantiate($consumeDataObject)->consume(['hell'],function ($message) {
    var_dump($message);
}));
