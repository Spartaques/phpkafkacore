<?php

use Spartaques\CoreKafka\Examples\SyncCallback;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerProperties;

require 'vendor/autoload.php';

$consumer = new ConsumerWrapper();

$consumeDataObject = new ConsumerProperties(
    ['group.id' => 'test',
        'client.id' => 'test',
        'metadata.broker.list' => 'kafka:9092',
        'auto.offset.reset' => 'smallest',
        'enable.auto.commit' => "false",
//        'auto.commit.interval.ms' => 0
        ]
);

$consumer->init($consumeDataObject)->consume(['test123'],new SyncCallback());
