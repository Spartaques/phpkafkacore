<?php


use Spartaques\CoreKafka\Common\CallbacksCollection;
use Spartaques\CoreKafka\Common\ConfigurationCallbacksKeys;
use Spartaques\CoreKafka\Common\DefaultCallbacks;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerProperties;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;
use Spartaques\CoreKafka\Examples\SyncCallback;

require 'vendor/autoload.php';

$callbacksInstance = new DefaultCallbacks();


$collection = new CallbacksCollection(
    [
        ConfigurationCallbacksKeys::CONSUME => $callbacksInstance->consume(),
        ConfigurationCallbacksKeys::DELIVERY_REPORT => $callbacksInstance->delivery(),
        ConfigurationCallbacksKeys::ERROR => $callbacksInstance->error(),
        ConfigurationCallbacksKeys::LOG => $callbacksInstance->log(),
        ConfigurationCallbacksKeys::OFFSET_COMMIT => $callbacksInstance->commit(),
        ConfigurationCallbacksKeys::REBALANCE => $callbacksInstance->rebalance(),
        ConfigurationCallbacksKeys::STATISTICS => $callbacksInstance->statistics(),
    ]
);

$consumer = new ConsumerWrapper();

$consumeDataObject = new ConsumerProperties(
    [
        'group.id' => 'test1',
        'client.id' => 'test',
        'metadata.broker.list' => 'kafka:9092',
        'auto.offset.reset' => 'latest',
        'enable.auto.commit' => "false",
//        'auto.commit.interval.ms' => 0
    ],
    $collection
);


// ітак, питання в тому як передати обєкт  ConsumerWrapper у всі коллбеки?
$consumer->init($consumeDataObject)->consume(['test123'], new SyncCallback());
