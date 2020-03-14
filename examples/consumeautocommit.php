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
        ConfigurationCallbacksKeys::ERROR => $callbacksInstance->error(),
        ConfigurationCallbacksKeys::LOG => $callbacksInstance->log(),
        ConfigurationCallbacksKeys::REBALANCE => $callbacksInstance->rebalance(),
    ]
);

$consumer = new ConsumerWrapper();

$consumeDataObject = new ConsumerProperties(
    [
        'group.id' => 'test2',
        'client.id' => 'test',
        'metadata.broker.list' => 'kafka:9092',
        'auto.offset.reset' => 'smallest',
//        'enable.auto.commit' => "false",
//        'auto.commit.interval.ms' => 0
        'log_level' => 6
    ],
    $collection
);


// ітак, питання в тому як передати обєкт  ConsumerWrapper у всі коллбеки?
$consumer->init($consumeDataObject)->consume(['test123'], function (\RdKafka\Message $message, ConsumerWrapper $consumer) {
    var_dump($message);
});
