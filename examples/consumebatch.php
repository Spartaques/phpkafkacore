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
        'auto.offset.reset' => 'latest',
        'auto.commit.interval.ms' => 1000,
        'message.max.bytes' => 15729152,
//        'enable.auto.commit' => "false",
//        'auto.commit.interval.ms' => 0
        'log_level' => 6
    ],
    $collection
);


// ітак, питання в тому як передати обєкт  ConsumerWrapper у всі коллбеки?
$consumer->initOld('kafka:9092', $consumeDataObject)->consumeBatch('test123', 0, 5000, 10000, function (array $messages, ConsumerWrapper $consumer) {
    var_dump(microtime(true). ' | '.count($messages));
});
