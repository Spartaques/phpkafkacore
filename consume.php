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
    ]);

// ітак, як відбувається реєстрація? Я рєгаю колекцію і передаю її якщо хочу в ConsumerProperties. Потім уже іспользую внутрі якщо хочу!!!


$consumer = new ConsumerWrapper();

$consumeDataObject = new ConsumerProperties(
    ['group.id' => 'test',
        'client.id' => 'test',
        'metadata.broker.list' => 'kafka:9092',
        'auto.offset.reset' => 'smallest',
        'enable.auto.commit' => "false",
//        'auto.commit.interval.ms' => 0
        'log_level' => 6
        ],
    $collection
);


// ітак, питання в тому як передати обєкт  ConsumerWrapper у всі коллбеки?
$consumer->init($consumeDataObject)->consume(['test123'],new SyncCallback());
