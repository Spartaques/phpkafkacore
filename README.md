# phpkafkacore
kafka wrapper

**Эта библиотека является оберткой на rdkafka, и позволяет более удобно использовать библиотеку без необходимости понимания
деталей работы rdkafka. Так же тут наведены примеры использования библиотеки с оптимальными настройками для определенных 
кейсов, которые рекомендуется использовать.**

Библиотека является независимой от фреймворков, что позволяет писать обертки.

Библиотеки решает следующие проблемы:
1) Возможность прокинуть внутрь любые конфигурации kafka. Но в примерах наведены самые оптимальные. Так же описаны самые важные настройки.
2) Framework-agnostic.
3) HighLevel консюмер и автоматическая ребалансировка консюмеров, что позволяет не заботиться о дублировании сообщений.
4) Возможность обрабатывать ошибки снаружи.


Пример использования Продюсера:
`
$producer = new Produce();


// producer initialization object
$produceData = new ProducerParamObject(
    'oauthdata',
    ['metadata.broker.list' => 'kafka:9092'],
    []
);

for ($i = 0; $i < 1; $i++) {
    // produce message using ProducerDataObject
    $producer->instantiate($produceData)->produce(new ProducerDataObject("Message $i",RD_KAFKA_PARTITION_UA));
}

$producer->flush(1000);

`
