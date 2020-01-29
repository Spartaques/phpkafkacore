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
```php
$producer = new Produce();
// producer initialization object
$produceData = new ProducerParamObject(
    'test',
    ['metadata.broker.list' => 'kafka:9092'],
    []
);

for ($i = 0; $i < 1; $i++) {
    // produce message using ProducerDataObject
    $producer->instantiate($produceData)->produce(new ProducerDataObject("Message $i",RD_KAFKA_PARTITION_UA));
}

$producer->flush(1000);
```
ProducerParamObject - обьект который содержит имя топика, масив конфигураций для продюсера и для топика (в большинстве случаев)
используется только конфигурация продюсера.
Метод instantiate инициализирует подключение только один раз, после чего каждый вызов produce на этом обьекте будет использовать
существующее подключение.
ProducerDataObject - обьект, который содержит само сообщение - payload (JSON), а так же номер партиции (нужно использовать всегда
 RD_KAFKA_PARTITION_UA таким образом KAFKA будет равномерно распределять сообщения между существующими партициями).
 Метод flush используется для того чтобы доставить сообщения из внутренней очереди в kafka, и принимает один аргумент - timeout в миллисекундах 
 за который сообщение должно быть доставлено.

`