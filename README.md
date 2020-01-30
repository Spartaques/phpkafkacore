# phpkafkacore
kafka wrapper

## Table of Contents

1. [Introduction](#introduction)
2. [Test Usage](#test usage)
3. [Examples](#examples)
4. [Usage](#usage)
   * [Producing](#producing)
   * [Consuming](#high-level-consuming)
   * [Proper Shutdown](#proper-shutdown)
5. [Documentation](#documentation)
   
**Эта библиотека является оберткой на rdkafka, и позволяет более удобно использовать библиотеку без необходимости понимания
деталей работы rdkafka. Так же тут наведены примеры использования библиотеки с оптимальными настройками для определенных 
кейсов, которые рекомендуется использовать.**



Библиотека является независимой от фреймворков, что позволяет писать обертки.

Библиотеки решает следующие проблемы:
1) Возможность прокинуть внутрь любые конфигурации kafka. Но в примерах наведены самые оптимальные. Так же описаны самые важные настройки.
2) Framework-agnostic.
3) HighLevel консюмер и автоматическая ребалансировка консюмеров, что позволяет не заботиться о дублировании сообщений.
4) Возможность обрабатывать ошибки снаружи.

## Test Usage

Чтобы потестировать как это все работает можно запустить
docker-compose up
php produce.php
php consume.php

## Examples

### Producing

```php
$producer = new Produce();
// producer initialization object
$produceData = new ProducerParamObject(
    'test',
    [
            'metadata.broker.list' => 'kafka:9092',
            'client.id' => 'clientid'
    ],
    []
);

for ($i = 0; $i < 1; $i++) {
    // produce message using ProducerDataObject
    $producer->instantiate($produceData)->produce(new ProducerDataObject("Message $i",RD_KAFKA_PARTITION_UA));
}

$producer->flush(1000);
```
**ProducerParamObject** - обьект который содержит имя топика, масив конфигураций для продюсера и для топика (в большинстве случаев
используется только конфигурация продюсера).
Метод instantiate инициализирует подключение только один раз, после чего каждый вызов produce на этом обьекте будет использовать
существующее подключение.
**ProducerDataObject** - обьект, который содержит само сообщение - payload (JSON), а так же номер партиции (нужно использовать всегда
 **RD_KAFKA_PARTITION_UA** таким образом KAFKA будет равномерно распределять сообщения между существующими партициями).
 Метод flush используется для того чтобы доставить сообщения из внутренней очереди в kafka.

(Опционально)
Доступные опции:
**client.id** лучше указывать всегда поскольку это позволяет идентифицировать клиента (машину) который запродюсил сообщение в топик.
**max.in.flight.requests.per.connection** -  если нужен строгий порядок сообщений в случае когда происходит retry (сообщения могут потерять
порядок) стоит установить это значение в 0 (по умолчанию 1000000)  
Описание остальных опций доступно здесь:
1) https://docs.confluent.io/current/installation/configuration/producer-configs.html#cp-config-producer
2) https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
Описание того как работает продюсер:
https://docs.confluent.io/current/installation/configuration/producer-configs.html#cp-config-producer

### Consuming

```php
$consumer = new Consume();

$consumeDataObject = new ConsumeParamObject(
    ['group.id' => 'test',
        'metadata.broker.list' => 'kafka:9092',
        'auto.offset.reset' => 'smallest',
        ]
);

$consumer->instantiate($consumeDataObject)->consume(['oauthdata'],function ($message) {
    var_dump($message);
});
```
**ConsumeParamObject** - обьект, который содержит масив конфигураций для консюмера а так же коллбек функцию ребалансировки (опционально).
Метод **instantiate** инициализирует подключение только один раз. после чего каждый вызов consume на этом обьекте будет использовать
существующее подключение.
Метод **consume** содержит название топика а так же пользовательскую функцию - замыкание.
Данный пример использует опции по умолчанию. 

(Опционально)
Доступные опции:
**client.id** - то же что и для продюсера.
**group.id** - название группы, к которой принадлежит консюмер.
**session.timeout.ms** - время жизни сессии, после которого kafka coordinator делает ребалансировку (распределяет подключенные консюмеры по партициям).
ПО умолчанию 10 секунд.
heartbeat.interval.ms - как часто консюмер шлет тактовые испульсы в coordinator.
**enable.auto.commit** - включенный или отключенный автокоммит. При выключенном автокоммите, когда делается коммит каждого сообщения
вручную, надежность выше, но при этом падает производительность. Но при грамотной обработке сигналов завершения процесса можно 
добиться надежной работы и при включенном автокоммите (это описано чуть ниже).
**auto.commit.interval.ms** - как часто консюмер коммитит сообщения (чем чаще тем надежнее). ПО умолчанию 5 сек.
**auto.offset.reset** - определяет позицию в логе (offset) с которой читать когда нету закомиченной позиции (консюмер в группе первый раз подписался
на партицию)

Это были основные опции. Про все остальные можно почитать тут:
https://docs.confluent.io/current/installation/configuration/consumer-configs.html#
https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
Про то как работает консюмер можно почитать тут:
https://docs.confluent.io/current/clients/consumer.html 

### Proper Shutdown

Для того чтобы консюмер работал коректно в случае удаления процессов или перезапуска Docker контейнеров, нужно сделать обработку сигналов http://man7.org/linux/man-pages/man7/signal.7.html
Это уже сделано в самой библиотеке, поэтому об этом не надо беспокоиться.


Для того чтобы не потерять сообщения

## Documentation

