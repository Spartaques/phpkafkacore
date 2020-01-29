# phpkafkacore
kafka wrapper

Эта библиотека является оберткой на rdkafka, и позволяет более удобно использовать библиотеку без необходимости понимания
деталей работы rdkafka. Так же тут наведены примеры использования библиотеки с оптимальными настройками для определенных кейсов.

Библиотека является независимой от фреймворков, что позволяет писать обертки.

Библиотеки решает следующие проблемы:
1) Одноразовая инициализация консюмера и продюсера вместо в коде.
2) Возможность прокинуть внутрь любые конфигурации kafka. Но в примерах наведены самые оптимальные. Так же описаны самые важные настройки.
3) Framework-agnostic.
4) HighLevel консюмер и автоматическая ребалансировка консюмеров, что позволяет не заботиться о дублировании сообщений.
5) Возможность обрабатывать ошибки снаружи.


