<?php


namespace Spartaques\CoreKafka\Common;


class ConfigurationCallbacksKeys
{
    //The consume callback is used with RdKafka::Consumer::consume_callback() methods and will be called for each consumed message.
    const CONSUME = 'consume';

    //The delivery report callback will be called once for each message accepted by RdKafka::Producer::produce()
    // (et.al) with RdKafka::Message::err() set to indicate the result of the produce request.
    //The callback is called when a message is succesfully produced or if librdkafka encountered a permanent failure,
    // or the retry counter for temporary errors has been exhausted.
    //An application must call RdKafka::poll() at regular intervals to serve queued delivery report callbacks.
    const DELIVERY_REPORT = 'delivery_report';

    const ERROR = 'error';

    const LOG = 'log';

    //Set offset commit callback for use with consumer groups.
    //
    //The results of automatic or manual offset commits will be scheduled for this callback and is served by RdKafka::KafkaConsumer::consume().
    //
    //If no partitions had valid offsets to commit this callback will be called with err == ERR__NO_OFFSET which is not to be considered an error.
    const OFFSET_COMMIT = 'offset_commit';

    const REBALANCE = 'rebalance';

    const STATISTICS = 'statistics';

    const CALLBACKS_MAP = [
        self::STATISTICS,
        self::REBALANCE,
        self::OFFSET_COMMIT,
        self::LOG,
        self::ERROR,
        self::DELIVERY_REPORT,
        self::CONSUME
    ];
}
