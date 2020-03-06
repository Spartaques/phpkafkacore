<?php

declare(ticks=1); // PHP internal, make signal handling work

namespace Spartaques\CoreKafka\Consume\HighLevel;

use KafkaConsumeException;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\ConsumerShouldBeInstantiatedException;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\KafkaRebalanceCbException;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use Spartaques\CoreKafka\Exceptions\KafkaBrokerException;

/**
 * Class ConsumerWrapper
 * @package Spartaques\CoreKafka\Consume\HighLevel
 */
class ConsumerWrapper
{
    /**
     * @var
     */
    protected $consumer;

    /**
     * @var bool
     */
    protected $instantiated = false;

    /**
     * @param ConsumerParamObject $object
     * @param int $connectionTimeout
     * @return ConsumerWrapper
     * @throws ConsumerShouldBeInstantiatedException
     * @throws KafkaBrokerException
     */
    public function init(ConsumerParamObject $object,$connectionTimeout = 1000): ConsumerWrapper
    {
        if($this->instantiated) {
            return $this;
        }

        $this->defineSignalsHandling();

        $this->consumer = $this->initConsumerConnection($object);

        $metadata = $this->getMetadata(true, null, $connectionTimeout);

        $brokers = $metadata->getBrokers();
        if(count($brokers) < 1 ) {
            throw new KafkaBrokerException();
        }

        $this->instantiated = true;

        return $this;
    }

    // An application should make sure to call consume() at regular intervals, even if no messages are expected, to serve any queued callbacks waiting to be called.
    // This is especially important when a rebalnce_cb has been registered as it needs to be called and handled properly to synchronize internal consumer state.
    /**
     * @param array $topics
     * @param callable $callback
     * @param int $timeout
     * @throws KafkaConsumeException
     */
    public function consume(array $topics, callable  $callback, int $timeout = 10000):void
    {
        $this->consumer->subscribe($topics);

        echo "Waiting for partition assignment... (make take some time when\n";
        echo "quickly re-joining the group after leaving it.)\n";

        while (true) {
            $message = $this->consumer->consume($timeout);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $callback($message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    var_dump($message->err);
                    throw new KafkaConsumeException($message->errstr(), $message->err);
                    break;
            }
        }
    }

    /**
     * @param mixed|null $message_or_offsets
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function commit($message_or_offsets = NULL):void
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        $this->consumer->commit($message_or_offsets);
    }

    /**
     * @param null $message_or_offsets
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function commitAsync($message_or_offsets = NULL):void
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        $this->consumer->commitAsync($message_or_offsets);
    }

    /**
     * @return array
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function getAssignment():array
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        return $this->consumer->getAssignment();
    }

    /**
     * @param array $topics
     * @param int $timeout_ms
     * @return array
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function getCommittedOffsets(array $topics , int $timeout_ms = 10000): array
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        return $this->consumer->getCommittedOffsets($topics, $timeout_ms);
    }

    /**
     * @param bool $all_topics
     * @param RdKafka\KafkaConsumerTopic|null $only_topic
     * @param int $timeout_ms
     * @return mixed
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function getMetadata(bool $all_topics , RdKafka\KafkaConsumerTopic $only_topic = NULL , int $timeout_ms = 1000): RdKafka\Metadata
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        return $this->consumer->getMetadata($all_topics, $only_topic, $timeout_ms);
    }

    /**
     * @return array
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function getSubscription(): array
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        return  $this->consumer->getSubscription();
    }

    /**
     * @param array $topicPartitions
     * @param int $timeout_ms
     * @return array
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function offsetsForTimes(array $topicPartitions , int $timeout_ms = 1000): array
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        return $this->consumer->offsetsForTimes($topicPartitions, $timeout_ms);
    }

    /**
     * @param string $topic
     * @param int $partition
     * @param int $low
     * @param int $high
     * @param int $timeout_ms
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function queryWatermarkOffsets(string $topic , int $partition , int &$low , int &$high , int $timeout_ms):void
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        $this->consumer->queryWatermarkOffsets($topic, $partition, $low, $high, $timeout_ms);
    }

    /**
     * @param array $topics
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function subscribe(array $topics) :void
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        $this->consumer->subscribe($topics);
    }


    /**
     *
     */
    public function unsubscribe():void
    {
        $this->consumer->unsubscribe();
    }

    /**
     * @param ConsumerParamObject $object
     * @return KafkaConsumer
     */
    private function initConsumerConnection(ConsumerParamObject $object): KafkaConsumer
    {
        $kafkaConf = new Conf();

        foreach ($object->getKafkaConf() as $key => $value) {
            $kafkaConf->set($key, $value);
        }

        $kafkaConf->setRebalanceCb($object->getRebalanceCbCallback() ?? $this->defaultRebalanceCb());

        $kafkaConf->setErrorCb($this->errorCb());

        return new KafkaConsumer($kafkaConf);
    }

    /**
     * @return bool
     */
    public function isInstantiated(): bool
    {
        return $this->instantiated;
    }

    /**
     * @return callable
     */
    private function defaultRebalanceCb():callable
    {
        return function (KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo "Assign: ";
                    var_dump($partitions);
                    $kafka->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    echo "Revoke: ";
                    var_dump($partitions);
                    $kafka->assign(NULL);
                    break;

                default:
                    throw new KafkaRebalanceCbException($err);
            }
        };
    }

    /**
     * @return callable
     */
    private function errorCb():callable
    {
        return function ($kafka, $err, $reason) {
            throw new \KafkaConfigErrorCallbackException("Kafka error: %s (reason: %s)\n", rd_kafka_err2str($err), $reason);
        };
    }

    /**
     *
     */
    public function close()
    {
        echo 'Stopping consumer by closing connection.' . PHP_EOL;
        $this->consumer->close();
    }


    /**
     * @param array $topicPartitions
     * @return mixed
     */
    public function getOffsetPositions(array $topicPartitions)
    {
        return $this->consumer->getOffsetPositions($topicPartitions);
    }

    /**
     * @param int $signalNumber
     */
    private function signalHandler(int $signalNumber): void
    {
        echo 'Handling signal: #' . $signalNumber . PHP_EOL;

        switch ($signalNumber) {
            case SIGTERM:  // 15 : supervisor default stop
            case SIGQUIT:  // 3  : kill -s QUIT
                echo 'process closed with SIGQUIT'. PHP_EOL;
                $this->consumer->close();
                exit(1);
                break;
            case SIGINT:   // 2  : ctrl+c
                echo 'process closed with SIGINT'. PHP_EOL;
                $this->consumer->close();
                exit(1);
                break;
            case SIGHUP:   // 1  : kill -s HUP
//                $this->consumer->restart();
                break;
            case SIGUSR1:  // 10 : kill -s USR1
                // send an alarm in 1 second
                pcntl_alarm(1);
                break;
            case SIGUSR2:  // 12 : kill -s USR2
                // send an alarm in 10 seconds
                pcntl_alarm(10);
                break;
            default:
                break;
        }
    }

    /**
     * Alarm handler
     *
     * @param  int $signalNumber
     * @return void
     */
    private function alarmHandler($signalNumber)
    {
        echo 'Handling alarm: #' . $signalNumber . PHP_EOL;

        echo memory_get_usage(true) . PHP_EOL;
    }

    /**
     *
     */
    private function defineSignalsHandling():void
    {
        if (extension_loaded('pcntl')) {
            define('AMQP_WITHOUT_SIGNALS', false);

            pcntl_signal(SIGTERM, [$this, 'signalHandler']);
            pcntl_signal(SIGHUP, [$this, 'signalHandler']);
            pcntl_signal(SIGINT, [$this, 'signalHandler']);
            pcntl_signal(SIGQUIT, [$this, 'signalHandler']);
            pcntl_signal(SIGUSR1, [$this, 'signalHandler']);
            pcntl_signal(SIGUSR2, [$this, 'signalHandler']);
            pcntl_signal(SIGALRM, [$this, 'alarmHandler']);
        } else {
            echo 'Unable to process signals.' . PHP_EOL;
            exit(1);
        }
    }
}
