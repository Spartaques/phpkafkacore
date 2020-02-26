<?php

declare(ticks=1); // PHP internal, make signal handling work

namespace Spartaques\CoreKafka\Consume\HighLevel;

use KafkaConsumeException;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\KafkaRebalanceCbException;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use Spartaques\CoreKafka\Exceptions\KafkaBrokerException;

class ConsumerWrapper
{
    protected $consumer;

    protected $instantiated = false;

    public function init(ConsumerParamObject $object,$connectionTimeout = 1000): ConsumerWrapper
    {
        if($this->instantiated) {
            return $this;
        }

        $this->defineSignalsHandling();

        $this->consumer = $this->initConsumerConnection($object);

        $metadata = $this->consumer->getMetadata(true, null, $connectionTimeout);

        $brokers = $metadata->getBrokers();
        if(count($brokers) < 1 ) {
            throw new KafkaBrokerException();
        }

        $this->instantiated = true;

        return $this;
    }

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

    // An application should make sure to call consume() at regular intervals, even if no messages are expected, to serve any queued callbacks waiting to be called.
    // This is especially important when a rebalnce_cb has been registered as it needs to be called and handled properly to synchronize internal consumer state.
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

    private function errorCb():callable
    {
        return function ($kafka, $err, $reason) {
            throw new \KafkaConfigErrorCallbackException("Kafka error: %s (reason: %s)\n", rd_kafka_err2str($err), $reason);
        };
    }

    public function close()
    {
        echo 'Stopping consumer by closing connection.' . PHP_EOL;
        $this->consumer->close();
    }


    /**
     * @param array \RdKafka\TopicPartition[] $topicPartitions
     */
    public function getCommittedOffsets(array $topicPartitions, $timeoutMs = 100): array
    {
        return $this->consumer->getCommittedOffsets($topicPartitions, $timeoutMs);
    }

    public function getOffsetPositions(array $topicPartitions)
    {
        return $this->consumer->getOffsetPositions($topicPartitions);
    }

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
}
