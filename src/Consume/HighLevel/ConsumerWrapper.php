<?php

declare(ticks=1); // PHP internal, make signal handling work

namespace Spartaques\CoreKafka\Consume\HighLevel;

use KafkaConsumeException;
use Spartaques\CoreKafka\Consume\HighLevel\Contracts\Callback;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\ConsumerShouldBeInstantiatedException;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\KafkaRebalanceCbException;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use Spartaques\CoreKafka\Exceptions\KafkaBrokerException;
use Symfony\Component\Console\Output\ConsoleOutput;

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
     * @var ConsoleOutput $output
     */
    private $output;

    /**
     * @var bool
     */
    protected $instantiated = false;

    /**
     * @param ConsumerProperties $consumerProperties
     * @param int $connectionTimeout
     * @return ConsumerWrapper
     * @throws ConsumerShouldBeInstantiatedException
     * @throws KafkaBrokerException
     */
    public function init(ConsumerProperties $consumerProperties,$connectionTimeout = 1000): ConsumerWrapper
    {
        if($this->instantiated) {
            return $this;
        }

        $this->output = new ConsoleOutput();

        $this->output->writeln('<comment>Consumer initialization...</comment>');

        $this->defineSignalsHandling();

        $this->consumer = $this->initConsumerConnection($consumerProperties);

        $metadata = $this->getMetadata(true, null, $connectionTimeout);

        $brokers = $metadata->getBrokers();
        if(count($brokers) < 1 ) {
            throw new KafkaBrokerException();
        }
        $this->instantiated = true;

        $this->output->writeln('<comment>Consumer Properties parsed </comment>');

        $this->output->writeln('<comment>Consumer initialized </comment>');

        return $this;
    }

    // An application should make sure to call consume() at regular intervals, even if no messages are expected, to serve any queued callbacks waiting to be called.
    // This is especially important when a rebalnce_cb has been registered as it needs to be called and handled properly to synchronize internal consumer state.

    /**
     * @param array $topics
     * @param $callback
     * @param int $timeout
     * @throws KafkaConsumeException
     */
    public function consume(array $topics,  $callback, int $timeout = 10000):void
    {
        $this->consumer->subscribe($topics);

        $this->output->writeln('<info>Waiting for partition assignment... (make take some time when</info>');
        $this->output->writeln('<info>quickly re-joining the group after leaving it</info>');

        while (true) {
            $message = $this->consumer->consume($timeout);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->callback($callback, $message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $this->output->writeln('<info>No more messages; will wait for more</info>');
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $this->output->writeln('<info>Timed out</info>');
                    break;
                default:
                    $this->output->writeln('<error>'.$message->err.'</error>');
                    throw new KafkaConsumeException($message->errstr(), $message->err);
                    break;
            }
        }
    }

    /**
     * @param $callback
     * @param int $timeout
     * @throws KafkaConsumeException
     */
    public function consumeWithManualAssign($callback, int $timeout = 10000): void
    {
        $this->output->writeln('<info>Waiting for partition assignment... (make take some time when</info>');
        $this->output->writeln('<info>quickly re-joining the group after leaving it</info>');

        while (true) {
            $message = $this->consumer->consume($timeout);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->callback($callback, $message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $this->output->writeln('<info>No more messages; will wait for more</info>');
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $this->output->writeln('<info>Timed out</info>');
                    break;
                default:
                    $this->output->writeln('<error>'.$message->err.'</error>');
                    throw new KafkaConsumeException($message->errstr(), $message->err);
                    break;
            }
        }
    }

    /**
     * @param mixed|null $message_or_offsets
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function commitSync($message_or_offsets = NULL):void
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
     */
    public function getMetadata(bool $all_topics , RdKafka\KafkaConsumerTopic $only_topic = NULL , int $timeout_ms = 1000): \RdKafka\Metadata
    {
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
     * @param ConsumerProperties $object
     * @return KafkaConsumer
     */
    private function initConsumerConnection(ConsumerProperties $object): KafkaConsumer
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

    public function assign( array $topic_partitions = null)
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        $this->consumer->assign($topic_partitions);

        return $this;
    }

    /**
     * @return callable
     */
    private function defaultRebalanceCb():callable
    {
        return function (KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $this->output->writeln('<info>Assign: </info>');
                    var_dump($partitions);
                    $kafka->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    $this->output->writeln('<error>Revoke: </error>');
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
        $this->output->writeln('<info>Stopping consumer by closing connection.</info>');
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
        $this->output->writeln('<error>'.'Handling signal: #' . $signalNumber.'</error>');

        switch ($signalNumber) {
            case SIGTERM:  // 15 : supervisor default stop
            case SIGQUIT:  // 3  : kill -s QUIT
                echo 'process closed with SIGQUIT'. PHP_EOL;
                $this->close();
                exit(1);
                break;
            case SIGINT:   // 2  : ctrl+c
                echo 'process closed with SIGINT'. PHP_EOL;
                $this->close();
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
            pcntl_signal(SIGTERM, [$this, 'signalHandler']);
            pcntl_signal(SIGHUP, [$this, 'signalHandler']);
            pcntl_signal(SIGINT, [$this, 'signalHandler']);
            pcntl_signal(SIGQUIT, [$this, 'signalHandler']);
            pcntl_signal(SIGUSR1, [$this, 'signalHandler']);
            pcntl_signal(SIGUSR2, [$this, 'signalHandler']);
            pcntl_signal(SIGALRM, [$this, 'alarmHandler']);
        } else {
            $this->output->writeln('<error>Unable to process signals.</error>');
            exit(1);
        }
    }

    /**
     * @param $callback
     * @param \RdKafka\Message $message
     */
    private function callback($callback, \RdKafka\Message $message): void
    {
        if($callback instanceof \Closure) {
            $callback($message, $this);
            return;
        }

        if($callback instanceof Callback) {
            /** @var Callback $instance */
            $callback->callback($message, $this);
            return;
        }

        throw new \RuntimeException('wrong instance');
    }
}
