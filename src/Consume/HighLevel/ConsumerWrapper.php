<?php

declare(ticks=1); // PHP internal, make signal handling work

namespace Spartaques\CoreKafka\Consume\HighLevel;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use Spartaques\CoreKafka\Common\CallbacksCollection;
use Spartaques\CoreKafka\Common\ConfigurationCallbacksKeys;
use Spartaques\CoreKafka\Consume\HighLevel\Contracts\Callback;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\ConsumerShouldBeInstantiatedException;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\KafkaConsumeException;
use Spartaques\CoreKafka\Consume\HighLevel\VendorExtends\Output;
use Spartaques\CoreKafka\Exceptions\KafkaBrokerException;

/**
 * Class ConsumerWrapper
 * @package Spartaques\CoreKafka\Consume\HighLevel
 */
class ConsumerWrapper
{
    /**
     * @var KafkaConsumer $consumer
     */
    protected $consumer;


    protected $output;

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

        $this->output = new Output();

        $this->output->comment('Consumer initialization...');

        $this->defineSignalsHandling();

        $this->consumer = $this->initConsumerConnection($consumerProperties, $consumerProperties->getCallbacksCollection());

        $metadata = $this->consumer->getMetadata(true, null, $connectionTimeout);

        $brokers = $metadata->getBrokers();
        if(count($brokers) < 1 ) {
            throw new KafkaBrokerException();
        }
        $this->instantiated = true;

        $this->output->comment('Consumer initialized');

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

        $this->output->info('Waiting for partition assignment... (make take some time when');
        $this->output->info('quickly re-joining the group after leaving it');

        while (true) {
            $message = $this->consumer->consume($timeout);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->callback($callback, $message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $this->output->info('No more messages; will wait for more');
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $this->output->info('Timed out');
                    break;
                default:
                    $this->output->error($message->err);
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
        $this->output->info('Waiting for partition assignment... (make take some time when');
        $this->output->info('quickly re-joining the group after leaving it');

        while (true) {
            $message = $this->consumer->consume($timeout);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->callback($callback, $message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $this->output->info('No more messages; will wait for more');
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $this->output->info('Timed out');
                    break;
                default:
                    $this->output->info($message->err);
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
     * @param ConsumerProperties $consumerProperties
     * @param CallbacksCollection $callbacksCollection
     * @return KafkaConsumer
     */
    public function initConsumerConnection(ConsumerProperties $consumerProperties, CallbacksCollection $callbacksCollection): KafkaConsumer
    {
        $kafkaConf = new Conf();

        foreach ($consumerProperties->getKafkaConf() as $key => $value) {
            $kafkaConf->set($key, $value);
        }

        /**
         * @var \Closure $callback
         */
        foreach ($callbacksCollection as $key => $callback) {
            switch ($key) {
                case ConfigurationCallbacksKeys::CONSUME: {$kafkaConf->setConsumeCb($callback->bindTo($this));} break;
                case ConfigurationCallbacksKeys::ERROR: {$kafkaConf->setErrorCb($callback->bindTo($this)); break;}
                case ConfigurationCallbacksKeys::LOG: {$kafkaConf->setLogCb($callback->bindTo($this)); break;}
                case ConfigurationCallbacksKeys::OFFSET_COMMIT: {$kafkaConf->setOffsetCommitCb($callback->bindTo($this)); break;}
                case ConfigurationCallbacksKeys::REBALANCE: {$kafkaConf->setRebalanceCb($callback->bindTo($this)); break;}
                case ConfigurationCallbacksKeys::STATISTICS: {$kafkaConf->setStatsCb($callback->bindTo($this)); break;}
            }
        }

        $this->output->comment('callbacks registered');

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
     * @param array|null $topic_partitions
     * @return $this
     * @throws ConsumerShouldBeInstantiatedException
     */
    public function assign( array $topic_partitions = null)
    {
        if(!$this->instantiated) {
            throw  new ConsumerShouldBeInstantiatedException();
        }

        $this->consumer->assign($topic_partitions);

        return $this;
    }

    /**
     * @return Output
     */
    public function getOutput(): Output
    {
        return $this->output;
    }

    /**
     *
     */
    public function close()
    {
        $this->output->info('Stopping consumer by closing connection');
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
    public function signalHandler(int $signalNumber): void
    {
        $this->output->error('Handling signal: #' . $signalNumber);

        switch ($signalNumber) {
            case SIGTERM:  // 15 : supervisor default stop
            case SIGQUIT:  // 3  : kill -s QUIT
                echo 'process closed with SIGQUIT'. PHP_EOL;
                $this->close();
                exit(1);
                break;
            case SIGINT:   // 2  : ctrl+c
                $this->output->warn('process closed with SIGINT');
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
    public function alarmHandler($signalNumber)
    {
        $this->output->warn("Handling alarm: # . $signalNumber. memory usage: ". memory_get_usage(true));
    }

    /**
     *
     */
    public function defineSignalsHandling():void
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
            $this->output->error('Unable to process signal.');
            exit(1);
        }
    }

    /**
     * @param $callback
     * @param \RdKafka\Message $message
     */
    public function callback($callback, \RdKafka\Message $message): void
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
