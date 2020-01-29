<?php

namespace Spartaques\CoreKafka\Consume\HighLevel;

use KafkaConsumeException;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\KafkaRebalanceCbException;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

class Consume
{
    protected $consumer;

    protected $instantiated = false;


    public function instantiate(ConsumeParamObject $object)
    {
        if($this->instantiated) {
            return $this;
        }

        $this->consumer = $this->instantiateConsumer($object);
        $metadata = $this->consumer->getMetadata(true, null, 100);
        $brokers = $metadata->getBrokers();
        if(count($brokers) < 1 ) {
            throw new \KafkaBrokerException();
        }
        $this->instantiated = true;

        return $this;
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

    private function instantiateConsumer(ConsumeParamObject $object): KafkaConsumer
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
}
