<?php

namespace App\Consume\HighLevel;

use KafkaConsumeException;
use KafkaRebalanceCbException;
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
        $this->instantiated = true;

        return $this;
    }

    public function consume(array $topics, callable  $callback, int $timeout = 120000)
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
}
