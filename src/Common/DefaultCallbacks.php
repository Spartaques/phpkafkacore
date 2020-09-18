<?php


namespace Spartaques\CoreKafka\Common;


use RdKafka\Kafka;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\KafkaRebalanceCbException;

/**
 * Class DefaultCallbacks
 * @package Spartaques\CoreKafka\Common\HighLevel
 */
class DefaultCallbacks
{
    /**
     * @return \Closure
     */
    public function syncRebalance(): \Closure
    {
        return function (KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $this->getOutput()->info('<info>Assign: </info>');
                    var_dump($partitions);
                    $this->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:

                    $this->getOutput()->writeln('<error>Revoke: </error>');

                    var_dump($partitions);

                    $this->commitSync();

                    $this->getOutput()->error('offset commited:');
                    $this->assign(NULL);
                    break;

                default:
                    throw new KafkaRebalanceCbException($err);
            }
        };
    }

    /**
     * @return \Closure
     */
    public function rebalance() : \Closure
    {
        return function (KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $this->getOutput()->info('<info>Assign: </info>');
                    var_dump($partitions);
                    $this->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:

                    $this->getOutput()->writeln('<error>Revoke: </error>');

                    var_dump($partitions);

                    $this->assign(NULL);
                    break;

                default:
                    throw new KafkaRebalanceCbException($err);
            }
        };
    }

    /**
     * @return \Closure
     */
    public function consume(): \Closure
    {
        return function ($message) {
            $this->getOutput()->info('consume callback');
            var_dump($message);
        };
    }

    /**
     * @return \Closure
     */
    public function delivery(): \Closure
    {
        return function ($kafka, $message) {
            if ($message->err) {
                $this->getOutput()->warn('message permanently failed to be delivered');
            } else {
                $this->getOutput()->info('message successfully delivered');
                // message successfully delivered
            }
        };
    }

    /**
     * @return \Closure
     */
    public function error() : \Closure
    {
        return function ($kafka, $err, $reason) {
            $this->getOutput()->warn(sprintf("Kafka error: %s (reason: %s)\n", rd_kafka_err2str($err), $reason));
        };
    }

    /**
     * @return \Closure
     */
    public function log(): \Closure
    {
        return function ($kafka, $level, $facility, $message) {
            $this->getOutput()->warn(sprintf("Kafka %s: %s (level: %d)\n", $facility, $message, $level));
        };
    }

    /**
     * @return \Closure
     */
    public function commit(): \Closure
    {
        return function (KafkaConsumer $kafka, $err, array $partitions) {

            if($err === RD_KAFKA_RESP_ERR__NO_OFFSET) {
                return;
            }

            $text = 'commit callback. ';

            foreach ($partitions as $partition) {
                $text .= "partition # {$partition->getPartition()} . offset # {$partition->getOffset()}   |  ";
            }

            $this->getOutput()->info($text);
        };
    }

    /**
     * @return \Closure
     */
    public function statistics(): \Closure
    {
        return function ($kafka, $json, $json_len) {
            echo 'statistics';
        };
    }
}
