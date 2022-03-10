<?php


namespace Spartaques\CoreKafka\Produce;

use RdKafka\Conf;
use RdKafka\Metadata;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\Topic;
use RdKafka\TopicConf;
use Spartaques\CoreKafka\Common\CallbacksCollection;
use Spartaques\CoreKafka\Common\ConfigurationCallbacksKeys;
use Spartaques\CoreKafka\Consume\HighLevel\Exceptions\KafkaTopicNameException;
use Spartaques\CoreKafka\Exceptions\KafkaBrokerException;
use Spartaques\CoreKafka\Produce\Exceptions\KafkaProduceFlushNotImplementedException;
use Spartaques\CoreKafka\Produce\Exceptions\KafkaProduceFlushTimeoutException;

/**
 * Class ProducerWrapper
 * @package Spartaques\CoreKafka\Produce
 */
class ProducerWrapper
{
    /**
     * @var
     */
    protected $producer;

    /**
     * @var
     */
    protected $topic;

    /**
     * @var Conf $kafkaConf
     */
    protected $kafkaConf;

    /**
     * @var TopicConf $topicConf
     */
    protected $topicConf;

    /**
     * @var bool
     */
    protected $instantiated = false;

    /**
     * @param ProducerProperties $producerProperties
     * @return $this
     * @throws KafkaTopicNameException
     */
    public function init(ProducerProperties $producerProperties): self
    {
        if ($this->instantiated) {
            return $this;
        }

        $this->producer = $this->initProducer($producerProperties);

        $this->topic = $this->instantiateTopic($producerProperties);

        if($producerProperties->getCallbacksCollection() !== null) {
            $this->registerConfigurationCallbacks($this->kafkaConf, $producerProperties->getCallbacksCollection());
        }
        $this->instantiated = true;

        return $this;
    }

    /**
     * @param ProducerData $dataObject
     * @param int $timeout
     * @return $this
     */
    public function produce(ProducerData $dataObject, $timeout = 0): self
    {
        $this->topic->produce(
            $dataObject->getPartition(),
            $dataObject->getMsgFlags(),
            $dataObject->getPayload(),
            $dataObject->getMessageKey()
        );

        $this->producer->poll($timeout);

        return $this;
    }

    /**
     *
     */
    public function produceWithHeaders(ProducerData $dataObject, $timeout = 0)
    {
        $this->topic->producev(
            $dataObject->getPartition(),
            $dataObject->getMsgFlags(),
            $dataObject->getPayload(),
            $dataObject->getMessageKey(),
            $dataObject->getHeaders()
        );

        $this->producer->poll($timeout);

        return $this;
    }

    /**
     * @param int $ms
     * @throws KafkaProduceFlushNotImplementedException
     * @throws KafkaProduceFlushTimeoutException
     */
    public function flush(int $ms = 10000): void
    {
        for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
            $result = $this->producer->flush($ms);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }

        if (RD_KAFKA_RESP_ERR__TIMED_OUT === $result) {
            throw new KafkaProduceFlushTimeoutException('Flush timeout exception!!');
        }

        if (RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED === $result) {
            throw new KafkaProduceFlushNotImplementedException('Was unable to flush, messages might be lost!');
        }
    }

    /**
     * @param ProducerProperties $producerProperties
     * @return array
     */
    private function initProducer(ProducerProperties $producerProperties): Producer
    {
        $this->kafkaConf = new Conf();

        foreach ($producerProperties->getKafkaConf() as $key => $value) {
            $this->kafkaConf->set($key, $value);
        }

        return new Producer($this->kafkaConf);
    }

    /**
     * @param ProducerProperties $producerProperties
     * @return Topic
     * @throws KafkaTopicNameException
     */
    private function instantiateTopic(ProducerProperties $producerProperties): Topic
    {
        $this->topicConf = new TopicConf();

        foreach ($producerProperties->getTopicConf() as $key => $value) {
            $this->topicConf->set($key, $value);
        }

        if (empty($producerProperties->getTopicName())) {
            throw new KafkaTopicNameException();
        }

        return $this->producer->newTopic($producerProperties->getTopicName(), $this->topicConf);
    }

    /**
     * @return bool
     */
    public function isInstantiated(): bool
    {
        return $this->instantiated;
    }

    private function registerConfigurationCallbacks(Conf $conf, CallbacksCollection $callbacksCollection)
    {
        /**
         * @var \Closure $callback
         */
        foreach ($callbacksCollection as $key => $callback) {
            switch ($key) {
                case ConfigurationCallbacksKeys::DELIVERY_REPORT: { $conf->setDrMsgCb($callback->bindTo($this)); break;}
                case ConfigurationCallbacksKeys::ERROR: {$conf->setErrorCb($callback->bindTo($this)); break;}
                case ConfigurationCallbacksKeys::LOG: {$conf->setLogCb($callback->bindTo($this)); break;}
            }
        }
    }
}
