<?php


namespace Spartaques\CoreKafka\Produce;

use RdKafka\Conf;
use RdKafka\Metadata;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\Topic;
use RdKafka\TopicConf;
use Spartaques\CoreKafka\Produce\Exceptions\KafkaProduceFlushNotImplementedException;
use Spartaques\CoreKafka\Produce\Exceptions\KafkaProduceFlushTimeoutException;

class Produce
{
    protected $producer;

    protected $topic;

    protected $instantiated = false;

    public function instantiate(ProducerParamObject $paramObject): self
    {
        if($this->instantiated) {
            return $this;
        }

        $this->producer =  $this->instantiateProducer($paramObject);
        $this->topic = $this->instantiateTopic($paramObject);
        $this->instantiated = true;

        return $this;
    }

    public function produce(ProducerDataObject $dataObject, $timeout = 0): self
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

    public function flush(int $ms = 100):void
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

        if(RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED === $result) {
            throw new KafkaProduceFlushNotImplementedException('Was unable to flush, messages might be lost!');
        }
    }

    /**
     * @param ProducerParamObject $paramObject
     * @return array
     */
    private function instantiateProducer(ProducerParamObject $paramObject): Producer
    {
        $kafkaConf = new Conf();

        foreach ($paramObject->getKafkaConf() as $key => $value) {
            $kafkaConf->set($key, $value);
        }

        return new Producer($kafkaConf);
    }

    private function instantiateTopic(ProducerParamObject $paramObject): Topic
    {
        $topicConf = new TopicConf();

        foreach ($paramObject->getTopicConf() as $key => $value) {
            $topicConf->set($key, $value);
        }

        return $this->producer->newTopic($paramObject->getTopicName(), $topicConf);
    }

    /**
     * @return bool
     */
    public function isInstantiated(): bool
    {
        return $this->instantiated;
    }
}
