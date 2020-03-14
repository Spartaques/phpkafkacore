<?php


namespace Spartaques\CoreKafka\Produce;


use Spartaques\CoreKafka\Common\CallbacksCollection;

class ProducerProperties
{
    /**
     * @var string
     */
    private $topicName;

    /**
     * @var array
     */
    private $kafkaConf;

    /**
     * @var array
     */
    private $topicConf;

    /**
     * @var CallbacksCollection
     */
    private $callbacksCollection;

    /**
     * ProducerProperties constructor.
     * @param string $topicName
     * @param array $kafkaConf
     * @param array $topicConf
     */
    public function __construct(string $topicName,array $kafkaConf,array $topicConf = [], CallbacksCollection $callbacksCollection = null)
    {
        $this->topicName = $topicName;
        $this->kafkaConf = $kafkaConf;
        $this->topicConf = $topicConf;
        $this->callbacksCollection = $callbacksCollection;
    }

    /**
     * @return string
     */
    public function getTopicName(): string
    {
        return $this->topicName;
    }

    /**
     * @return array
     */
    public function getKafkaConf(): array
    {
        return $this->kafkaConf;
    }

    /**
     * @return array
     */
    public function getTopicConf(): array
    {
        return $this->topicConf;
    }

    /**
     * @return CallbacksCollection
     */
    public function getCallbacksCollection(): ?CallbacksCollection
    {
        return $this->callbacksCollection;
    }
}
