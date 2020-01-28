<?php


namespace App\Produce;


class ProducerParamObject
{
    public $topicName;

    public $kafkaConf;

    public $topicConf;

    public function __construct(string $topicName,array $kafkaConf,array $topicConf)
    {
        $this->topicName = $topicName;
        $this->kafkaConf = $kafkaConf;
        $this->topicConf = $topicConf;
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
}
