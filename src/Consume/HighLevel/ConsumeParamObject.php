<?php

namespace Microfin\CoreKafka\Consume\HighLevel;

class ConsumeParamObject
{
    public $kafkaConf;
    /**
     * @var callable
     */
    private $rebalanceCbCallback;

    public function __construct(array $kafkaConf, callable $rebalanceCbCallback = null)
    {
        $this->kafkaConf = $kafkaConf;
        $this->rebalanceCbCallback = $rebalanceCbCallback;
    }

    /**
     * @return array
     */
    public function getKafkaConf(): array
    {
        return $this->kafkaConf;
    }

    /**
     * @return callable
     */
    public function getRebalanceCbCallback(): ?callable
    {
        return $this->rebalanceCbCallback;
    }
}
