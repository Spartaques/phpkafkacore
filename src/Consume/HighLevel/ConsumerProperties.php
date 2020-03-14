<?php

namespace Spartaques\CoreKafka\Consume\HighLevel;

use Spartaques\CoreKafka\Common\CallbacksCollection;

/**
 * Class ConsumerProperties
 * @package Spartaques\CoreKafka\Consume\HighLevel
 */
class ConsumerProperties
{
    /**
     * @var array
     */
    public $kafkaConf;
    /**
     * @var CallbacksCollection
     */
    private $callbacksCollection;


    /**
     * ConsumerProperties constructor.
     * @param array $kafkaConf
     * @param CallbacksCollection $callbacksCollection
     */
    public function __construct(array $kafkaConf, CallbacksCollection $callbacksCollection = null)
    {
        $this->kafkaConf = $kafkaConf;
        $this->callbacksCollection = $callbacksCollection;
    }

    /**
     * @return array
     */
    public function getKafkaConf(): array
    {
        return $this->kafkaConf;
    }

    /**
     * @return CallbacksCollection
     */
    public function getCallbacksCollection(): ?CallbacksCollection
    {
        return $this->callbacksCollection;
    }
}
