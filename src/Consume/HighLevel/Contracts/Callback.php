<?php


namespace Spartaques\CoreKafka\Consume\HighLevel\Contracts;


use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;

/**
 * Interface Callback
 * @package Spartaques\CoreKafka\Consume\HighLevel\Contracts
 */
interface Callback
{
    /**
     * @param \RdKafka\Message $message
     * @param ConsumerWrapper $consumer
     * @return mixed
     */
    public function callback(\RdKafka\Message $message, ConsumerWrapper $consumer);
}
