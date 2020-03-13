<?php


namespace Spartaques\CoreKafka\Consume\HighLevel\Contracts;


use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;

interface Callback
{
    public function callback(\RdKafka\Message $message, ConsumerWrapper $consumer);
}
