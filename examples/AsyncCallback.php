<?php


namespace Spartaques\CoreKafka\Examples;


use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;

class AsyncCallback implements \Spartaques\CoreKafka\Consume\HighLevel\Contracts\Callback
{

    public function callback(\RdKafka\Message $message, ConsumerWrapper $consumerWrapper)
    {
        var_dump($message->offset);
        $consumerWrapper->commitAsync();
    }
}
