<?php


namespace Spartaques\CoreKafka\Examples;


use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;
use Spartaques\CoreKafka\Consume\HighLevel\Contracts\Callback;

class AsyncHandledCallback implements Callback
{

    /**
     * @param \RdKafka\Message $message
     * @param ConsumerWrapper $consumerWrapper
     */
    public function callback(\RdKafka\Message $message, ConsumerWrapper $consumerWrapper)
    {
        try {
            var_dump($message->offset);
            $consumerWrapper->commitAsync();
        } finally {
            try {
                $consumerWrapper->commitSync();
            } finally {
                $consumerWrapper->close();
            }
        }
    }
}
