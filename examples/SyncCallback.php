<?php

namespace Spartaques\CoreKafka\Examples;

use RdKafka\Message;
use Spartaques\CoreKafka\Consume\HighLevel\ConsumerWrapper;

class SyncCallback implements \Spartaques\CoreKafka\Consume\HighLevel\Contracts\Callback
{

    public function callback(\RdKafka\Message $message, ConsumerWrapper $consumer)
    {
        $consumer->commitSync();
    }
}
