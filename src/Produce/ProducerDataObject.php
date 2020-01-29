<?php


namespace Microfin\CoreKafka\Produce;


class ProducerDataObject
{
    public $payload;

    public $partition;

    public $msgFlags;

    public $messageKey;

    public function __construct(string $payload,int $partition = 0,int $msgFlags = 0, ?string $messageKey = null)
    {
        $this->payload = $payload;
        $this->partition = $partition;
        $this->msgFlags = $msgFlags;
        $this->messageKey = $messageKey;
    }

    public function getPayload(): string
    {
        return $this->payload;
    }

    public function getPartition(): int
    {
        return $this->partition;
    }

    public function getMsgFlags(): int
    {
        return $this->msgFlags;
    }

    public function getMessageKey(): ?string
    {
        return $this->messageKey;
    }
}
