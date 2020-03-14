<?php


namespace Spartaques\CoreKafka\Produce;


/**
 * Class ProducerDataObject
 * @package Spartaques\CoreKafka\Produce
 */
class ProducerData
{
    /**
     * @var string
     */
    public $payload;

    /**
     * @var int
     */
    public $partition;

    /**
     * @var int
     */
    public $msgFlags;

    /**
     * @var string|null
     */
    public $messageKey;
    /**
     * @var array
     */
    private $headers;

    /**
     * ProducerDataObject constructor.
     * @param string $payload
     * @param int $partition
     * @param int $msgFlags
     * @param string|null $messageKey
     * @param array $headers
     */
    public function __construct(string $payload,int $partition = 0,int $msgFlags = 0, ?string $messageKey = null, array $headers = null)
    {
        $this->payload = $payload;
        $this->partition = $partition;
        $this->msgFlags = $msgFlags;
        $this->messageKey = $messageKey;
        $this->headers = $headers;
    }

    /**
     * @return string
     */
    public function getPayload(): string
    {
        return $this->payload;
    }

    /**
     * @return int
     */
    public function getPartition(): int
    {
        return $this->partition;
    }

    /**
     * @return int
     */
    public function getMsgFlags(): int
    {
        return $this->msgFlags;
    }

    /**
     * @return string|null
     */
    public function getMessageKey(): ?string
    {
        return $this->messageKey;
    }

    /**
     * @return array
     */
    public function getHeaders(): array
    {
        return $this->headers;
    }
}
