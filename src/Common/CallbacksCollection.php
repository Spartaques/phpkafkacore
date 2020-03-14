<?php


namespace Spartaques\CoreKafka\Common;


use Exception;
use Traversable;


class CallbacksCollection implements \Traversable, \IteratorAggregate
{
    protected $items;

    public function __construct(array $items)
    {
        foreach ($items as $key => $item) {
            if (!in_array($key, ConfigurationCallbacksKeys::CALLBACKS_MAP, true)) {
                throw new \RuntimeException('wrong key for callback');
            }
            $this->set($key, $item);
        }
    }

    public function set( string $key, \Closure $callback)
    {
        $this->items[$key] = $callback;
    }

    public function get($key): \Closure
    {
        return $this->items[$key];
    }

    /**
     * @inheritDoc
     */
    public function getIterator()
    {
        return new \ArrayIterator($this->items);
    }
}
