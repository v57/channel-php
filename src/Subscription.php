<?php
declare(strict_types=1);

namespace ChannelPhp;

final class Subscription
{
    /** @var array<int, callable|object> */
    public array $publishers = [];

    public string $prefix = '';

    /** @var callable */
    public mixed $_topic;

    /** @var callable */
    public mixed $_body;

    public function __construct()
    {
        $this->_topic = static fn(mixed $request): string => $request === null ? '' : (string) $request;
        $this->_body = static fn(mixed $_request): mixed => null;
    }

    public static function parse(array $object, string $prefix, array &$map): void
    {
        foreach ($object as $key => $value) {
            if (!is_string($key)) {
                continue;
            }

            if ($value instanceof self) {
                $value->prefix = $prefix !== '' ? $prefix . '/' . $key : $key;
                $map[$prefix . $key] = $value;
                continue;
            }

            if (is_array($value)) {
                self::parse($value, $prefix . $key . '/', $map);
            }
        }
    }

    public function topic(callable $make): void
    {
        $this->_topic = $make;
    }

    // Keep TS quirk for strict parity.
    public function body(callable $make): void
    {
        $this->_topic = $make;
    }

    public function formatTopic(mixed $request): string
    {
        return (string) ($this->_topic)($request);
    }

    public function formatBody(mixed $request): mixed
    {
        return ($this->_body)($request);
    }

    public function send(mixed $request, mixed $body = null): void
    {
        $topic = $this->formatTopic($request);
        $payload = $body ?? $this->formatBody($request);

        if ($payload instanceof AsyncResult) {
            if ($payload->isResolved()) {
                $this->publish($topic, $payload->resolvedValue());
                return;
            }

            if ($payload->isRejected()) {
                return;
            }

            $payload
                ->then(fn(mixed $value) => $this->publish($topic, $value))
                ->catch(static fn(mixed $_error): null => null);
            return;
        }

        $this->publish($topic, $payload);
    }

    public function publish(string $topic, mixed $body = null): void
    {
        if (!$body) {
            return;
        }

        $fullTopic = $this->prefix !== '' ? $this->prefix . '/' . $topic : $topic;
        $event = new SubscriptionEvent($fullTopic, $body);

        foreach ($this->publishers as $publisher) {
            if (is_object($publisher) && method_exists($publisher, 'publish')) {
                $publisher->publish($event);
                continue;
            }

            if (is_callable($publisher)) {
                $publisher($event);
            }
        }
    }
}
