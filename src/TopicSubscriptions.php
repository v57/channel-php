<?php
declare(strict_types=1);

namespace ChannelPhp;

final class TopicSubscriptions
{
    private int $id = 0;

    /** @var array<string, array<int, callable>> */
    private array $subscribed = [];

    public function addTopic(string $topic, callable $event): callable
    {
        $id = ++$this->id;
        if (!isset($this->subscribed[$topic])) {
            $this->subscribed[$topic] = [];
        }
        $this->subscribed[$topic][$id] = $event;

        return function () use ($topic, $id): bool {
            if (!isset($this->subscribed[$topic])) {
                return false;
            }

            unset($this->subscribed[$topic][$id]);
            if ($this->subscribed[$topic] !== []) {
                return false;
            }

            unset($this->subscribed[$topic]);
            return true;
        };
    }

    public function receivedEvent(string $topic, mixed $event): void
    {
        if (!isset($this->subscribed[$topic])) {
            return;
        }

        foreach ($this->subscribed[$topic] as $callback) {
            $callback($event);
        }
    }
}
