<?php
declare(strict_types=1);

namespace ChannelPhp;

interface ConnectionInterface
{
    public function send(array $body): mixed;

    public function sent(int $id): void;

    public function cancel(int $id): bool;

    public function notify(array $body): void;

    /** @return callable(): bool */
    public function addTopic(string $topic, callable $event): callable;

    public function stop(): void;
}
