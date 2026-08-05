<?php
declare(strict_types=1);

namespace ChannelPhp;

final class Controller
{
    public function __construct(
        private readonly mixed $responseImpl,
        private readonly mixed $subscribeImpl,
        private readonly mixed $unsubscribeImpl,
        private readonly mixed $eventImpl,
        public readonly Sender $sender,
        public mixed $state,
    ) {
    }

    public function response(array $response): void
    {
        ($this->responseImpl)($response);
    }

    public function subscribe(string $topic): void
    {
        ($this->subscribeImpl)($topic);
    }

    public function unsubscribe(string $topic): void
    {
        ($this->unsubscribeImpl)($topic);
    }

    public function event(string $topic, mixed $event): void
    {
        ($this->eventImpl)($topic, $event);
    }
}
