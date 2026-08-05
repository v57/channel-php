<?php
declare(strict_types=1);

namespace ChannelPhp;

use ChannelPhp\Transports\WebSocketClient;

final class ClientSender
{
    public function __construct(
        private readonly Sender $sender,
        public readonly WebSocketClient $ws,
    ) {
    }

    /** @return array<int, mixed> */
    public function streams(): array
    {
        return $this->sender->streams;
    }

    /** @return array<int, CancellableRequest> */
    public function requests(): array
    {
        return $this->sender->requests;
    }

    public function send(string $path, mixed $body = null, mixed $context = null): mixed
    {
        return $this->sender->send($path, $body, $context);
    }

    public function request(string $path, mixed $body = null, mixed $context = null): RequestHandle
    {
        return $this->sender->request($path, $body, $context);
    }

    public function values(string $path, mixed $body = null, mixed $context = null): Values
    {
        return $this->sender->values($path, $body, $context);
    }

    public function subscribe(string $path, mixed $body, callable $event, mixed $context = null): Cancellable
    {
        return $this->sender->subscribe($path, $body, $event, $context);
    }

    public function stop(): void
    {
        $this->sender->stop();
    }
}
