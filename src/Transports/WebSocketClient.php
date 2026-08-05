<?php
declare(strict_types=1);

namespace ChannelPhp\Transports;

use ChannelPhp\ConnectionInterface;
use ChannelPhp\Internal\JsonCodec;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Http\Client as CoHttpClient;

final class WebSocketClient implements ConnectionInterface
{
    private int $id = 0;

    /** @var array<int, mixed> */
    private array $pending = [];

    /** @var array<string, array<int, callable>> */
    private array $subscribed = [];

    private int $topicId = 0;
    private int $isWaiting = 0;
    private int $isWaitingLength = 0;

    /** @var array<int, mixed> */
    private array $queue = [];

    private bool $isConnected = false;
    private bool $isRunning = true;

    private ?CoHttpClient $ws = null;

    private mixed $onOpen = null;
    private mixed $onClose = null;
    private mixed $onMessage = null;

    public function __construct(
        private readonly string $address,
        private readonly mixed $headersProvider = null,
    ) {
        if (Coroutine::getCid() < 0 || !function_exists('go')) {
            throw new \RuntimeException('WebSocketClient requires OpenSwoole coroutine context.');
        }

        go(fn() => $this->runLoop());
    }

    public function setOnOpen(?callable $handler): void
    {
        $this->onOpen = $handler;
    }

    public function setOnClose(?callable $handler): void
    {
        $this->onClose = $handler;
    }

    public function setOnMessage(?callable $handler): void
    {
        $this->onMessage = $handler;
    }

    public function isRunning(): bool
    {
        return $this->isRunning;
    }

    public function isConnected(): bool
    {
        return $this->isConnected;
    }

    public function send(array $body): int
    {
        $requestId = $this->id++;
        $this->pending[$requestId] = $body;

        if ($this->ws === null) {
            return $requestId;
        }

        if ($this->isWaiting === 0) {
            $this->sendNow($body);
            $this->isWaiting = 1;
            $this->isWaitingLength = 0;

            go(function (): void {
                Coroutine::usleep(10_000);
                if ($this->isWaitingLength > 4000) {
                    $this->isWaiting = 3;
                }
            });

            return $requestId;
        }

        if ($this->isWaiting === 1) {
            $this->sendNow($body);
            $this->isWaitingLength++;
            if ($this->isWaitingLength > 4000) {
                $this->isWaiting = 3;
            }
            return $requestId;
        }

        if ($this->isWaiting === 2) {
            $this->queue[] = $body;
            return $requestId;
        }

        if ($this->isWaiting === 3) {
            $this->sendNow($body);
            $this->isWaiting = 2;
            go(function (): void {
                Coroutine::usleep(1_000);
                $this->isWaiting = 3;
                if ($this->queue !== []) {
                    $payload = $this->queue;
                    $this->queue = [];
                    $this->sendNow($payload);
                }
            });
            return $requestId;
        }

        return $requestId;
    }

    public function sent(int $id): void
    {
        unset($this->pending[$id]);
    }

    public function cancel(int $id): bool
    {
        if ($this->isConnected) {
            return false;
        }

        if (!array_key_exists($id, $this->pending)) {
            return false;
        }

        unset($this->pending[$id]);
        return true;
    }

    public function notify(array $body): void
    {
        $this->sendNow($body);
    }

    public function addTopic(string $topic, callable $event): callable
    {
        $eventId = ++$this->topicId;
        $this->subscribed[$topic] ??= [];
        $this->subscribed[$topic][$eventId] = $event;

        return function () use ($topic, $eventId): bool {
            if (!isset($this->subscribed[$topic])) {
                return false;
            }

            unset($this->subscribed[$topic][$eventId]);
            if ($this->subscribed[$topic] !== []) {
                return false;
            }

            unset($this->subscribed[$topic]);
            return true;
        };
    }

    public function receivedEvent(string $topic, mixed $body): void
    {
        if (!isset($this->subscribed[$topic])) {
            return;
        }

        foreach ($this->subscribed[$topic] as $callback) {
            $callback($body);
        }
    }

    public function throttle(): void
    {
        $this->isWaiting = 3;
    }

    public function stop(): void
    {
        $this->isRunning = false;
        $this->pending = [];

        if ($this->ws !== null) {
            try {
                $this->ws->close();
            } catch (\Throwable) {
            }
        }
    }

    private function sendNow(mixed $body): void
    {
        if ($this->ws === null) {
            return;
        }

        try {
            $this->ws->push(JsonCodec::encode($body));
        } catch (\Throwable) {
        }
    }

    private function runLoop(): void
    {
        while ($this->isRunning) {
            $this->connectAndRun();
            $this->isConnected = false;
            $this->ws = null;

            if ($this->onClose !== null) {
                ($this->onClose)();
            }

            if ($this->isRunning) {
                Coroutine::usleep(100_000);
            }
        }
    }

    private function connectAndRun(): void
    {
        $parts = parse_url($this->address);
        if ($parts === false) {
            $this->isRunning = false;
            return;
        }

        $scheme = strtolower((string) ($parts['scheme'] ?? 'ws'));
        $host = (string) ($parts['host'] ?? '127.0.0.1');
        $port = (int) ($parts['port'] ?? ($scheme === 'wss' ? 443 : 80));
        $path = (string) ($parts['path'] ?? '/');
        if (isset($parts['query'])) {
            $path .= '?' . $parts['query'];
        }

        $client = new CoHttpClient($host, $port, $scheme === 'wss');
        $client->set(['timeout' => 1.0]);

        $headers = $this->resolveHeaders();
        if ($headers !== []) {
            $client->setHeaders($headers);
        }

        $ok = $client->upgrade($path);
        if (!$ok) {
            $statusCode = property_exists($client, 'statusCode') ? (int) $client->statusCode : 0;
            if ($statusCode === 101) {
                $this->isRunning = false;
            }
            $client->close();
            return;
        }

        $this->ws = $client;
        $this->isConnected = true;

        if ($this->onOpen !== null) {
            ($this->onOpen)();
        }

        if ($this->pending !== []) {
            $this->sendNow(array_values($this->pending));
        }

        while ($this->isRunning) {
            $frame = $client->recv(1.0);
            if ($frame === false) {
                $errCode = property_exists($client, 'errCode') ? (int) $client->errCode : 0;
                if ($errCode === SOCKET_ETIMEDOUT) {
                    continue;
                }
                break;
            }

            if (!is_object($frame) || !property_exists($frame, 'data')) {
                break;
            }

            if ($frame->data === '' || $frame->data === null) {
                continue;
            }

            try {
                $value = JsonCodec::decode((string) $frame->data);
                if ($this->onMessage !== null) {
                    ($this->onMessage)($value);
                }
            } catch (\Throwable) {
                break;
            }
        }

        try {
            $client->close();
        } catch (\Throwable) {
        }
    }

    /** @return array<string, string> */
    private function resolveHeaders(): array
    {
        if (!is_callable($this->headersProvider)) {
            return [];
        }

        $headers = ($this->headersProvider)();
        if (!is_array($headers)) {
            return [];
        }

        $result = [];
        foreach ($headers as $key => $value) {
            if (!is_string($key) || $value === null || $value === '') {
                continue;
            }
            $result[$key] = (string) $value;
        }

        return $result;
    }
}
