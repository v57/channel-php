<?php
declare(strict_types=1);

namespace ChannelPhp\Transports;

use ChannelPhp\ConnectionInterface;
use ChannelPhp\Internal\JsonCodec;
use ChannelPhp\Internal\WebSocketFrameReader;
use ChannelPhp\Internal\WebSocketProtocolException;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Client as CoClient;

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

    /** Connected socket (OpenSwoole\Coroutine\Client) once the handshake succeeded. */
    private ?object $ws = null;

    /** Largest message accepted from the server: 16 MiB, the TS/Bun default. */
    public const DEFAULT_MAX_PAYLOAD = 16 * 1024 * 1024;
    private const MAX_HANDSHAKE_BYTES = 64 * 1024;

    // For the connection attempt that just ended (read from the close hook): whether it got past
    // the handshake, and why it failed (null for a plain close or an unreachable address).
    private bool $wasOpen = false;
    private int $attempts = 0;
    private ?string $closeError = null;

    // Writes are serialized: a coroutine that finds another one mid-write queues its frame.
    private bool $writing = false;
    /** @var list<string> */
    private array $writeQueue = [];

    private mixed $onOpen = null;
    private mixed $onClose = null;
    private mixed $onMessage = null;

    public function __construct(
        private readonly string $address,
        private readonly mixed $headersProvider = null,
        private readonly int $maxPayload = self::DEFAULT_MAX_PAYLOAD,
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

    public function wasOpen(): bool
    {
        return $this->wasOpen;
    }

    public function closeError(): ?string
    {
        return $this->closeError;
    }

    /** Forget queued requests matching $predicate (e.g. ones that were just failed). */
    public function dropPending(callable $predicate): void
    {
        foreach ($this->pending as $id => $body) {
            if (is_array($body) && $predicate($body)) {
                unset($this->pending[$id]);
            }
        }
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
            $this->writeFrame(WebSocketFrameReader::encode(JsonCodec::encode($body)));
        } catch (\Throwable) {
        }
    }

    private function writeFrame(string $frame): void
    {
        if ($this->writing) {
            $this->writeQueue[] = $frame;
            return;
        }

        $this->writing = true;
        try {
            $ws = $this->ws;
            $data = $frame;
            while ($ws !== null) {
                if ($ws->send($data) === false) {
                    $this->writeQueue = [];
                    break;
                }
                if ($this->writeQueue === []) {
                    break;
                }
                $data = implode('', $this->writeQueue);
                $this->writeQueue = [];
            }
        } finally {
            $this->writing = false;
        }
    }

    /**
     * Microseconds before reconnect attempt $attempt (0 = first retry): 100 ms doubling up to 5 s, scaled to
     * 50-100% by $jitter (0..1) so clients don't retry in lockstep. Same numbers as the TypeScript client.
     */
    public static function reconnectDelay(int $attempt, float $jitter): int
    {
        return (int) (min(5_000_000, 100_000 * 2 ** min($attempt, 16)) * (0.5 + $jitter / 2));
    }

    private function runLoop(): void
    {
        while ($this->isRunning) {
            $this->wasOpen = false;
            $this->closeError = null;
            try {
                $this->connectAndRun();
            } catch (\Throwable $error) {
                $this->closeError ??= $error->getMessage() !== '' ? $error->getMessage() : get_class($error);
            }
            $this->isConnected = false;
            $this->ws = null;
            $this->writeQueue = [];

            if ($this->onClose !== null) {
                ($this->onClose)();
            }

            if ($this->isRunning) {
                Coroutine::usleep(self::reconnectDelay($this->attempts++, mt_rand() / mt_getrandmax()));
            }
        }
    }

    private function connectAndRun(): void
    {
        $parts = parse_url($this->address);
        if ($parts === false || !isset($parts['host'])) {
            $this->isRunning = false;
            $this->closeError = 'invalid address: ' . $this->address;
            return;
        }

        $scheme = strtolower((string) ($parts['scheme'] ?? 'ws'));
        $host = (string) $parts['host'];
        $port = (int) ($parts['port'] ?? ($scheme === 'wss' ? 443 : 80));
        $path = (string) ($parts['path'] ?? '/');
        if (isset($parts['query'])) {
            $path .= '?' . $parts['query'];
        }

        $type = SWOOLE_SOCK_TCP;
        if ($scheme === 'wss') {
            if (!defined('SWOOLE_SSL')) {
                $this->isRunning = false;
                $this->closeError = 'wss:// needs OpenSwoole built with OpenSSL';
                return;
            }
            $type |= constant('SWOOLE_SSL');
        }

        $client = new CoClient($type);
        if (!$client->connect($host, $port, 1.0)) {
            return; // not reachable (yet): pending requests are kept and re-sent on connect
        }

        try {
            $reader = new WebSocketFrameReader($this->maxPayload);
            if (!$this->handshake($client, $host, $port, $path, $reader)) {
                return;
            }

            $this->ws = $client;
            $this->isConnected = true;
            $this->wasOpen = true;
            $this->attempts = 0;

            if ($this->onOpen !== null) {
                ($this->onOpen)();
            }

            if ($this->pending !== []) {
                $this->sendNow(array_values($this->pending));
            }

            $this->readLoop($client, $reader);
        } finally {
            try {
                $client->close();
            } catch (\Throwable) {
            }
        }
    }

    private function handshake(object $client, string $host, int $port, string $path, WebSocketFrameReader $reader): bool
    {
        $key = base64_encode(random_bytes(16));
        $request = "GET {$path} HTTP/1.1\r\n"
            . "Host: {$host}:{$port}\r\n"
            . "Upgrade: websocket\r\n"
            . "Connection: Upgrade\r\n"
            . "Sec-WebSocket-Key: {$key}\r\n"
            . "Sec-WebSocket-Version: 13\r\n";
        foreach ($this->resolveHeaders() as $name => $value) {
            $request .= "{$name}: {$value}\r\n";
        }
        $request .= "\r\n";

        if ($client->send($request) === false) {
            return false;
        }

        $response = '';
        $deadline = microtime(true) + 5.0;
        while (($end = strpos($response, "\r\n\r\n")) === false) {
            if (strlen($response) > self::MAX_HANDSHAKE_BYTES || microtime(true) > $deadline) {
                $this->closeError = 'websocket handshake failed: no valid response';
                return false;
            }
            $chunk = $client->recv(1.0);
            if ($chunk === false && (int) $client->errCode === SOCKET_ETIMEDOUT) {
                continue;
            }
            if (!is_string($chunk) || $chunk === '') {
                // Closed before answering: treated like an unreachable address (retry).
                return false;
            }
            $response .= $chunk;
        }

        $head = substr($response, 0, $end);
        $lines = explode("\r\n", $head);
        if (preg_match('#^HTTP/1\.[01] (\d{3})#', $lines[0] ?? '', $match) !== 1) {
            $this->closeError = 'websocket handshake failed: invalid HTTP response';
            return false;
        }
        if ($match[1] !== '101') {
            $this->closeError = 'websocket handshake failed: HTTP ' . $match[1];
            return false;
        }

        $accept = null;
        foreach (array_slice($lines, 1) as $line) {
            $colon = strpos($line, ':');
            if ($colon !== false && strtolower(trim(substr($line, 0, $colon))) === 'sec-websocket-accept') {
                $accept = trim(substr($line, $colon + 1));
            }
        }
        $expected = base64_encode(sha1($key . '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', true));
        if ($accept !== $expected) {
            $this->closeError = 'websocket handshake failed: invalid Sec-WebSocket-Accept';
            return false;
        }

        $reader->push((string) substr($response, $end + 4));
        return true;
    }

    private function readLoop(object $client, WebSocketFrameReader $reader): void
    {
        while ($this->isRunning) {
            try {
                while (($frame = $reader->next()) !== null) {
                    if (!$this->handleFrame($frame)) {
                        return;
                    }
                }
            } catch (WebSocketProtocolException $error) {
                $this->closeError = 'connection closed: ' . $error->closeCode . ' ' . $error->getMessage();
                $this->sendClose($error->closeCode);
                return;
            }

            $chunk = $client->recv(1.0);
            if ($chunk === false) {
                if ((int) $client->errCode === SOCKET_ETIMEDOUT) {
                    continue;
                }
                return;
            }
            if ($chunk === '') {
                return; // peer closed the socket
            }
            $reader->push($chunk);
        }
    }

    /** @param array{opcode:int, data:string} $frame */
    private function handleFrame(array $frame): bool
    {
        switch ($frame['opcode']) {
            case WebSocketFrameReader::OPCODE_CLOSE:
                $code = strlen($frame['data']) >= 2 ? unpack('n', $frame['data'])[1] : 1005;
                if ($code !== 1000 && $code !== 1001 && $code !== 1005) {
                    $this->closeError = 'connection closed: ' . $code;
                }
                $this->sendClose(1000);
                return false;
            case WebSocketFrameReader::OPCODE_PING:
                $this->writeFrame(WebSocketFrameReader::encode($frame['data'], WebSocketFrameReader::OPCODE_PONG));
                return true;
            case WebSocketFrameReader::OPCODE_PONG:
                return true;
        }

        // Text, or binary treated as text (unchanged behaviour).
        if ($frame['data'] === '') {
            return true;
        }

        try {
            $value = JsonCodec::decode($frame['data']);
        } catch (\Throwable $error) {
            // Same as TS: a peer that sends invalid JSON gets disconnected; pending calls fail
            // with this error (see ClientTransport) instead of waiting forever.
            $this->closeError = 'invalid message from server: ' . $error->getMessage();
            $this->sendClose(1003);
            return false;
        }

        if ($this->onMessage !== null) {
            ($this->onMessage)($value);
        }
        return true;
    }

    private function sendClose(int $code): void
    {
        try {
            $this->writeFrame(WebSocketFrameReader::encode(pack('n', $code), WebSocketFrameReader::OPCODE_CLOSE));
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
            if (!is_string($key) || $value === null || $value === '' || !is_scalar($value)) {
                continue;
            }
            $value = (string) $value;
            // Refuse header injection: names are tokens, values have no CR/LF.
            if (preg_match('/^[!#$%&\'*+.^_`|~0-9A-Za-z-]+$/', $key) !== 1 || strpbrk($value, "\r\n") !== false) {
                continue;
            }
            $result[$key] = $value;
        }

        return $result;
    }
}
