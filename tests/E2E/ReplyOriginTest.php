<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\E2E;

use ChannelPhp\Channel;
use ChannelPhp\Sender;
use ChannelPhp\Tests\Support\CoroutineTestCase;
use ChannelPhp\Transports\RunningServer;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Socket;

/**
 * A server that forwards calls to a service it holds the sender of, like the Hub does. Request ids are one counter
 * for every connection, so another peer must not be able to answer what the server asked the service (audit C1).
 */
final class ReplyOriginTest extends CoroutineTestCase
{
    public function testAResponseFromAnotherConnectionAnswersNoForwardedCall(): void
    {
        $running = $this->relay();

        try {
            $this->co(function () use ($running): void {
                $released = false;
                $service = $this->service($released)->connect('ws://127.0.0.1:' . $running->port);
                $victim = (new Channel())->connect('ws://127.0.0.1:' . $running->port);

                try {
                    self::assertTrue($service->send('register'));

                    $result = null;
                    $finished = false;
                    go(static function () use ($victim, &$result, &$finished): void {
                        try {
                            $result = $victim->send('call', 'x');
                        } catch (\Throwable $error) {
                            $result = 'error: ' . $error->getMessage();
                        }
                        $finished = true;
                    });
                    Coroutine::usleep(100_000);

                    $attacker = self::attacker($running->port);
                    self::sendText($attacker, json_encode(array_map(static fn(int $id): array => ['id' => $id, 'body' => 'forged'], range(0, 255))));
                    self::sendText($attacker, json_encode(array_map(static fn(int $id): array => ['id' => $id, 'topic' => 'x', 'body' => 'forged'], range(0, 255))));
                    Coroutine::usleep(100_000);

                    $released = true;
                    self::waitUntil(static function () use (&$finished): bool {
                        return $finished;
                    });
                    self::assertSame('real', $result);
                    $attacker->close();
                } finally {
                    $victim->stop();
                    $service->stop();
                }
            });
        } finally {
            $running->stop();
        }
    }

    public function testAnotherConnectionCanNeitherFeedNorEndAForwardedStream(): void
    {
        $running = $this->relay();

        try {
            $this->co(function () use ($running): void {
                $released = false;
                $service = $this->service($released)->connect('ws://127.0.0.1:' . $running->port);
                $victim = (new Channel())->connect('ws://127.0.0.1:' . $running->port);

                try {
                    self::assertTrue($service->send('register'));

                    $values = [];
                    $finished = false;
                    go(static function () use ($victim, &$values, &$finished): void {
                        try {
                            foreach ($victim->values('watch') as $value) {
                                $values[] = $value;
                            }
                        } catch (\Throwable $error) {
                            $values[] = 'error: ' . $error->getMessage();
                        }
                        $finished = true;
                    });
                    Coroutine::usleep(100_000);

                    $attacker = self::attacker($running->port);
                    self::sendText($attacker, json_encode(array_map(static fn(int $id): array => ['id' => $id, 'body' => 'forged'], range(0, 255))));
                    self::sendText($attacker, json_encode(array_map(static fn(int $id): array => ['id' => $id, 'done' => true], range(0, 255))));
                    Coroutine::usleep(100_000);

                    $released = true;
                    self::waitUntil(static function () use (&$finished): bool {
                        return $finished;
                    });
                    self::assertSame(['real'], $values);
                    $attacker->close();
                } finally {
                    $victim->stop();
                    $service->stop();
                }
            });
        } finally {
            $running->stop();
        }
    }

    private function relay(): RunningServer
    {
        $service = null;
        $server = (new Channel())
            ->post('register', static function (array $body) use (&$service): bool {
                $service = $body['sender'];
                return true;
            })
            ->post('call', static function (array $body) use (&$service): mixed {
                /** @var Sender $service */
                return $service->send('svc/read', $body['body']);
            })
            ->stream('watch', static function () use (&$service): \Generator {
                /** @var Sender $service */
                foreach ($service->values('svc/values') as $value) {
                    yield $value;
                }
            });

        return $server->listen($this->allocatePort());
    }

    /** The service answers only once released, so the attacker's frames arrive while the server waits for it */
    private function service(bool &$released): Channel
    {
        return (new Channel())
            ->post('svc/read', static function () use (&$released): string {
                self::waitUntil(static function () use (&$released): bool {
                    return $released;
                });
                return 'real';
            })
            ->stream('svc/values', static function () use (&$released): \Generator {
                self::waitUntil(static function () use (&$released): bool {
                    return $released;
                });
                yield 'real';
            });
    }

    private static function waitUntil(callable $condition): void
    {
        for ($i = 0; $i < 1000 && !$condition(); $i++) {
            Coroutine::usleep(5_000);
        }
    }

    /** A raw WebSocket connection to the server, so it can send any frame */
    private static function attacker(int $port): Socket
    {
        $socket = new Socket(AF_INET, SOCK_STREAM, 0);
        self::assertTrue($socket->connect('127.0.0.1', $port, 2.0));
        $key = base64_encode(random_bytes(16));
        $socket->sendAll("GET / HTTP/1.1\r\nHost: 127.0.0.1:{$port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Key: {$key}\r\nSec-WebSocket-Version: 13\r\n\r\n");
        $response = '';
        while (!str_contains($response, "\r\n\r\n")) {
            $chunk = $socket->recv(65536, 2.0);
            self::assertIsString($chunk);
            self::assertNotSame('', $chunk);
            $response .= $chunk;
        }
        self::assertStringStartsWith('HTTP/1.1 101', $response);

        return $socket;
    }

    /** A masked text frame, as clients send them. The zero mask leaves the payload as it is. */
    private static function sendText(Socket $socket, string $payload): void
    {
        $length = strlen($payload);
        $header = "\x81" . ($length < 126 ? chr(0x80 | $length)
            : ($length < 65536 ? chr(0x80 | 126) . pack('n', $length) : chr(0x80 | 127) . pack('J', $length)));
        $socket->sendAll($header . "\0\0\0\0" . $payload);
    }

    private function allocatePort(): int
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if (!is_resource($server)) {
            throw new \RuntimeException('Failed to allocate local test port: ' . $errstr);
        }

        $name = stream_socket_get_name($server, false);
        fclose($server);

        if (!is_string($name) || !str_contains($name, ':')) {
            throw new \RuntimeException('Could not parse allocated port');
        }

        $parts = explode(':', $name);
        return (int) end($parts);
    }
}
