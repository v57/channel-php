<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\E2E;

use ChannelPhp\Tests\Support\CoroutineTestCase;
use ChannelPhp\Transports\WebSocketClient;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Socket;

/** The client used to retry every 100 ms forever while the server was down (TS parity: channel docs/client.md). */
final class ReconnectBackoffTest extends CoroutineTestCase
{
    private const PORT = 6661;

    public function testReconnectDelayDoublesUpTo5Seconds(): void
    {
        $delays = array_map(static fn(int $attempt) => WebSocketClient::reconnectDelay($attempt, 1), range(0, 7));
        self::assertSame([100_000, 200_000, 400_000, 800_000, 1_600_000, 3_200_000, 5_000_000, 5_000_000], $delays);
        self::assertSame(50_000, WebSocketClient::reconnectDelay(0, 0));
        self::assertSame(2_500_000, WebSocketClient::reconnectDelay(1000, 0));
    }

    public function testClientBacksOffWhileTheServerRefusesTheUpgrade(): void
    {
        $this->co(function (): void {
            // Every request is refused with HTTP 500: each connection is one failed attempt.
            $server = new Socket(AF_INET, SOCK_STREAM, 0);
            $server->setOption(SOL_SOCKET, SO_REUSEADDR, 1);
            self::assertTrue($server->bind('127.0.0.1', self::PORT));
            self::assertTrue($server->listen(8));
            $attempts = 0;
            $running = true;
            go(static function () use ($server, &$attempts, &$running): void {
                while ($running) {
                    $connection = $server->accept(0.1);
                    if (!$connection instanceof Socket) {
                        continue;
                    }
                    $attempts++;
                    $connection->recv(65536, 1.0);
                    $connection->sendAll("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    $connection->close();
                }
            });

            $client = new WebSocketClient('ws://127.0.0.1:' . self::PORT);
            Coroutine::usleep(2_000_000);
            $client->stop();
            $running = false;
            Coroutine::usleep(200_000);
            $server->close();

            // 100 ms retries would make ~20 attempts in 2 s; backoff allows at most 1 + 6 (waits of 50..100, 100..200, ...).
            self::assertGreaterThanOrEqual(2, $attempts);
            self::assertLessThanOrEqual(8, $attempts);
        });
    }
}
