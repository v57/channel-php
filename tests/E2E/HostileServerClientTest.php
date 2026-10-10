<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\E2E;

use ChannelPhp\Channel;
use ChannelPhp\ChannelException;
use ChannelPhp\Tests\Support\CoroutineTestCase;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Socket;
use PHPUnit\Framework\Attributes\DataProvider;

/** Client against misbehaving servers: pending calls settle instead of hanging or crashing (AUDIT2). */
final class HostileServerClientTest extends CoroutineTestCase
{
    private const PORT = 6660;

    /** @return array<string, array{0: string, 1: ?string}> */
    public static function scenarios(): array
    {
        return [
            'invalid json' => ['badjson', 'invalid message from server'],
            'error object' => ['errorobj', '{"code":500,"message":"nope"}'],
            'declared length 2^64-1' => ['len64', 'connection closed: 1002'],
            'declared length 2^63-1' => ['len63', 'connection closed: 1009'],
            'close without reply' => ['close', 'disconnected'],
            'http 500 handshake' => ['http500', 'websocket handshake failed: HTTP 500'],
            'fragmented reply' => ['fragmented', null],
            'reply over openswoole default limit' => ['big', null],
        ];
    }

    #[DataProvider('scenarios')]
    public function testPendingCallSettles(string $scenario, ?string $expectedError): void
    {
        $this->co(function () use ($scenario, $expectedError): void {
            $server = new Socket(AF_INET, SOCK_STREAM, 0);
            $server->setOption(SOL_SOCKET, SO_REUSEADDR, 1);
            self::assertTrue($server->bind('127.0.0.1', self::PORT));
            self::assertTrue($server->listen(8));
            go(static function () use ($server, $scenario): void {
                $connection = $server->accept(5.0);
                if ($connection instanceof Socket) {
                    self::serve($connection, $scenario);
                }
            });

            $client = (new Channel())->connect(self::PORT);
            try {
                $result = $client->send('text/count', ['text' => 'hello']);
                self::assertNull($expectedError, 'expected an error, got a result');
                if ($scenario === 'big') {
                    self::assertSame(3 * 1024 * 1024, strlen($result));
                } else {
                    self::assertSame(5, $result);
                }
            } catch (ChannelException $error) {
                self::assertNotNull($expectedError, $error->getMessage());
                self::assertStringStartsWith($expectedError, $error->getMessage());
            } finally {
                $client->stop();
                $server->close();
            }
        });
    }

    private static function serve(Socket $connection, string $scenario): void
    {
        $request = '';
        while (!str_contains($request, "\r\n\r\n")) {
            $chunk = $connection->recv(65536, 2.0);
            if (!is_string($chunk) || $chunk === '') {
                return;
            }
            $request .= $chunk;
        }

        if ($scenario === 'http500') {
            $connection->sendAll("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
            Coroutine::usleep(200_000);
            $connection->close();
            return;
        }

        preg_match('/Sec-WebSocket-Key: (\S+)/i', $request, $match);
        $accept = base64_encode(sha1($match[1] . '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', true));
        $connection->sendAll("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {$accept}\r\n\r\n");

        $frame = $connection->recv(65536, 2.0); // the request (masked); its id is 0 for a fresh channel
        if (!is_string($frame) || $frame === '') {
            return;
        }

        $text = static fn(string $payload, int $opcode = 1, bool $fin = true): string => chr(($fin ? 0x80 : 0) | $opcode)
            . (strlen($payload) < 126 ? chr(strlen($payload))
                : (strlen($payload) < 65536 ? chr(126) . pack('n', strlen($payload)) : chr(127) . pack('J', strlen($payload))))
            . $payload;

        switch ($scenario) {
            case 'badjson':
                $connection->sendAll($text('{not json'));
                break;
            case 'errorobj':
                $connection->sendAll($text('{"id":0,"error":{"code":500,"message":"nope"}}'));
                break;
            case 'len64':
                $connection->sendAll("\x81\x7f" . str_repeat("\xff", 8));
                break;
            case 'len63':
                $connection->sendAll("\x81\x7f\x7f" . str_repeat("\xff", 7));
                break;
            case 'close':
                $connection->close();
                return;
            case 'fragmented':
                $connection->sendAll($text('{"id":0,', 1, false) . $text('"body":5}', 0, true));
                break;
            case 'big':
                $connection->sendAll($text('{"id":0,"body":"' . str_repeat('a', 3 * 1024 * 1024) . '"}'));
                break;
        }
        $connection->recv(65536, 1.5); // until the client goes away
        $connection->close();
    }
}
