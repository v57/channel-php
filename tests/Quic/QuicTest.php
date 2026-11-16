<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Quic;

use ChannelPhp\Channel;
use ChannelPhp\ChannelException;
use ChannelPhp\ClientSender;
use ChannelPhp\Quic\FrameReader;
use ChannelPhp\Quic\Frames;
use ChannelPhp\Quic\FrameTooLarge;
use ChannelPhp\Quic\OpenSsl;
use ChannelPhp\Quic\QuicLink;
use ChannelPhp\Quic\QuicServer;
use ChannelPhp\Quic\QuicUnavailable;
use ChannelPhp\Subscription;
use ChannelPhp\Tests\Support\CoroutineTestCase;
use OpenSwoole\Coroutine;

/**
 * QUIC transport: frames, the PHP client against the PHP server, the wire rules with raw frames, trust,
 * reconnect, the PHP client against the TypeScript fixture's QUIC server, and the TypeScript reference's client
 * against the PHP server. Mirrors channel-dart/test/quic_test.dart.
 */
final class QuicTest extends CoroutineTestCase
{
    /** The TypeScript reference's test certificate (localhost, 127.0.0.1 and ::1) */
    private const FINGERPRINT = 'DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B';

    private static ?string $cert = null;
    private static ?string $key = null;

    /** @var resource|null the TypeScript fixture with QUIC, on its own port */
    private static $fixture = null;

    private Subscription $ticks;
    private int $connected = 0;
    private int $disconnected = 0;
    private bool $dropped = false;

    public static function setUpBeforeClass(): void
    {
        $dir = dirname(__DIR__, 3) . '/channel/tests/quic/';
        self::$cert = is_file($dir . 'cert.pem') ? (string) file_get_contents($dir . 'cert.pem') : null;
        self::$key = is_file($dir . 'key.pem') ? (string) file_get_contents($dir . 'key.pem') : null;
    }

    public static function tearDownAfterClass(): void
    {
        if (is_resource(self::$fixture)) {
            proc_terminate(self::$fixture, SIGTERM);
            proc_close(self::$fixture);
            self::$fixture = null;
        }
    }

    protected function setUp(): void
    {
        if (self::$cert === null || self::$key === null) {
            self::markTestSkipped("the TypeScript reference's test certificate isn't there");
        }
        try {
            OpenSsl::instance();
        } catch (QuicUnavailable $error) {
            self::markTestSkipped($error->getMessage());
        }
        $this->ticks = new Subscription();
    }

    private static function certPath(): string
    {
        return dirname(__DIR__, 3) . '/channel/tests/quic/cert.pem';
    }

    private function serverChannel(): Channel
    {
        $ticks = $this->ticks;
        return (new Channel())
            ->post('hello', static fn(): string => 'world')
            ->post('echo', static fn(array $body): mixed => $body['body'] ?? null)
            ->post('name', static fn(array $body): mixed => $body['state']->headers['x-name'] ?? null)
            ->post('mirror', static fn(array $body): mixed => $body['sender']->send('hello'))
            ->post('slow', static function (): string {
                Coroutine::usleep(300_000);
                return 'slow';
            })
            ->post('disconnect', function (array $body): mixed {
                if (!$this->dropped) {
                    $this->dropped = true;
                    $body['sender']->stop();
                    Coroutine::usleep(5_000_000);
                }
                return 'dropped before';
            })
            ->stream('count', static function (): \Generator {
                for ($i = 0; $i < 3; $i++) {
                    yield $i;
                }
            })
            ->post('tick', static function () use ($ticks): mixed {
                $ticks->send('a', 'tick');
                return null;
            })
            ->events(['ticks' => $ticks]);
    }

    private function listen(int|string $address = '127.0.0.1:0', array $options = []): QuicServer
    {
        return $this->serverChannel()->listenQuic($address, $options + [
            'cert' => self::$cert,
            'key' => self::$key,
            'state' => static fn(array $headers): object => (object) ['headers' => $headers],
            'onConnect' => function (): void {
                $this->connected++;
            },
            'onDisconnect' => function (): void {
                $this->disconnected++;
            },
        ]);
    }

    private static function client(string $address, array $quic, array $headers = []): ClientSender
    {
        return (new Channel())
            ->post('hello', static fn(): string => 'client world')
            ->connect($address, ['quic' => $quic, 'headers' => $headers === [] ? null : static fn(): array => $headers]);
    }

    private static function waitFor(callable $ready, float $timeout = 5.0): void
    {
        $deadline = microtime(true) + $timeout;
        while (!$ready()) {
            if (microtime(true) > $deadline) {
                self::fail('timed out');
            }
            Coroutine::usleep(5_000);
        }
    }

    public function testFrames(): void
    {
        self::assertSame("\x05hello", Frames::encode('hello'));
        self::assertSame("\x40\x40", substr(Frames::encode(str_repeat('a', 64)), 0, 2));
        self::assertSame("\x80\x00\x40\x00", substr(Frames::encode(str_repeat('a', 0x4000)), 0, 4));
        $reader = new FrameReader();
        $stream = Frames::encode('one') . "\0" . Frames::encode(str_repeat('x', 300)) . Frames::encode('three');
        $frames = [];
        // Byte by byte: lengths and payloads split anywhere
        foreach (str_split($stream) as $byte) {
            array_push($frames, ...$reader->push($byte));
        }
        self::assertSame(['one', str_repeat('x', 300), 'three'], $frames);
        // 8-byte lengths
        $reader = new FrameReader();
        self::assertSame(['abc'], $reader->push("\xc0\0\0\0\0\0\0\x03abc"));
        // The limit: the frames before an oversized one still come out
        $small = new FrameReader(10);
        self::assertSame(['ok'], $small->push(Frames::encode('ok') . Frames::encode(str_repeat('y', 11))));
        self::assertTrue($small->failed());
        $this->expectException(FrameTooLarge::class);
        $small->push('');
    }

    public function testHelloParsing(): void
    {
        self::assertSame(['x-name' => 'b'], Frames::parseHello('{"hello":{"X-Name":"a","x-name":"b","n":1}}'));
        self::assertSame([], Frames::parseHello('{"hello":{}}'));
        self::assertNull(Frames::parseHello('{"hello":"text"}'));
        self::assertNull(Frames::parseHello('{"id":1}'));
        self::assertNull(Frames::parseHello('{nope'));
        self::assertSame("\x0c{\"hello\":{}}", Frames::hello([]));
    }

    public function testAddressesAndOptionsAreCheckedUpFront(): void
    {
        $this->co(function (): void {
            foreach (['ws://127.0.0.1:1', 'quic://127.0.0.1', 'quic://:1'] as $address) {
                try {
                    QuicLink::parseAddress($address);
                    self::fail("$address accepted");
                } catch (\InvalidArgumentException) {
                }
            }
            self::assertSame(['::1', 443], QuicLink::parseAddress('quic://[::1]:443'));
            self::assertSame(32, strlen(QuicLink::parseFingerprint(self::FINGERPRINT)));
            foreach ([['fingerprint' => 'abc'], ['nope' => 1]] as $quic) {
                try {
                    (new Channel())->connect('quic://127.0.0.1:9', ['quic' => $quic]);
                    self::fail(json_encode($quic) . ' accepted');
                } catch (\InvalidArgumentException) {
                }
            }
        });
    }

    public function testPostsHelloHeadersStreamsEventsManyInFlight(): void
    {
        $this->co(function (): void {
            $server = $this->listen();
            $client = self::client('quic://127.0.0.1:' . $server->port(), ['ca' => self::$cert], ['X-Name' => 'php']);
            try {
                self::assertSame('world', $client->send('hello'));
                $body = ['list' => [1, 'two', null], 'text' => str_repeat('é', 20000)];
                self::assertSame($body, $client->send('echo', $body));
                // The hello headers become the state, names lowercased
                self::assertSame('php', $client->send('name'));
                // The server calls the client back
                self::assertSame('client world', $client->send('mirror'));
                self::assertSame([0, 1, 2], iterator_to_array($client->values('count'), false));

                $got = [];
                $subscription = $client->subscribe('ticks', 'a', static function (mixed $event) use (&$got): void {
                    $got[] = $event;
                });
                $this->ticks->send('a', 'tick');
                $this->ticks->send('b', 'not for us');
                $this->ticks->send('a', 'tick');
                self::waitFor(static function () use (&$got): bool {
                    return count($got) === 2;
                });
                $subscription->cancel();
                Coroutine::usleep(20_000);
                $this->ticks->send('a', 'after cancel');
                Coroutine::usleep(50_000);
                self::assertSame(['tick', 'tick'], $got);

                // Many requests in flight at once
                $results = [];
                $done = new Coroutine\Channel(1);
                $count = 500;
                for ($i = 0; $i < $count; $i++) {
                    go(static function () use ($client, $i, &$results, $done, $count): void {
                        $results[$i] = $client->send('echo', $i);
                        if (count($results) === $count) {
                            $done->push(true);
                        }
                    });
                }
                self::assertTrue($done->pop(10.0));
                ksort($results);
                self::assertSame(range(0, $count - 1), $results);
                self::assertSame(1, $server->connectionCount());
            } finally {
                $client->stop();
                $server->stop();
            }
        });
    }

    public function testCertificateTrust(): void
    {
        $this->co(function (): void {
            $server = $this->listen();
            $address = 'quic://127.0.0.1:' . $server->port();
            $clients = [];
            try {
                $accepted = [
                    'pin' => [$address, ['fingerprint' => strtolower(str_replace(':', '', self::FINGERPRINT))]],
                    'server name' => [$address, ['ca' => self::$cert, 'servername' => 'localhost']],
                    'localhost' => ['quic://localhost:' . $server->port(), ['ca' => self::$cert]],
                ];
                foreach ($accepted as $name => [$target, $quic]) {
                    $clients[] = $client = self::client($target, $quic);
                    self::assertSame('world', $client->send('hello'), $name);
                }
                $refused = [
                    'system roots' => [],
                    'wrong pin' => ['fingerprint' => str_repeat('00', 32)],
                    'name not in cert' => ['ca' => self::$cert, 'servername' => 'example.com'],
                ];
                foreach ($refused as $name => $quic) {
                    try {
                        QuicLink::raw($address, $quic);
                        self::fail("$name: connected");
                    } catch (\RuntimeException $error) {
                        self::assertStringContainsString($name === 'wrong pin' ? 'fingerprint mismatch' : 'certificate verify failed', $error->getMessage(), $name);
                    }
                    $clients[] = $client = self::client($address, $quic);
                    Coroutine::usleep(300_000);
                    self::assertFalse($client->ws->isConnected(), $name);
                }
                self::assertSame(3, $this->connected);
            } finally {
                foreach ($clients as $client) {
                    $client->stop();
                }
                $server->stop();
            }
        });
    }

    public function testReconnect(): void
    {
        $this->co(function (): void {
            $server = $this->listen();
            $port = $server->port();
            $client = self::client('quic://127.0.0.1:' . $port, ['ca' => self::$cert]);
            try {
                self::assertSame('world', $client->send('hello'));
                // The server drops the connection without answering: like this port's WebSocket client, the
                // request fails, and the next one goes out on a new connection
                try {
                    $client->send('disconnect');
                    self::fail('answered');
                } catch (ChannelException $error) {
                    self::assertSame('disconnected', $error->getMessage());
                }
                self::assertSame('world', $client->send('hello'));
                self::assertSame(2, $this->connected);

                // The server goes away: a request made meanwhile goes to the next server on the port
                $server->stop();
                self::waitFor(static fn(): bool => !$client->ws->isConnected());
                $queued = null;
                go(static function () use ($client, &$queued): void {
                    $queued = $client->send('echo', 'queued');
                });
                Coroutine::usleep(200_000);
                $server = $this->listen('127.0.0.1:' . $port);
                self::waitFor(static function () use (&$queued): bool {
                    return $queued !== null;
                }, 10.0);
                self::assertSame('queued', $queued);
                self::assertTrue($client->ws->isConnected());
            } finally {
                $client->stop();
                $server->stop();
            }
        });
    }

    public function testTheServerClosesConnectionsOnStop(): void
    {
        $this->co(function (): void {
            $server = $this->listen();
            $client = self::client('quic://127.0.0.1:' . $server->port(), ['ca' => self::$cert]);
            try {
                self::assertSame('world', $client->send('hello'));
                $slow = null;
                go(static function () use ($client, &$slow): void {
                    try {
                        $slow = $client->send('slow');
                    } catch (ChannelException $error) {
                        $slow = $error->getMessage();
                    }
                });
                Coroutine::usleep(50_000);
                $server->stop();
                self::waitFor(static fn(): bool => !$client->ws->isConnected());
                self::assertSame([1, 1], [$this->connected, $this->disconnected]);
                self::assertSame(0, $server->connectionCount());
                self::waitFor(static function () use (&$slow): bool {
                    return $slow !== null;
                });
                self::assertSame('disconnected', $slow);
            } finally {
                $client->stop();
                $server->stop();
            }
        });
    }

    public function testAFrameLimitClosesTheConnection(): void
    {
        $this->co(function (): void {
            $server = $this->listen('127.0.0.1:0', ['maxPayload' => 100]);
            try {
                $raw = QuicLink::raw('quic://127.0.0.1:' . $server->port(), ['ca' => self::$cert]);
                $raw->write(Frames::hello([]));
                $raw->write(Frames::encode('{"id":1,"path":"echo","body":"small"}'));
                self::assertSame('small', json_decode((string) $raw->receiveWithin(5.0), true)['body']);
                $raw->write(Frames::encode(json_encode(['id' => 2, 'path' => 'echo', 'body' => str_repeat('x', 200)])));
                self::waitFor(static fn(): bool => !$raw->isOpen());
                $raw->close();
            } finally {
                $server->stop();
            }
        });
    }

    public function testRawHelloKeepaliveAndBatch(): void
    {
        $this->co(function (): void {
            $server = $this->listen();
            try {
                $raw = QuicLink::raw('quic://127.0.0.1:' . $server->port(), ['ca' => self::$cert]);
                $raw->write(Frames::hello(['X-Name' => 'raw']) . "\0" . Frames::encode('[{"id":1,"path":"name"},{"id":2,"path":"hello"}]'));
                $got = [];
                while (count($got) < 2) {
                    $text = $raw->receiveWithin(5.0);
                    self::assertNotNull($text);
                    $message = json_decode($text, true);
                    foreach (array_is_list($message) ? $message : [$message] as $item) {
                        $got[$item['id']] = $item['body'];
                    }
                }
                ksort($got);
                self::assertSame([1 => 'raw', 2 => 'world'], $got);
                $raw->close();
            } finally {
                $server->stop();
            }
        });
    }

    public function testOneStreamPerConnection(): void
    {
        $this->co(function (): void {
            $server = $this->listen();
            try {
                $raw = QuicLink::raw('quic://127.0.0.1:' . $server->port(), ['ca' => self::$cert]);
                $raw->write(Frames::hello([]));
                // The server has its stream before the next ones arrive
                self::waitFor(fn(): bool => $this->connected === 1);
                foreach ([false, true] as $index => $unidirectional) {
                    $raw->writeOnNewStream($unidirectional ? "\x05hello" : Frames::hello([]), $unidirectional);
                    self::waitFor(static fn(): bool => $raw->extraStreamRefused($index), 3.0);
                }
                $raw->write(Frames::encode('{"id":7,"path":"hello"}'));
                self::assertSame('world', json_decode((string) $raw->receiveWithin(5.0), true)['body']);
                self::assertSame(1, $this->connected);
                $raw->close();
            } finally {
                $server->stop();
            }
        });
    }

    /** @return array<string, array{string}> */
    public static function closingFrames(): array
    {
        return [
            "a first frame that isn't a hello" => [Frames::encode('{"id":1,"path":"hello"}')],
            "a hello that isn't an object" => [Frames::encode('{"hello":"text"}')],
            'invalid JSON' => [Frames::hello([]) . Frames::encode('{nope')],
            'a frame over the limit' => [Frames::hello([]) . "\x81\0\0\x01"],
        ];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('closingFrames')]
    public function testWireRuleViolationsCloseTheConnection(string $bytes): void
    {
        $this->co(function () use ($bytes): void {
            $server = $this->listen();
            try {
                $raw = QuicLink::raw('quic://127.0.0.1:' . $server->port(), ['ca' => self::$cert]);
                $raw->write($bytes);
                self::waitFor(static fn(): bool => !$raw->isOpen());
                self::assertNull($raw->receiveWithin(0.01));
                $raw->close();
            } finally {
                $server->stop();
            }
        });
    }

    public function testPhpClientAgainstTheTypeScriptFixture(): void
    {
        $port = (int) (getenv('CHANNEL_QUIC_TEST_PORT') ?: 2449);
        self::startFixture($port);
        $this->co(function () use ($port): void {
            $names = new Subscription();
            $client = (new Channel())
                ->post('hello', static fn(): string => 'client world')
                ->stream('stream/values', static function (): \Generator {
                    for ($i = 0; $i < 3; $i++) {
                        yield $i;
                    }
                })
                ->events(['names' => $names])
                ->connect("quic://127.0.0.1:$port", ['quic' => ['ca' => self::$cert], 'headers' => static fn(): array => ['X-Port' => 'php']]);
            try {
                self::assertSame('world', $client->send('hello'));
                self::assertSame(['a' => [1, 'b']], $client->send('echo', ['a' => [1, 'b']]));
                self::assertSame('client world', $client->send('mirror'));
                self::assertSame('quic', $client->send('auth', ['name' => 'quic']));
                self::assertSame('quic', $client->send('auth/name'));
                // The hello headers, lowercased
                self::assertSame('php', $client->send('v2/headers')['x-port'] ?? null);
                try {
                    $client->send('v2/error/code');
                    self::fail('no error');
                } catch (ChannelException) {
                }
                // The fixture calls this client's stream and subscribes to its events
                self::assertSame([0, 1, 2], iterator_to_array($client->values('mirror/stream'), false));
                $client->send('mirror/events');
                Coroutine::usleep(20_000);
                $names->send('1', 'h');
            } finally {
                $client->stop();
            }

            $pinned = (new Channel())->connect("quic://localhost:$port", ['quic' => ['fingerprint' => self::FINGERPRINT]]);
            try {
                self::assertSame('world', $pinned->send('hello'));
            } finally {
                $pinned->stop();
            }
        });
    }

    public function testTheTypeScriptClientAgainstThePhpServer(): void
    {
        if (!is_file(dirname(__DIR__, 3) . '/channel/quic.ts')) {
            self::markTestSkipped("the TypeScript reference isn't next to this checkout");
        }
        $this->co(function (): void {
            $server = $this->listen();
            try {
                $result = Coroutine\System::exec(sprintf(
                    'cd %s && bun tests/Quic/client.ts %d %s 2>&1',
                    escapeshellarg(dirname(__DIR__, 2)),
                    $server->port(),
                    escapeshellarg(self::certPath()),
                ));
                self::assertIsArray($result);
                $output = trim((string) $result['output']);
                self::assertSame(0, $result['code'], $output);
                $lines = explode("\n", $output);
                self::assertSame([
                    'hello' => 'world',
                    'echo' => ['a' => [1, 'b']],
                    'name' => 'ts',
                    'mirror' => 'ts world',
                    'values' => [0, 1, 2],
                    'event' => 'tick',
                ], json_decode((string) end($lines), true), $output);
                self::waitFor(fn(): bool => $this->connected >= 1 && $this->disconnected === $this->connected);
            } finally {
                $server->stop();
            }
        });
    }

    private static function startFixture(int $port): void
    {
        if (is_resource(self::$fixture)) {
            return;
        }
        $env = getenv();
        $env['CHANNEL_TEST_PORT'] = (string) $port;
        $process = proc_open(['bun', 'tests/server.ts'], [0 => ['pipe', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes, dirname(__DIR__, 3) . '/channel', $env);
        if (!is_resource($process)) {
            throw new \RuntimeException('failed to start the TypeScript fixture');
        }
        self::$fixture = $process;
        $deadline = microtime(true) + 10.0;
        while (microtime(true) < $deadline) {
            $socket = @stream_socket_client("tcp://127.0.0.1:$port", $errno, $errstr, 0.2);
            if (is_resource($socket)) {
                fclose($socket);
                return;
            }
            usleep(50_000);
        }
        throw new \RuntimeException("the TypeScript fixture didn't start on $port");
    }
}
