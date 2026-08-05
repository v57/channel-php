<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Unit;

use ChannelPhp\Channel;
use ChannelPhp\Internal\JsonCodec;
use ChannelPhp\Subscription;
use ChannelPhp\Transports\ServerTransport;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Http\Client as CoHttpClient;
use PHPUnit\Framework\TestCase;

final class ServerTransportCoverageTest extends TestCase
{
    public function testListenAcceptsStringAddress(): void
    {
        $port = $this->allocatePort();
        $running = (new Channel())
            ->post('hello', static fn(): string => 'world')
            ->listen('127.0.0.1:' . $port);

        try {
            self::assertSame('127.0.0.1', $running->host);
            self::assertSame($port, $running->port);
        } finally {
            $running->stop();
        }
    }

    public function testListenThrowsWhenPortIsInUse(): void
    {
        $guard = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        self::assertIsResource($guard);

        $name = stream_socket_get_name($guard, false);
        self::assertIsString($name);
        self::assertStringContainsString(':', $name);
        $port = (int) substr((string) strrchr($name, ':'), 1);

        try {
            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('OpenSwoole server failed to start');

            (new Channel())
                ->post('hello', static fn(): string => 'world')
                ->listen('127.0.0.1:' . $port);
        } finally {
            fclose($guard);
        }
    }

    public function testRunServerProcessLifecycleAndCallbacks(): void
    {
        if (!function_exists('pcntl_fork')) {
            self::markTestSkipped('pcntl_fork is required for server transport lifecycle test.');
        }
        if (!function_exists('swoole_event_exit')) {
            self::markTestSkipped('swoole_event_exit is required to stop in-process ServerTransport test.');
        }

        $port = $this->allocatePort();
        $resultFile = tempnam(sys_get_temp_dir(), 'channelphp-servertransport-');
        self::assertIsString($resultFile);

        $events = ['news' => new Subscription()];
        $onConnect = 0;
        $onDisconnect = 0;
        $headersByConnection = [];

        $channel = (new Channel())
            ->post('echo', static fn(array $body): mixed => $body['body'] ?? null)
            ->post('emit', static function (array $body) use ($events): string {
                $events['news']->send('room', $body['body'] ?? null);
                return 'emitted';
            })
            ->post('shutdown', static function (): string {
                @swoole_event_exit();
                return 'bye';
            })
            ->events($events);

        $clientPid = pcntl_fork();
        self::assertIsInt($clientPid);
        self::assertGreaterThanOrEqual(0, $clientPid);
        if ($clientPid === 0) {
            try {
                self::runClientSequence($port, $resultFile);
            } catch (\Throwable $error) {
                file_put_contents($resultFile, JsonCodec::encode(['error' => $error->getMessage()]));
                exit(1);
            }
            exit(0);
        }

        $signalPipe = fopen('php://temp', 'w+');
        self::assertIsResource($signalPipe);

        try {
            pcntl_async_signals(true);
            pcntl_signal(SIGALRM, static function (): void {
                @swoole_event_exit();
            });
            pcntl_alarm(10);

            $method = new \ReflectionMethod(ServerTransport::class, 'runServerProcess');
            $method->setAccessible(true);
            $method->invoke(null, $channel, '127.0.0.1', $port, [
                'state' => static function (array $headers) use (&$headersByConnection): object {
                    $headersByConnection[] = $headers;
                    $state = new \stdClass();
                    $state->headers = $headers;
                    return $state;
                },
                'onConnect' => static function (array $connection) use (&$onConnect): void {
                    $onConnect++;
                    if ($onConnect === 1) {
                        $connection['sender']->request('client/ask');
                        $cancelHandle = $connection['sender']->request('client/cancel');
                        $cancelHandle->cancel();
                    }
                },
                'onDisconnect' => static function (array $connection) use (&$onDisconnect): void {
                    $onDisconnect++;
                    // Exercise the connection stop path after close.
                    $connection['sender']->stop();
                },
            ], $signalPipe);
        } finally {
            pcntl_alarm(0);
            pcntl_signal(SIGALRM, SIG_DFL);

            if (is_resource($signalPipe)) {
                fclose($signalPipe);
            }
            pcntl_waitpid($clientPid, $status);
        }

        $raw = file_get_contents($resultFile);
        @unlink($resultFile);
        self::assertIsString($raw);
        self::assertNotSame('', $raw);

        $result = JsonCodec::decode($raw);
        self::assertIsArray($result);
        self::assertArrayNotHasKey('error', $result);

        self::assertTrue((bool) ($result['first']['upgrade'] ?? false));
        self::assertTrue((bool) ($result['second']['upgrade'] ?? false));
        self::assertSame('client/ask', $result['first']['ask']['path'] ?? null);
        self::assertArrayHasKey('cancel', $result['first']['cancel'] ?? []);
        self::assertSame('ping', $result['first']['echo']['body'] ?? null);
        self::assertSame('news/room', $result['first']['sub']['topic'] ?? null);
        self::assertSame('emitted', $result['first']['emit']['body'] ?? null);
        self::assertSame('bye', $result['second']['shutdown']['body'] ?? null);

        self::assertGreaterThanOrEqual(2, $onConnect);
        self::assertGreaterThanOrEqual(2, $onDisconnect);
        self::assertNotSame([], $headersByConnection);
        self::assertArrayHasKey('foo', $headersByConnection[0]);
        self::assertArrayHasKey('x-test', $headersByConnection[0]);
        self::assertSame([], $events['news']->publishers);
    }

    private static function runClientSequence(int $port, string $resultFile): void
    {
        $result = [
            'first' => ['upgrade' => false],
            'second' => ['upgrade' => false],
        ];

        Coroutine::run(static function () use ($port, &$result): void {
            $first = self::openClient($port, '/?foo=1&arr[]=x', ['x-test' => 'alpha']);
            $result['first']['upgrade'] = $first instanceof CoHttpClient;

            if ($first instanceof CoHttpClient) {
                $result['first']['ask'] = self::recvMatching(
                    $first,
                    static fn(array $payload): bool => ($payload['path'] ?? null) === 'client/ask',
                );

                $askId = $result['first']['ask']['id'] ?? null;
                if (is_int($askId)) {
                    $first->push(JsonCodec::encode(['id' => $askId, 'body' => 'from-client']));
                }

                $result['first']['cancel'] = self::recvMatching(
                    $first,
                    static fn(array $payload): bool => array_key_exists('cancel', $payload),
                );

                $first->push(JsonCodec::encode(['id' => 100, 'path' => 'echo', 'body' => 'ping']));
                $result['first']['echo'] = self::recvMatching(
                    $first,
                    static fn(array $payload): bool => ($payload['id'] ?? null) === 100,
                );

                $first->push(JsonCodec::encode(['id' => 101, 'sub' => 'news', 'body' => 'room']));
                $result['first']['sub'] = self::recvMatching(
                    $first,
                    static fn(array $payload): bool => ($payload['id'] ?? null) === 101
                        && array_key_exists('topic', $payload),
                );

                $first->push(JsonCodec::encode(['id' => 102, 'path' => 'emit', 'body' => 'first']));
                $result['first']['emit'] = self::recvMatching(
                    $first,
                    static fn(array $payload): bool => ($payload['id'] ?? null) === 102,
                );
                $result['first']['event'] = self::recvMatching(
                    $first,
                    static fn(array $payload): bool => ($payload['topic'] ?? null) === 'news/room',
                );

                $first->push('{');
                Coroutine::usleep(50_000);
                $first->close();
            }

            $second = self::openClient($port, '/?foo=2', ['x-test' => 'beta']);
            $result['second']['upgrade'] = $second instanceof CoHttpClient;
            if ($second instanceof CoHttpClient) {
                $second->push(JsonCodec::encode(['id' => 103, 'path' => 'shutdown']));
                $result['second']['shutdown'] = self::recvMatching(
                    $second,
                    static fn(array $payload): bool => ($payload['id'] ?? null) === 103,
                );
                $second->close();
            }
        });

        file_put_contents($resultFile, JsonCodec::encode($result));
    }

    private static function openClient(int $port, string $path, array $headers = []): ?CoHttpClient
    {
        for ($attempt = 0; $attempt < 80; $attempt++) {
            $client = new CoHttpClient('127.0.0.1', $port);
            if ($headers !== []) {
                $client->setHeaders($headers);
            }

            if ($client->upgrade($path)) {
                return $client;
            }

            $client->close();
            Coroutine::usleep(50_000);
        }

        return null;
    }

    private static function recvMatching(CoHttpClient $client, callable $matcher): ?array
    {
        for ($attempt = 0; $attempt < 10; $attempt++) {
            $frame = $client->recv(0.5);
            if (!is_object($frame) || !property_exists($frame, 'data')) {
                continue;
            }

            if (!is_string($frame->data) || $frame->data === '') {
                continue;
            }

            try {
                $payload = JsonCodec::decode($frame->data);
            } catch (\Throwable) {
                continue;
            }

            if (is_array($payload) && $matcher($payload)) {
                return $payload;
            }
        }

        return null;
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

        return (int) substr((string) strrchr($name, ':'), 1);
    }
}
