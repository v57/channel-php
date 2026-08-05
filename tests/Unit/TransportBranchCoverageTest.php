<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Unit;

use ChannelPhp\AsyncResult;
use ChannelPhp\Channel;
use ChannelPhp\Subscription;
use ChannelPhp\Tests\Support\CoroutineTestCase;
use ChannelPhp\Transports\ClientTransport;
use ChannelPhp\Transports\RunningServer;
use ChannelPhp\Transports\WebSocketClient;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Http\Client as CoHttpClient;

final class TransportBranchCoverageTest extends CoroutineTestCase
{
    public function testProcessTransportCoroutineAndStopBranches(): void
    {
        $this->co(function (): void {
            $subscription = new Subscription();
            $pending = null;

            $channel = (new Channel())
                ->post('hello', static fn(): string => 'world')
                ->post('slow', static function () use (&$pending): AsyncResult {
                    $pending = new AsyncResult();
                    return $pending;
                })
                ->events(['topic' => $subscription]);

            $sender = $channel->process();
            try {
                self::assertSame('world', $sender->send('hello'));

                $request = $sender->request('slow');
                $request->cancel();

                $handle = $sender->subscribe('topic', 'key', static fn(mixed $event): null => null);
                $handle->cancel();
            } finally {
                $sender->stop();
                $sender->stop();
            }
        });
    }

    public function testWebSocketClientSendBranchesAndStopCatch(): void
    {
        $this->co(function (): void {
            $client = $this->newWebSocketClient();
            $ws = new class extends CoHttpClient {
                public array $sent = [];
                public bool $throwOnClose = false;

                public function __construct()
                {
                }

                public function push($payload, $opcode = null, $flags = null)
                {
                    $this->sent[] = $payload;
                    return true;
                }

                public function close()
                {
                    if ($this->throwOnClose) {
                        throw new \RuntimeException('close failed');
                    }
                    return true;
                }
            };

            $this->setPrivate($client, 'ws', $ws);
            $client->send(['path' => 'a']);
            $this->setPrivate($client, 'isWaitingLength', 5001);
            Coroutine::usleep(20_000);
            self::assertSame(3, $this->getPrivate($client, 'isWaiting'));

            $this->setPrivate($client, 'queue', [['path' => 'queued']]);
            $this->setPrivate($client, 'isWaiting', 3);
            $client->send(['path' => 'b']);
            Coroutine::usleep(10_000);
            self::assertSame([], $this->getPrivate($client, 'queue'));

            $this->setPrivate($client, 'isWaiting', 99);
            $id = $client->send(['path' => 'c']);
            self::assertIsInt($id);

            $sendNow = new \ReflectionMethod(WebSocketClient::class, 'sendNow');
            $sendNow->setAccessible(true);
            $this->setPrivate($client, 'ws', null);
            $sendNow->invoke($client, ['path' => 'noop']);

            $ws->throwOnClose = true;
            $this->setPrivate($client, 'ws', $ws);
            $client->stop();
            self::assertFalse($client->isRunning());
        });
    }

    public function testClientTransportOnOpenHookAndCleanupIdempotence(): void
    {
        $this->co(function (): void {
            $events = ['news' => new Subscription()];
            $connected = 0;

            $client = ClientTransport::connect(
                (new Channel())->events($events),
                'ws://127.0.0.1:1',
                ['onConnect' => static function () use (&$connected): void {
                    $connected++;
                }],
            );

            try {
                $ws = $client->ws;

                $onOpen = $this->getPrivate($ws, 'onOpen');
                self::assertIsCallable($onOpen);
                $onOpen();
                self::assertSame(1, $connected);

                self::assertNotSame([], $events['news']->publishers);

                $this->setPrivate($ws, 'isRunning', false);
                $onClose = $this->getPrivate($ws, 'onClose');
                self::assertIsCallable($onClose);
                $onClose();
                $onClose();

                self::assertSame([], $events['news']->publishers);
            } finally {
                $client->stop();
            }
        });
    }

    public function testWebSocketClientConnectFailPathUsesHeadersAndQueryAndSleeps(): void
    {
        $this->co(function (): void {
            $closed = 0;
            $client = new WebSocketClient('ws://127.0.0.1:1/path?q=1', static fn(): array => ['x-token' => '1']);
            $client->setOnClose(static function () use (&$closed): void {
                $closed++;
            });

            Coroutine::usleep(250_000);
            $client->stop();
            Coroutine::usleep(20_000);

            self::assertGreaterThanOrEqual(1, $closed);
        });
    }

    public function testWebSocketClientOnOpenAndPendingReplay(): void
    {
        $port = $this->allocatePort();
        $server = (new Channel())
            ->post('hello', static fn(): string => 'world')
            ->listen($port, ['state' => static fn(array $headers): object => new \stdClass()]);

        try {
            $this->co(function () use ($server): void {
                $opened = 0;
                $client = new WebSocketClient('ws://127.0.0.1:' . $server->port);
                $client->send(['id' => 1, 'path' => 'hello']);
                $client->setOnOpen(static function () use (&$opened): void {
                    $opened++;
                });

                Coroutine::usleep(1_200_000);
                $client->stop();
                Coroutine::usleep(20_000);

                self::assertGreaterThanOrEqual(1, $opened);
            });
        } finally {
            $server->stop();
        }
    }

    public function testRunningServerForceKillPath(): void
    {
        $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        self::assertIsArray($pair);
        self::assertCount(2, $pair);

        $pid = pcntl_fork();
        if ($pid === 0) {
            fclose($pair[0]);
            pcntl_signal(SIGTERM, SIG_IGN);
            pcntl_async_signals(true);
            fwrite($pair[1], "ready\n");
            fflush($pair[1]);
            fclose($pair[1]);
            while (true) {
                usleep(100_000);
            }
            exit(0);
        }
        fclose($pair[1]);
        self::assertIsInt($pid);
        self::assertGreaterThan(0, $pid);

        stream_set_timeout($pair[0], 1);
        $ready = fgets($pair[0]);
        fclose($pair[0]);
        self::assertSame("ready\n", $ready);

        $running = new RunningServer($pid, '127.0.0.1', 0);
        $startedAt = microtime(true);
        $running->stop();
        self::assertGreaterThan(1.5, microtime(true) - $startedAt);

        $alive = @posix_kill($pid, 0);
        self::assertFalse($alive);
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

    private function newWebSocketClient(): WebSocketClient
    {
        $reflection = new \ReflectionClass(WebSocketClient::class);
        /** @var WebSocketClient $client */
        $client = $reflection->newInstanceWithoutConstructor();

        $this->setPrivate($client, 'id', 0);
        $this->setPrivate($client, 'pending', []);
        $this->setPrivate($client, 'subscribed', []);
        $this->setPrivate($client, 'topicId', 0);
        $this->setPrivate($client, 'isWaiting', 0);
        $this->setPrivate($client, 'isWaitingLength', 0);
        $this->setPrivate($client, 'queue', []);
        $this->setPrivate($client, 'isConnected', false);
        $this->setPrivate($client, 'isRunning', true);
        $this->setPrivate($client, 'ws', null);
        $this->setPrivate($client, 'onOpen', null);
        $this->setPrivate($client, 'onClose', null);
        $this->setPrivate($client, 'onMessage', null);
        $this->setPrivate($client, 'address', 'ws://127.0.0.1:65535');
        $this->setPrivate($client, 'headersProvider', null);

        return $client;
    }

    private function setPrivate(object $object, string $property, mixed $value): void
    {
        $reflection = new \ReflectionProperty($object, $property);
        $reflection->setAccessible(true);
        $reflection->setValue($object, $value);
    }

    private function getPrivate(object $object, string $property): mixed
    {
        $reflection = new \ReflectionProperty($object, $property);
        $reflection->setAccessible(true);
        return $reflection->getValue($object);
    }
}
