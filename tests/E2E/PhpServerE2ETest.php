<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\E2E;

use ChannelPhp\Channel;
use ChannelPhp\ChannelException;
use ChannelPhp\ClientSender;
use ChannelPhp\Subscription;
use ChannelPhp\Tests\Support\CoroutineTestCase;
use ChannelPhp\Transports\RunningServer;
use OpenSwoole\Coroutine;

final class PhpServerE2ETest extends CoroutineTestCase
{
    public function testPhpServerRequestStreamAndAuthLifecycle(): void
    {
        $running = $this->bootServer();

        try {
            $this->co(function () use ($running): void {
                $client = (new Channel())->connect('ws://127.0.0.1:' . $running->port);

                try {
                    self::assertSame('world', $client->send('hello'));
                    self::assertSame('payload', $client->send('echo', 'payload'));
                    self::assertNull($client->send('empty'));

                    $streamValues = [];
                    foreach ($client->values('stream/values') as $value) {
                        $streamValues[] = $value;
                    }
                    self::assertSame([0, 1, 2], $streamValues);

                    $cancelled = [];
                    foreach ($client->values('stream/cancel') as $value) {
                        $cancelled[] = $value;
                        if ($value === 2) {
                            break;
                        }
                    }
                    self::assertSame([0, 1, 2], $cancelled);

                    self::assertSame('tester', $client->send('auth', ['name' => 'tester']));
                    self::assertSame('tester', $client->send('auth/name'));

                    $other = (new Channel())->connect('ws://127.0.0.1:' . $running->port);
                    try {
                        try {
                            $other->send('auth/name');
                            self::fail('Expected unauthorized state error');
                        } catch (ChannelException $error) {
                            self::assertSame('unauthorized', $error->getMessage());
                        }
                    } finally {
                        $other->stop();
                    }
                } finally {
                    $client->stop();
                }
            });
        } finally {
            $running->stop();
        }
    }

    public function testPhpServerEventsSubscribeAndUnsubscribe(): void
    {
        $running = $this->bootServer();

        try {
            $this->co(function () use ($running): void {
                $client = (new Channel())->connect('ws://127.0.0.1:' . $running->port);
                $received = [];

                try {
                    $subscription = $client->subscribe('hello', 'topic-1', static function (mixed $event) use (&$received): void {
                        $received[] = $event;
                    });

                    self::assertSame('ok', $client->send('emit/hello', ['topic' => 'topic-1', 'event' => 'first']));
                    Coroutine::usleep(25_000);
                    self::assertSame(['first'], $received);

                    $subscription->cancel();
                    self::assertSame('ok', $client->send('emit/hello', ['topic' => 'topic-1', 'event' => 'second']));
                    Coroutine::usleep(25_000);
                    self::assertSame(['first'], $received);
                } finally {
                    $client->stop();
                }
            });
        } finally {
            $running->stop();
        }
    }

    public function testPhpServerMirrorAndMirrorStreamWithClientApi(): void
    {
        $running = $this->bootServer();

        try {
            $this->co(function () use ($running): void {
                $clientChannel = (new Channel())->api([
                    'hello' => static fn(): string => 'client world',
                    'stream' => [
                        'values' => static function (): \Generator {
                            for ($i = 0; $i < 3; $i++) {
                                yield $i;
                            }
                        },
                        'cancel' => static function (): \Generator {
                            for ($i = 0; $i < 10; $i++) {
                                yield $i;
                            }
                        },
                    ],
                ]);
                $client = $clientChannel->connect('ws://127.0.0.1:' . $running->port);

                try {
                    self::assertSame('client world', $client->send('mirror'));

                    $values = [];
                    foreach ($client->values('mirror/stream') as $value) {
                        $values[] = $value;
                    }
                    self::assertSame([0, 1, 2], $values);
                } finally {
                    $client->stop();
                }
            });
        } finally {
            $running->stop();
        }
    }

    private function bootServer(): RunningServer
    {
        $events = [
            'hello' => new Subscription(),
        ];

        $serverChannel = (new Channel())
            ->post('hello', static fn(): string => 'world')
            ->post('echo', static fn(array $body): mixed => $body['body'] ?? null)
            ->post('empty', static fn(): mixed => null)
            ->post('auth', static function (array $body): string {
                $name = (string) ($body['body']['name'] ?? '');
                if (is_object($body['state'])) {
                    $body['state']->name = $name;
                }
                return $name;
            })
            ->post('auth/name', static function (array $body): string {
                if (is_object($body['state']) && isset($body['state']->name)) {
                    return (string) $body['state']->name;
                }
                throw new \RuntimeException('unauthorized');
            })
            ->post('emit/hello', static function (array $body) use ($events): string {
                $topic = (string) ($body['body']['topic'] ?? '');
                $event = $body['body']['event'] ?? null;
                $events['hello']->send($topic, $event);
                return 'ok';
            })
            ->post('mirror', static fn(array $body): mixed => $body['sender']->send('hello'))
            ->stream('stream/values', static function (): \Generator {
                for ($i = 0; $i < 3; $i++) {
                    yield $i;
                    Coroutine::usleep(1_000);
                }
            })
            ->stream('stream/cancel', static function (): \Generator {
                for ($i = 0; $i < 10; $i++) {
                    yield $i;
                    Coroutine::usleep(1_000);
                }
            })
            ->stream('mirror/stream', static function (array $body): \Generator {
                foreach ($body['sender']->values('stream/values') as $value) {
                    yield $value;
                }
            })
            ->stream('mirror/stream/cancel', static function (array $body): \Generator {
                foreach ($body['sender']->values('stream/cancel') as $value) {
                    yield $value;
                }
            })
            ->events($events);

        $port = $this->allocatePort();
        return $serverChannel->listen($port, [
            'state' => static fn(array $headers): object => new \stdClass(),
        ]);
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
