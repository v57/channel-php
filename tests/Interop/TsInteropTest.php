<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Interop;

use ChannelPhp\Channel;
use ChannelPhp\ChannelException;
use ChannelPhp\ClientSender;
use ChannelPhp\Subscription;
use ChannelPhp\Tests\Support\TsInteropTestCase;
use OpenSwoole\Coroutine;

final class TsInteropTest extends TsInteropTestCase
{
    private function connect(Channel $channel): ClientSender
    {
        return $channel->connect('ws://127.0.0.1:2049');
    }

    public function testHelloEchoAndEmpty(): void
    {
        $this->co(function (): void {
            $client = $this->connect(new Channel());
            try {
                self::assertSame('world', $client->send('hello'));
                self::assertSame('payload', $client->send('echo', 'payload'));
                self::assertNull($client->send('empty'));
            } finally {
                $client->stop();
            }
        });
    }

    public function testUnknownApiReturnsNotFound(): void
    {
        $this->co(function (): void {
            $client = $this->connect(new Channel());
            try {
                try {
                    $client->send('notfound');
                    self::fail('Expected ChannelException for unknown API');
                } catch (ChannelException $error) {
                    self::assertSame('api not found', $error->getMessage());
                }
            } finally {
                $client->stop();
            }
        });
    }

    public function testUnknownSubscriptionReturnsNotFound(): void
    {
        $this->co(function (): void {
            $client = $this->connect(new Channel());
            try {
                try {
                    $client->subscribe('notfound', 'test', static fn(mixed $event): null => null);
                    self::fail('Expected ChannelException for unknown subscription');
                } catch (ChannelException $error) {
                    self::assertSame('subscription not found', $error->getMessage());
                }
            } finally {
                $client->stop();
            }
        });
    }

    public function testStreamValuesAndCancel(): void
    {
        $this->co(function (): void {
            $client = $this->connect(new Channel());
            try {
                $values = [];
                foreach ($client->values('stream/values') as $value) {
                    $values[] = $value;
                }
                self::assertSame([0, 1, 2], $values);

                $cancelled = [];
                foreach ($client->values('stream/cancel') as $value) {
                    $cancelled[] = $value;
                    if ($value === 2) {
                        break;
                    }
                }
                self::assertSame([0, 1, 2], $cancelled);
            } finally {
                $client->stop();
            }
        });
    }

    public function testAuthStateAndUnauthorized(): void
    {
        $this->co(function (): void {
            $authorized = $this->connect(new Channel());
            try {
                self::assertSame('tester', $authorized->send('auth', ['name' => 'tester']));
                self::assertSame('tester', $authorized->send('auth/name'));
            } finally {
                $authorized->stop();
            }

            $unauthorized = $this->connect(new Channel());
            try {
                try {
                    $unauthorized->send('auth/name');
                    self::fail('Expected unauthorized error');
                } catch (ChannelException $error) {
                    self::assertSame('unauthorized', $error->getMessage());
                }
            } finally {
                $unauthorized->stop();
            }
        });
    }

    public function testMirrorAndMergedApi(): void
    {
        $this->co(function (): void {
            $channel = (new Channel())->api([
                'hello' => static fn(): string => 'client world',
            ]);

            $client = $this->connect($channel);
            try {
                self::assertSame('client world', $client->send('mirror'));
                self::assertSame('world', $client->send('merged/hello'));
            } finally {
                $client->stop();
            }
        });
    }

    public function testMirrorStreamAndMirrorStreamCancel(): void
    {
        $this->co(function (): void {
            $channel = (new Channel())->api([
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

            $client = $this->connect($channel);
            try {
                $values = [];
                foreach ($client->values('mirror/stream') as $value) {
                    $values[] = $value;
                }
                self::assertSame([0, 1, 2], $values);

                $cancelled = [];
                foreach ($client->values('mirror/stream/cancel') as $value) {
                    $cancelled[] = $value;
                    if ($value === 2) {
                        break;
                    }
                }
                self::assertSame([0, 1, 2], $cancelled);
            } finally {
                $client->stop();
            }
        });
    }

    public function testMirrorEventsLifecycleDoesNotBreakConnection(): void
    {
        $this->co(function (): void {
            $names = new Subscription();
            $channel = (new Channel())->events(['names' => $names]);
            $client = $this->connect($channel);

            try {
                self::assertNull($client->send('mirror/events'));
                Coroutine::usleep(10_000);

                $names->send('2', 'po');
                $names->send('1', 'h');
                Coroutine::usleep(20_000);
                $names->send('1', 'we');

                self::assertSame('world', $client->send('hello'));
            } finally {
                $client->stop();
            }
        });
    }
}
