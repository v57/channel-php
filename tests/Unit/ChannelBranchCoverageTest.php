<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Unit;

use ChannelPhp\AsyncResult;
use ChannelPhp\CancellableRequest;
use ChannelPhp\Channel;
use ChannelPhp\ConnectionInterface;
use ChannelPhp\Controller;
use ChannelPhp\Subscription;
use PHPUnit\Framework\TestCase;
use function ChannelPhp\makeSender;

final class ChannelBranchCoverageTest extends TestCase
{
    public function testReceiveListPayloadAndMissingPathError(): void
    {
        $channel = (new Channel())->post('ok', static fn(): string => 'yes');
        [$sender, $controller, $state] = $this->makeController($channel);

        try {
            $channel->receive([
                ['id' => 1, 'path' => 'ok'],
                'ignored',
                ['id' => 2, 'path' => 'missing'],
            ], $controller);

            self::assertSame([
                ['id' => 1, 'body' => 'yes'],
                ['id' => 2, 'error' => 'api not found'],
            ], $state->responses);
        } finally {
            $sender->stop();
        }
    }

    public function testPathWithoutIdAndThrowingPathWithId(): void
    {
        $channel = new Channel();
        $channel->post('noid', static fn(): string => 'ignored');
        $channel->post('throws', static function (): never {
            throw new \RuntimeException('boom');
        });

        [$sender, $controller, $state] = $this->makeController($channel);

        try {
            $channel->receiveOne(['path' => 'noid'], $controller);
            $channel->receiveOne(['id' => 10, 'path' => 'throws'], $controller);

            self::assertSame([
                ['id' => 10, 'error' => 'boom'],
            ], $state->responses);
        } finally {
            $sender->stop();
        }
    }

    public function testStreamBranchesArrayErrorMissingAndCancelBreak(): void
    {
        $channel = new Channel();
        $channel->stream('arr', static fn(): array => [1, 2]);
        $channel->stream('bad', static fn(): string => 'nope');
        $channel->stream('throws', static function (): never {
            throw new \RuntimeException('stream boom');
        });
        $channel->stream('cancel-break', static function (array $body): \Generator {
            $id = (int) ($body['body']['id'] ?? 0);
            $sender = $body['sender'];
            return (function () use ($id, $sender): \Generator {
                if (isset($sender->streams[$id])) {
                    $sender->streams[$id]->cancelled = true;
                }
                yield 'value';
            })();
        });

        [$sender, $controller, $state] = $this->makeController($channel);

        try {
            $channel->receiveOne(['id' => 1, 'stream' => 'arr'], $controller);
            $channel->receiveOne(['id' => 2, 'stream' => 'bad'], $controller);
            $channel->receiveOne(['id' => 3, 'stream' => 'throws'], $controller);
            $channel->receiveOne(['id' => 4, 'stream' => 'missing'], $controller);
            $channel->receiveOne(['stream' => 'arr'], $controller);
            $channel->receiveOne(['id' => 5, 'stream' => 'cancel-break', 'body' => ['id' => 5]], $controller);

            self::assertContains(['id' => 1, 'body' => 1], $state->responses);
            self::assertContains(['id' => 1, 'body' => 2], $state->responses);
            self::assertContains(['id' => 1, 'done' => true], $state->responses);
            self::assertContains(['id' => 2, 'error' => 'stream must return iterable'], $state->responses);
            self::assertContains(['id' => 3, 'error' => 'stream boom'], $state->responses);
            self::assertContains(['id' => 4, 'error' => 'api not found'], $state->responses);

            foreach ($state->responses as $response) {
                self::assertNotSame(['id' => 5, 'done' => true], $response);
                self::assertNotSame(['id' => 5, 'body' => 'value'], $response);
            }
        } finally {
            $sender->stop();
        }
    }

    public function testCancelBranchesAndTaskCancellation(): void
    {
        $cancelActions = 0;
        $pendingOk = null;
        $pendingFail = null;

        $channel = new Channel();
        $channel->post('later-ok', static function () use (&$pendingOk): AsyncResult {
            $pendingOk = new AsyncResult();
            return $pendingOk;
        });
        $channel->post('later-fail', static function () use (&$pendingFail): AsyncResult {
            $pendingFail = new AsyncResult();
            return $pendingFail;
        });

        [$sender, $controller, $state] = $this->makeController($channel);

        try {
            $channel->receiveOne(['cancel' => null], $controller);

            $returnTracker = new class {
                public int $count = 0;
                public function return(): void
                {
                    $this->count++;
                }
            };
            $sender->streams[7] = $returnTracker;
            $sender->requests[7] = new CancellableRequest(static function (): void {
            });
            $channel->receiveOne(['cancel' => 7], $controller);

            $channel->receiveOne(['id' => 21, 'path' => 'later-ok'], $controller);
            $channel->receiveOne(['cancel' => 21], $controller);
            $pendingOk?->resolve('late');

            $channel->receiveOne(['id' => 22, 'path' => 'later-fail'], $controller);
            $channel->receiveOne(['cancel' => 22], $controller);
            $pendingFail?->reject('late');

            self::assertSame(1, $returnTracker->count);
            self::assertContains(['id' => 21, 'error' => 'cancelled'], $state->responses);
            self::assertContains(['id' => 22, 'error' => 'cancelled'], $state->responses);

            foreach ($state->responses as $response) {
                self::assertNotSame(['id' => 21, 'body' => 'late'], $response);
                self::assertNotSame(['id' => 22, 'error' => 'late'], $response);
            }
        } finally {
            $sender->stop();
        }
    }

    public function testAsyncPathSuccessClearsTaskAfterResponse(): void
    {
        $pending = new AsyncResult();
        $channel = (new Channel())->post('later', static fn(): AsyncResult => $pending);

        [$sender, $controller, $state] = $this->makeController($channel);

        try {
            $channel->receiveOne(['id' => 55, 'path' => 'later'], $controller);
            self::assertArrayHasKey(55, $sender->requests);

            $pending->resolve('done');

            self::assertContains(['id' => 55, 'body' => 'done'], $state->responses);
            self::assertArrayNotHasKey(55, $sender->requests);
        } finally {
            $sender->stop();
        }
    }

    public function testSubscriptionErrorBranches(): void
    {
        $rejecting = new Subscription();
        $rejecting->topic(static fn(): string => 'x');
        $rejecting->_body = static fn(): AsyncResult => AsyncResult::rejected('sub failed');

        $throwing = new Subscription();
        $throwing->topic(static function (): string {
            throw new \RuntimeException('topic failed');
        });

        $channel = (new Channel())->events([
            'rejecting' => $rejecting,
            'throwing' => $throwing,
        ]);

        [$sender, $controller, $state] = $this->makeController($channel);

        try {
            $channel->receiveOne(['id' => 1, 'sub' => 'missing'], $controller);
            $channel->receiveOne(['id' => 2, 'sub' => 'rejecting'], $controller);
            $channel->receiveOne(['id' => 3, 'sub' => 'throwing'], $controller);

            self::assertContains(['id' => 1, 'error' => 'subscription not found'], $state->responses);
            self::assertContains(['id' => 2, 'error' => 'sub failed'], $state->responses);
            self::assertContains(['id' => 3, 'error' => 'topic failed'], $state->responses);
        } finally {
            $sender->stop();
        }
    }

    public function testResponseMissingRequestAndTopicWithoutEventBody(): void
    {
        $requestResponse = null;

        $channel = new Channel();
        [$sender, $controller, $state] = $this->makeController($channel);

        try {
            $channel->receiveOne(['id' => 999, 'body' => 'ignored'], $controller);

            $pending = $channel->makeRequest('x', null, null, static function (array $response) use (&$requestResponse): void {
                $requestResponse = $response;
            });

            $channel->receiveOne(['id' => $pending['id'], 'topic' => 'some/topic', 'body' => 0], $controller);

            self::assertSame(['id' => $pending['id'], 'topic' => 'some/topic', 'body' => 0], $requestResponse);
            self::assertSame([], $state->events);
        } finally {
            $sender->stop();
        }
    }

    public function testMergeKeepsPostOtherAndStreamOtherUnderPrefix(): void
    {
        $match = static fn(string $path): bool => str_starts_with($path, 'dyn/');
        $inner = (new Channel())
            ->postOther($match, static fn(array $body): string => 'post:' . (string) $body['body'])
            ->streamOther(static fn(string $path): bool => $path === 'feed', static function (): \Generator {
                yield 1;
            });
        $outer = (new Channel())->merge($inner, 'm');

        self::assertCount(1, $outer->otherPostApi);
        self::assertTrue(($outer->otherPostApi[0]['path'])('m/dyn/x'));
        self::assertFalse(($outer->otherPostApi[0]['path'])('dyn/x'));
        self::assertTrue(($outer->otherStreamApi[0]['path'])('m/feed'));

        $sender = $outer->process();
        try {
            self::assertSame('post:a', $sender->send('m/dyn/x', 'a'));
            try {
                $sender->send('dyn/x', 'a');
                self::fail('unscoped path must not reach the merged catch-all');
            } catch (\Throwable $error) {
                self::assertStringContainsString('api not found', $error->getMessage());
            }
        } finally {
            $sender->stop();
        }

        // Without a prefix the matchers are reused as-is
        $flat = (new Channel())->merge($inner);
        self::assertSame($match, $flat->otherPostApi[0]['path']);
    }

    public function testMergeAndApiParserBranches(): void
    {
        $sourceSub = new Subscription();
        $source = (new Channel())
            ->post('hello', static fn(): string => 'world')
            ->stream('values', static function (): \Generator {
                yield 1;
            })
            ->events(['news' => $sourceSub]);

        $target = (new Channel())->merge($source, 'pref');
        self::assertArrayHasKey('pref/hello', $target->postApi);
        self::assertArrayHasKey('pref/values', $target->streamApi);
        self::assertArrayHasKey('pref/news', $target->eventsApi ?? []);

        $arrayCallable = new class {
            public function gen(): \Generator
            {
                yield 1;
            }
        };
        $invokable = new class {
            public function __invoke(): \Generator
            {
                yield 2;
            }
        };
        $magic = new class {
            public function __call(string $name, array $args): string
            {
                return $name;
            }
        };

        $target->api([
            0 => static fn(): string => 'skip',
            'arrCallable' => [$arrayCallable, 'gen'],
            'invoke' => $invokable,
            'broken' => [$magic, 'missing'],
            'nested' => ['_' => static fn(): string => 'root'],
        ]);

        self::assertArrayHasKey('arrCallable', $target->streamApi);
        self::assertArrayHasKey('invoke', $target->streamApi);
        self::assertArrayHasKey('broken', $target->postApi);
        self::assertArrayHasKey('nested', $target->postApi);
    }

    /**
     * @return array{0: \ChannelPhp\Sender, 1: Controller, 2: object}
     */
    private function makeController(Channel $channel): array
    {
        $sender = makeSender($channel, new class implements ConnectionInterface {
            public function send(array $body): mixed
            {
                return 0;
            }

            public function sent(int $id): void
            {
            }

            public function cancel(int $id): bool
            {
                return false;
            }

            public function notify(array $body): void
            {
            }

            public function addTopic(string $topic, callable $event): callable
            {
                return static fn(): bool => true;
            }

            public function stop(): void
            {
            }
        });

        $state = (object) [
            'responses' => [],
            'events' => [],
        ];

        $controller = new Controller(
            responseImpl: static function (array $response) use ($state): void {
                $state->responses[] = $response;
            },
            subscribeImpl: static fn(string $topic): bool => false,
            unsubscribeImpl: static fn(string $topic): bool => false,
            eventImpl: static function (string $topic, mixed $event) use ($state): void {
                $state->events[] = ['topic' => $topic, 'body' => $event];
            },
            sender: $sender,
            state: new \stdClass(),
        );

        return [$sender, $controller, $state];
    }
}
