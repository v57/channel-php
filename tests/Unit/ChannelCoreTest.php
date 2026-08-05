<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Unit;

use ChannelPhp\AsyncResult;
use ChannelPhp\CancellableRequest;
use ChannelPhp\Channel;
use ChannelPhp\ConnectionInterface;
use ChannelPhp\Controller;
use ChannelPhp\Subscription;
use ChannelPhp\SubscriptionEvent;
use PHPUnit\Framework\TestCase;
use function ChannelPhp\makeSender;

final class ChannelCoreTest extends TestCase
{
    public function testPostOtherAndStreamOtherRouteDynamicPaths(): void
    {
        $channel = new Channel();
        $channel->postOther(
            static fn(string $path): bool => str_starts_with($path, 'dynamic/'),
            static fn(array $body): string => $body['path'] . ':' . (string) $body['body'],
        );
        $channel->streamOther(
            static fn(string $path): bool => str_ends_with($path, '/stream'),
            static function (array $body): \Generator {
                yield $body['body'];
            },
        );

        $sender = $channel->process();
        self::assertSame('dynamic/echo:ok', $sender->send('dynamic/echo', 'ok'));

        $values = [];
        foreach ($sender->values('dynamic/stream', 7) as $value) {
            $values[] = $value;
        }

        self::assertSame([7], $values);
        $sender->stop();
    }

    public function testDisconnectRunsCleanupCallbacksAndClearsMaps(): void
    {
        $disconnected = 0;
        $channel = (new Channel())->onDisconnect(static function () use (&$disconnected): void {
            $disconnected++;
            throw new \RuntimeException('ignored');
        });
        $sender = $channel->process();

        $closedStreams = 0;
        $sender->streams[1] = new class($closedStreams) {
            public function __construct(private int &$closedStreams)
            {
            }

            public function return(): void
            {
                $this->closedStreams++;
            }
        };
        $sender->streams[2] = new class {
            public function return(): void
            {
                throw new \RuntimeException('ignored');
            }
        };

        $cancelActions = 0;
        $request = new CancellableRequest();
        $request->onCancel(static function () use (&$cancelActions): void {
            $cancelActions++;
        });
        $request->onCancel(static function () use (&$cancelActions): void {
            $cancelActions++;
            throw new \RuntimeException('ignored');
        });
        $sender->requests[10] = $request;

        $channel->makeRequest('pending', null, null, static fn(array $response): bool => false, $sender);
        $channel->disconnect([], $sender);

        self::assertSame(1, $disconnected);
        self::assertSame(1, $closedStreams);
        self::assertSame(2, $cancelActions);
        self::assertSame([], $sender->streams);
        self::assertSame([], $sender->requests);
        self::assertSame([], $channel->requests);
    }

    public function testValuesReturnNotifiesByRequestIdWhenSendIdIsMissing(): void
    {
        $channel = new Channel();

        $connection = new class() implements ConnectionInterface {
            public array $cancelIds = [];
            public array $notifyBodies = [];

            public function send(array $body): mixed
            {
                return null;
            }

            public function sent(int $id): void
            {
            }

            public function cancel(int $id): bool
            {
                $this->cancelIds[] = $id;
                return false;
            }

            public function notify(array $body): void
            {
                $this->notifyBodies[] = $body;
            }

            public function addTopic(string $topic, callable $event): callable
            {
                return static fn(): bool => true;
            }

            public function stop(): void
            {
            }
        };

        $sender = makeSender($channel, $connection);
        $values = $sender->values('stream/stuck');

        $start = new \ReflectionMethod($values, 'start');
        $start->setAccessible(true);
        $start->invoke($values);

        $values->returnStream();

        self::assertSame([], $connection->cancelIds);
        self::assertSame([['cancel' => 0]], $connection->notifyBodies);
        self::assertSame([], $channel->requests);
    }

    public function testRequestCancelNotifiesWhenTransportCancelFails(): void
    {
        $channel = new Channel();

        $connection = new class() implements ConnectionInterface {
            public int $nextId = 200;
            public array $cancelIds = [];
            public array $notifyBodies = [];

            public function send(array $body): int
            {
                return $this->nextId++;
            }

            public function sent(int $id): void
            {
            }

            public function cancel(int $id): bool
            {
                $this->cancelIds[] = $id;
                return false;
            }

            public function notify(array $body): void
            {
                $this->notifyBodies[] = $body;
            }

            public function addTopic(string $topic, callable $event): callable
            {
                return static fn(): bool => true;
            }

            public function stop(): void
            {
            }
        };

        $sender = makeSender($channel, $connection);
        $request = $sender->request('slow');
        $requestId = array_key_first($channel->requests);

        $request->cancel();

        self::assertSame([200], $connection->cancelIds);
        self::assertSame([['cancel' => $requestId]], $connection->notifyBodies);
        self::assertSame([], $channel->requests);
    }

    public function testRequestCancelNotifiesByRequestIdWhenSendIdIsMissing(): void
    {
        $channel = new Channel();

        $connection = new class() implements ConnectionInterface {
            public array $cancelIds = [];
            public array $notifyBodies = [];

            public function send(array $body): mixed
            {
                return null;
            }

            public function sent(int $id): void
            {
            }

            public function cancel(int $id): bool
            {
                $this->cancelIds[] = $id;
                return false;
            }

            public function notify(array $body): void
            {
                $this->notifyBodies[] = $body;
            }

            public function addTopic(string $topic, callable $event): callable
            {
                return static fn(): bool => true;
            }

            public function stop(): void
            {
            }
        };

        $sender = makeSender($channel, $connection);
        $request = $sender->request('slow');
        $requestId = array_key_first($channel->requests);

        $request->cancel();

        self::assertSame([], $connection->cancelIds);
        self::assertSame([['cancel' => $requestId]], $connection->notifyBodies);
        self::assertSame([], $channel->requests);
    }

    public function testValuesReturnClearsPendingEvenWithoutTransportCancelSupport(): void
    {
        $channel = new Channel();

        $connection = new class() implements ConnectionInterface {
            public int $nextId = 100;
            public array $sendBodies = [];
            public array $cancelIds = [];
            public array $notifyBodies = [];

            public function send(array $body): int
            {
                $this->sendBodies[] = $body;
                return $this->nextId++;
            }

            public function sent(int $id): void
            {
            }

            public function cancel(int $id): bool
            {
                $this->cancelIds[] = $id;
                return false;
            }

            public function notify(array $body): void
            {
                $this->notifyBodies[] = $body;
            }

            public function addTopic(string $topic, callable $event): callable
            {
                return static fn(): bool => true;
            }

            public function stop(): void
            {
            }
        };

        $sender = makeSender($channel, $connection);
        $values = $sender->values('stream/stuck');

        $start = new \ReflectionMethod($values, 'start');
        $start->setAccessible(true);
        $start->invoke($values);
        self::assertCount(1, $channel->requests);

        $values->returnStream();

        self::assertSame([100], $connection->cancelIds);
        self::assertSame([['cancel' => 0]], $connection->notifyBodies);
        self::assertSame([], $channel->requests);
    }

    public function testProcessStopClearsPendingRequests(): void
    {
        $channel = new Channel();
        $channel->post('wait', static function (array $body): AsyncResult {
            /** @var CancellableRequest|null $task */
            $task = $body['task'] ?? null;
            $task?->onCancel(static fn(): bool => true);
            return new AsyncResult();
        });

        $sender = $channel->process();
        for ($i = 0; $i < 10; $i++) {
            $sender->request('wait');
        }

        self::assertNotSame([], $sender->requests);
        self::assertNotSame([], $channel->requests);

        $sender->stop();
        self::assertSame([], $sender->requests);
        self::assertSame([], $channel->requests);
    }

    public function testSubscriptionTopicAndBodyCanBothReplaceTopicFormatter(): void
    {
        $subscription = new Subscription();
        $events = [];

        $subscription->publishers[] = static function (SubscriptionEvent $event) use (&$events): void {
            $events[] = ['topic' => $event->topic, 'body' => $event->body];
        };

        $subscription->topic(static fn(string $body): string => 'topic-' . $body);
        $subscription->body(static fn(string $body): string => 'body-' . $body);
        $subscription->send('x', 'payload');

        self::assertSame([['topic' => 'body-x', 'body' => 'payload']], $events);
    }

    public function testSubscribeWaitsForAsyncBodyResultBeforeResponding(): void
    {
        $news = new Subscription();
        $news->topic(static fn(): string => 'ready');
        $news->_body = static fn(array $request): AsyncResult => AsyncResult::resolved(strtoupper((string) $request['value']));

        $channel = (new Channel())->events(['news' => $news]);
        $sender = $channel->process();

        $responses = [];
        $subscriptions = [];
        $controller = new Controller(
            responseImpl: static function (array $response) use (&$responses): void {
                $responses[] = $response;
            },
            subscribeImpl: static function (string $topic) use (&$subscriptions): void {
                $subscriptions[] = $topic;
            },
            unsubscribeImpl: static fn(string $topic): bool => false,
            eventImpl: static fn(string $topic, mixed $event): bool => false,
            sender: $sender,
            state: [],
        );

        $channel->receiveOne(['id' => 9, 'sub' => 'news', 'body' => ['value' => 'ok']], $controller);

        self::assertSame(['news/ready'], $subscriptions);
        self::assertSame([['topic' => 'news/ready', 'body' => 'OK', 'id' => 9]], $responses);

        $sender->stop();
    }

    public function testReceiveOneHandlesAsyncPostRejection(): void
    {
        $channel = (new Channel())->post('fails', static fn(): AsyncResult => AsyncResult::rejected('boom'));
        $sender = $channel->process();

        $responses = [];
        $controller = new Controller(
            responseImpl: static function (array $response) use (&$responses): void {
                $responses[] = $response;
            },
            subscribeImpl: static fn(string $topic): bool => false,
            unsubscribeImpl: static fn(string $topic): bool => false,
            eventImpl: static fn(string $topic, mixed $event): bool => false,
            sender: $sender,
            state: [],
        );

        $channel->receiveOne(['id' => 1, 'path' => 'fails'], $controller);

        self::assertSame([['id' => 1, 'error' => 'boom']], $responses);
        $sender->stop();
    }
}
