<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Unit;

use ChannelPhp\CancellableRequest;
use ChannelPhp\AsyncResult;
use ChannelPhp\Channel;
use ChannelPhp\RequestHandle;
use ChannelPhp\RunningStream;
use ChannelPhp\Subscription;
use ChannelPhp\SubscriptionEvent;
use ChannelPhp\TopicSubscriptions;
use ChannelPhp\Internal\JsonCodec;
use ChannelPhp\Transports\WebSocketClient;
use ChannelPhp\Internal\Waiter;
use OpenSwoole\Coroutine\Http\Client as CoHttpClient;
use PHPUnit\Framework\TestCase;

final class LowLevelCoverageTest extends TestCase
{
    public function testCancellableRequestRunsCancelActionsAndImpl(): void
    {
        $actions = 0;
        $implCalled = false;

        $request = new CancellableRequest(static function () use (&$implCalled): void {
            $implCalled = true;
        });
        $request->onCancel(static function () use (&$actions): void {
            $actions++;
        });
        $request->onCancel(static function () use (&$actions): void {
            $actions++;
            throw new \RuntimeException('ignored');
        });

        $request->cancel();

        self::assertTrue($request->isCancelled);
        self::assertSame(2, $actions);
        self::assertTrue($implCalled);
    }

    public function testRequestHandleAwaitAndCancel(): void
    {
        $waiter = new Waiter();
        $waiter->resolve('ok');
        $cancelled = false;

        $handle = new RequestHandle($waiter, static function () use (&$cancelled): void {
            $cancelled = true;
        });

        self::assertSame('ok', $handle->await());
        $handle->cancel();
        self::assertTrue($cancelled);
    }

    public function testAsyncResultGuardsAndCallbacks(): void
    {
        $resolved = new AsyncResult();
        $resolvedValue = null;
        $resolved->then(static function (mixed $value) use (&$resolvedValue): void {
            $resolvedValue = $value;
        });
        $resolved->resolve(1);
        $resolved->resolve(2);
        $resolved->reject('ignored');
        self::assertSame(1, $resolvedValue);

        $rejected = new AsyncResult();
        $rejectedValue = null;
        $rejected->catch(static function (mixed $error) use (&$rejectedValue): void {
            $rejectedValue = $error;
        });
        $rejected->reject('boom');
        $rejected->reject('later');
        $rejected->resolve('ignored');
        self::assertSame('boom', $rejectedValue);
    }

    public function testWaiterGuardsAndThrowablePath(): void
    {
        $waiter = new Waiter();
        $waiter->resolve('ok');
        $waiter->resolve('again');
        self::assertSame('ok', $waiter->await());

        $throwing = new Waiter();
        $throwing->reject(new \RuntimeException('fail'));
        try {
            $throwing->await();
            self::fail('Expected RuntimeException from Waiter');
        } catch (\RuntimeException $error) {
            self::assertSame('fail', $error->getMessage());
        }

        $rejecting = new Waiter();
        $rejecting->reject('first');
        $rejecting->reject('second');
        try {
            $rejecting->await();
            self::fail('Expected exception from Waiter rejection');
        } catch (\Throwable $error) {
            self::assertSame('first', $error->getMessage());
        }
    }

    public function testWaiterAwaitSleepsUntilSignalResolves(): void
    {
        $waiter = new Waiter();
        $parentPid = posix_getpid();

        pcntl_async_signals(true);
        pcntl_signal(SIGUSR1, static function () use ($waiter): void {
            $waiter->resolve('signal');
        });

        $pid = pcntl_fork();
        self::assertIsInt($pid);
        self::assertGreaterThanOrEqual(0, $pid);
        if ($pid === 0) {
            usleep(30_000);
            @posix_kill($parentPid, SIGUSR1);
            exit(0);
        }

        try {
            self::assertSame('signal', $waiter->await());
        } finally {
            @pcntl_waitpid($pid, $status);
            pcntl_signal(SIGUSR1, SIG_DFL);
        }
    }

    public function testJsonCodecCanForceFallbackDecoder(): void
    {
        putenv('CHANNEL_PHP_DISABLE_SIMDJSON=1');
        $_ENV['CHANNEL_PHP_DISABLE_SIMDJSON'] = '1';
        try {
            $decoded = JsonCodec::decode('{"ok":true,"n":1}');
            self::assertSame(['ok' => true, 'n' => 1], $decoded);
        } finally {
            putenv('CHANNEL_PHP_DISABLE_SIMDJSON');
            unset($_ENV['CHANNEL_PHP_DISABLE_SIMDJSON']);
        }
    }

    public function testValuesErrorClosedAndPendingCancelBranches(): void
    {
        $channel = new Channel();
        $sender = $channel->process();
        $values = $sender->values('unused');

        try {
            $processResponse = new \ReflectionMethod($values, 'processResponse');
            $processResponse->setAccessible(true);

            try {
                $processResponse->invoke($values, ['error' => 'boom']);
                self::fail('Expected ChannelException for error response');
            } catch (\ChannelPhp\ChannelException $error) {
                self::assertSame('boom', $error->getMessage());
            }

            $this->setPrivate($values, 'isClosed', true);
            self::assertSame(['value' => null, 'done' => true], $values->next());

            $waiter = new Waiter();
            $this->setPrivate($values, 'pending', [$waiter]);
            $this->setPrivate($values, 'rid', 99);
            $values->cancel();

            self::assertSame(['done' => true], $waiter->await());
        } finally {
            $sender->stop();
        }
    }

    public function testRunningStreamReturnSetsCancelled(): void
    {
        $running = new RunningStream(new \stdClass());
        $running->return();
        self::assertTrue($running->cancelled);
    }

    public function testTopicSubscriptionsLifecycle(): void
    {
        $subscriptions = new TopicSubscriptions();
        $events = [];

        $cancelA = $subscriptions->addTopic('hello', static function (mixed $body) use (&$events): void {
            $events[] = ['a', $body];
        });
        $cancelB = $subscriptions->addTopic('hello', static function (mixed $body) use (&$events): void {
            $events[] = ['b', $body];
        });

        $subscriptions->receivedEvent('missing', 'noop');
        $subscriptions->receivedEvent('hello', 'world');

        self::assertSame([['a', 'world'], ['b', 'world']], $events);
        self::assertFalse($cancelA());
        self::assertTrue($cancelB());
        self::assertFalse($cancelB());
    }

    public function testSubscriptionParseNestedAndNonStringKeys(): void
    {
        $root = new Subscription();
        $nested = new Subscription();
        $map = [];

        Subscription::parse(
            [
                0 => new Subscription(),
                'root' => $root,
                'group' => ['nested' => $nested],
            ],
            '',
            $map,
        );

        self::assertArrayNotHasKey('0', $map);
        self::assertArrayHasKey('root', $map);
        self::assertArrayHasKey('group/nested', $map);
        self::assertSame('root', $root->prefix);
        self::assertSame('group//nested', $nested->prefix);
    }

    public function testSubscriptionAsyncAndPublishBranches(): void
    {
        $subscription = new Subscription();
        $subscription->prefix = 'events';
        $published = [];

        $subscription->publishers[] = new class($published) {
            public function __construct(private array $published)
            {
            }

            public function publish(SubscriptionEvent $event): void
            {
                $this->published[] = ['topic' => $event->topic, 'body' => $event->body];
            }

            public function events(): array
            {
                return $this->published;
            }
        };

        $subscription->send('hello', AsyncResult::resolved('world'));
        $subscription->send('hello', AsyncResult::rejected('ignored'));
        $subscription->publish('hello', 0);

        /** @var object $publisher */
        $publisher = $subscription->publishers[0];
        self::assertTrue(method_exists($publisher, 'events'));
        self::assertSame([['topic' => 'events/hello', 'body' => 'world']], $publisher->events());
    }

    public function testWebSocketClientConstructorFailsOutsideCoroutine(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('requires OpenSwoole coroutine context');
        new WebSocketClient('ws://127.0.0.1:65535');
    }

    public function testWebSocketClientStateTopicAndCancelBranches(): void
    {
        $client = $this->newWebSocketClient();
        self::assertTrue($client->isRunning());
        self::assertFalse($client->isConnected());

        $open = static fn(): null => null;
        $close = static fn(): null => null;
        $message = static fn(mixed $m): null => null;
        $client->setOnOpen($open);
        $client->setOnClose($close);
        $client->setOnMessage($message);
        self::assertSame($open, $this->getPrivate($client, 'onOpen'));
        self::assertSame($close, $this->getPrivate($client, 'onClose'));
        self::assertSame($message, $this->getPrivate($client, 'onMessage'));

        $events = [];
        $cancel1 = $client->addTopic('topic/1', static function (mixed $event) use (&$events): void {
            $events[] = 'a:' . $event;
        });
        $cancel2 = $client->addTopic('topic/1', static function (mixed $event) use (&$events): void {
            $events[] = 'b:' . $event;
        });

        $client->receivedEvent('unknown', 'none');
        $client->receivedEvent('topic/1', 'ok');
        self::assertSame(['a:ok', 'b:ok'], $events);

        self::assertFalse($cancel1());
        self::assertTrue($cancel2());
        self::assertFalse($cancel2());

        $client->throttle();
        self::assertSame(3, $this->getPrivate($client, 'isWaiting'));

        $this->setPrivate($client, 'isConnected', true);
        self::assertFalse($client->cancel(1));
        $this->setPrivate($client, 'isConnected', false);

        self::assertFalse($client->cancel(42));
        $this->setPrivate($client, 'pending', [42 => ['id' => 42]]);
        self::assertTrue($client->cancel(42));
    }

    public function testWebSocketClientSendNotifyStopAndResolveHeaders(): void
    {
        $client = $this->newWebSocketClient(static fn(): array => [
            'token' => 'abc',
            'empty' => '',
            'nil' => null,
            'x-number' => 7,
        ]);

        $fake = new class extends CoHttpClient {
            public array $sent = [];
            public bool $closed = false;
            public bool $throwOnPush = false;

            public function __construct()
            {
            }

            public function push($payload, $opcode = null, $flags = null)
            {
                if ($this->throwOnPush) {
                    throw new \RuntimeException('push failed');
                }
                $this->sent[] = $payload;
                return true;
            }

            public function close()
            {
                $this->closed = true;
                return true;
            }
        };

        // no socket branch
        $id0 = $client->send(['path' => 'hello']);
        self::assertSame(0, $id0);

        $this->setPrivate($client, 'ws', $fake);
        $this->setPrivate($client, 'isWaiting', 1);
        $this->setPrivate($client, 'isWaitingLength', 4001);
        $id1 = $client->send(['path' => 'sync']);
        self::assertSame(1, $id1);

        $this->setPrivate($client, 'isWaiting', 2);
        $id2 = $client->send(['path' => 'queued']);
        self::assertSame(2, $id2);
        self::assertNotSame([], $this->getPrivate($client, 'queue'));

        $client->notify(['topic' => 'news', 'body' => 'hello']);
        $fake->throwOnPush = true;
        $client->notify(['topic' => 'news', 'body' => 'ignored']);
        $fake->throwOnPush = false;

        self::assertNotSame([], $fake->sent);
        $client->sent(0);
        self::assertArrayNotHasKey(0, $this->getPrivate($client, 'pending'));

        $this->setPrivate($client, 'pending', [7 => ['id' => 7]]);
        $client->stop();
        self::assertTrue($fake->closed);
        self::assertFalse($client->isRunning());
        self::assertSame([], $this->getPrivate($client, 'pending'));

        $resolve = new \ReflectionMethod(WebSocketClient::class, 'resolveHeaders');
        $resolve->setAccessible(true);
        $headers = $resolve->invoke($client);
        self::assertSame(['token' => 'abc', 'x-number' => '7'], $headers);

        $clientBad = $this->newWebSocketClient(static fn(): string => 'bad');
        self::assertSame([], $resolve->invoke($clientBad));

        $clientNone = $this->newWebSocketClient();
        self::assertSame([], $resolve->invoke($clientNone));
    }

    public function testWebSocketClientRunLoopInvalidAddressTriggersCloseHook(): void
    {
        $client = $this->newWebSocketClient(null, 'http://:' );

        $closed = 0;
        $client->setOnClose(static function () use (&$closed): void {
            $closed++;
        });

        $runLoop = new \ReflectionMethod(WebSocketClient::class, 'runLoop');
        $runLoop->setAccessible(true);
        $runLoop->invoke($client);

        self::assertSame(1, $closed);
        self::assertFalse($client->isRunning());
        self::assertFalse($client->isConnected());
        self::assertNull($this->getPrivate($client, 'ws'));
    }

    public function testClientSenderWrappersExposeUnderlyingSender(): void
    {
        $channel = (new Channel())->post('hello', static fn(): string => 'world');
        $sender = $channel->process();
        $clientSender = new \ChannelPhp\ClientSender($sender, $this->newWebSocketClient());

        try {
            self::assertIsArray($clientSender->streams());
            self::assertIsArray($clientSender->requests());
            self::assertSame('world', $clientSender->send('hello'));
            $request = $clientSender->request('hello');
            self::assertSame('world', $request->await());
            $request->cancel();
        } finally {
            $clientSender->stop();
        }
    }

    private function newWebSocketClient(
        ?callable $headersProvider = null,
        string $address = 'ws://127.0.0.1:65535',
    ): WebSocketClient
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
        $this->setPrivate($client, 'address', $address);
        $this->setPrivate($client, 'headersProvider', $headersProvider);

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
