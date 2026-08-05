<?php
declare(strict_types=1);

namespace ChannelPhp\Transports;

use ChannelPhp\Channel;
use ChannelPhp\ConnectionInterface;
use ChannelPhp\Controller;
use ChannelPhp\Sender;
use OpenSwoole\Coroutine;
use function ChannelPhp\makeSender;

final class ProcessTransport
{
    public static function process(Channel $channel): Sender
    {
        $connection = new class($channel) implements ConnectionInterface {
            private ?Controller $controller = null;

            public function __construct(private readonly Channel $channel)
            {
            }

            public function setController(Controller $controller): void
            {
                $this->controller = $controller;
            }

            public function send(array $body): mixed
            {
                if ($this->controller !== null) {
                    $this->channel->receive($body, $this->controller);
                }
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
                if ($this->controller !== null) {
                    $this->channel->receive($body, $this->controller);
                }
            }

            public function addTopic(string $topic, callable $event): callable
            {
                return static fn(): bool => false;
            }

            public function stop(): void
            {
                if ($this->controller === null) {
                    return;
                }

                $state = $this->controller->state;
                $sender = $this->controller->sender;
                $this->controller = null;
                $this->channel->disconnect($state, $sender);
            }
        };

        $sender = makeSender($channel, $connection);
        $state = new \stdClass();

        $controller = null;
        $controller = new Controller(
            responseImpl: static function (array $body) use (&$controller, $channel): void {
                if ($controller !== null) {
                    if (Coroutine::getCid() >= 0 && function_exists('go')) {
                        go(static function () use ($channel, $body, &$controller): void {
                            if ($controller !== null) {
                                $channel->receive($body, $controller);
                            }
                        });
                    } else {
                        $channel->receive($body, $controller);
                    }
                }
            },
            subscribeImpl: static fn(string $topic): bool => false,
            unsubscribeImpl: static fn(string $topic): bool => false,
            eventImpl: static fn(string $topic, mixed $event): bool => false,
            sender: $sender,
            state: $state,
        );

        $connection->setController($controller);
        return $sender;
    }
}
