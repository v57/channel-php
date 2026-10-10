<?php
declare(strict_types=1);

namespace ChannelPhp\Transports;

use ChannelPhp\Channel;
use ChannelPhp\ConnectionInterface;
use ChannelPhp\Controller;
use ChannelPhp\Internal\JsonCodec;
use ChannelPhp\Subscription;
use ChannelPhp\SubscriptionEvent;
use ChannelPhp\TopicSubscriptions;
use OpenSwoole\WebSocket\Server as WebSocketServer;
use OpenSwoole\WebSocket\Frame;
use OpenSwoole\Http\Request;
use function ChannelPhp\makeSender;

final class ServerTransport
{
    public static function listen(Channel $channel, int|string $address, array $options = []): RunningServer
    {
        $host = '127.0.0.1';
        if (is_string($address)) {
            [$host, $port] = explode(':', $address, 2);
            $port = (int) $port;
        } else {
            $port = (int) $address;
        }

        $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        if ($pair === false) {
            throw new \RuntimeException('Failed to create startup signal pipe');
        }

        $pid = pcntl_fork();
        if (!is_int($pid)) {
            fclose($pair[0]);
            fclose($pair[1]);
            throw new \RuntimeException('pcntl_fork is unavailable in this context (call listen() outside coroutine).');
        }
        if ($pid === -1) {
            fclose($pair[0]);
            fclose($pair[1]);
            throw new \RuntimeException('pcntl_fork failed');
        }

        if ($pid === 0) {
            fclose($pair[0]);
            self::runServerProcess($channel, $host, $port, $options, $pair[1]);
            exit(0);
        }

        fclose($pair[1]);
        stream_set_timeout($pair[0], 10);
        $line = fgets($pair[0]);
        fclose($pair[0]);

        if ($line === false || !str_starts_with(trim($line), 'ready')) {
            @posix_kill($pid, SIGKILL);
            @pcntl_waitpid($pid, $status);
            throw new \RuntimeException('OpenSwoole server failed to start');
        }

        return new RunningServer($pid, $host, $port);
    }

    private static function runServerProcess(
        Channel $channel,
        string $host,
        int $port,
        array $options,
        $signalPipe,
    ): void {
        if (function_exists('swoole_event_exit')) {
            @swoole_event_exit();
        }

        $server = new WebSocketServer($host, $port, SWOOLE_BASE);
        $server->set([
            'worker_num' => 1,
            'daemonize' => false,
            'max_request' => 0,
            'dispatch_mode' => 2,
            'log_file' => '/tmp/channel-php-openswoole.log',
            // Largest WebSocket message accepted: 16 MiB, the TS/Bun default (OpenSwoole's default
            // is 2 MiB). Note: this does NOT avoid OpenSwoole 26.2's segfault on a declared frame
            // length of 2^64-1 (upstream bug, AUDIT §2.6); only an upstream fix does.
            'package_max_length' => (int) ($options['maxPayload'] ?? WebSocketClient::DEFAULT_MAX_PAYLOAD),
        ]);

        /** @var array<int, array{state:mixed,sender:mixed,subscriptions:TopicSubscriptions,topics:array<string,bool>}> $contexts */
        $contexts = [];

        $publisherCleanupDone = false;
        $publisherRefs = [];

        $cleanupPublishers = static function () use (&$publisherCleanupDone, &$publisherRefs): void {
            if ($publisherCleanupDone) {
                return;
            }
            $publisherCleanupDone = true;

            foreach ($publisherRefs as $item) {
                /** @var Subscription $subscription */
                $subscription = $item['subscription'];
                $publisher = $item['publisher'];
                $index = array_search($publisher, $subscription->publishers, true);
                if ($index !== false) {
                    unset($subscription->publishers[$index]);
                    $subscription->publishers = array_values($subscription->publishers);
                }
            }

            $publisherRefs = [];
        };

        if ($channel->eventsApi !== null) {
            foreach ($channel->eventsApi as $subscription) {
                $publisher = static function (SubscriptionEvent $event) use (&$contexts, $server): void {
                    $message = JsonCodec::encode(['topic' => $event->topic, 'body' => $event->body]);
                    foreach ($contexts as $fd => $context) {
                        if (!isset($context['topics'][$event->topic])) {
                            continue;
                        }
                        try {
                            $server->push($fd, $message);
                        } catch (\Throwable) {
                        }
                    }
                };

                $subscription->publishers[] = $publisher;
                $publisherRefs[] = ['subscription' => $subscription, 'publisher' => $publisher];
            }
        }

        $stateFactory = $options['state'] ?? null;
        $onConnect = $options['onConnect'] ?? null;
        $onDisconnect = $options['onDisconnect'] ?? null;

        $server->on('start', static function () use ($signalPipe): void {
            fwrite($signalPipe, "ready\n");
            fflush($signalPipe);
        });

        $server->on('open', static function (WebSocketServer $server, Request $request) use (
            $channel,
            $stateFactory,
            $onConnect,
            &$contexts,
        ): void {
            $headers = [];
            if (isset($request->get) && is_array($request->get)) {
                foreach ($request->get as $key => $value) {
                    $headers[(string) $key] = is_scalar($value) ? (string) $value : null;
                }
            }
            if (isset($request->header) && is_array($request->header)) {
                foreach ($request->header as $key => $value) {
                    $headers[(string) $key] = is_scalar($value) ? (string) $value : null;
                }
            }

            $state = new \stdClass();
            if (is_callable($stateFactory)) {
                $state = $stateFactory($headers);
            }

            $fd = (int) $request->fd;
            $subscriptions = new TopicSubscriptions();

            $connection = new class($server, $fd, $subscriptions) implements ConnectionInterface {
                public function __construct(
                    private readonly WebSocketServer $server,
                    private readonly int $fd,
                    private readonly TopicSubscriptions $subscriptions,
                ) {
                }

                public function send(array $body): mixed
                {
                    try {
                        $this->server->push($this->fd, JsonCodec::encode($body));
                    } catch (\Throwable) {
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
                    try {
                        $this->server->push($this->fd, JsonCodec::encode($body));
                    } catch (\Throwable) {
                    }
                }

                public function addTopic(string $topic, callable $event): callable
                {
                    return $this->subscriptions->addTopic($topic, $event);
                }

                public function stop(): void
                {
                    try {
                        $this->server->disconnect($this->fd);
                    } catch (\Throwable) {
                    }
                }
            };

            $sender = makeSender($channel, $connection);
            $contexts[$fd] = [
                'state' => $state,
                'sender' => $sender,
                'subscriptions' => $subscriptions,
                'topics' => [],
            ];

            if (is_callable($onConnect)) {
                $onConnect([
                    'state' => $state,
                    'sender' => $sender,
                    'subscriptions' => $subscriptions,
                ]);
            }
        });

        $server->on('message', static function (WebSocketServer $server, Frame $frame) use (
            $channel,
            &$contexts,
        ): void {
            $fd = (int) $frame->fd;
            if (!isset($contexts[$fd])) {
                return;
            }

            if (!is_string($frame->data)) {
                return;
            }

            try {
                $payload = JsonCodec::decode($frame->data);
            } catch (\Throwable) {
                $server->disconnect($fd);
                return;
            }

            $controller = new Controller(
                responseImpl: static function (array $response) use ($server, $fd): void {
                    try {
                        $server->push($fd, JsonCodec::encode($response));
                    } catch (\Throwable) {
                    }
                },
                subscribeImpl: static function (string $topic) use (&$contexts, $fd): void {
                    $contexts[$fd]['topics'][$topic] = true;
                },
                unsubscribeImpl: static function (string $topic) use (&$contexts, $fd): void {
                    unset($contexts[$fd]['topics'][$topic]);
                },
                eventImpl: static function (string $topic, mixed $event) use (&$contexts, $fd): void {
                    $contexts[$fd]['subscriptions']->receivedEvent($topic, $event);
                },
                sender: $contexts[$fd]['sender'],
                state: $contexts[$fd]['state'],
            );

            $channel->receive($payload, $controller);
        });

        $server->on('close', static function (WebSocketServer $server, int $fd) use (
            $channel,
            $onDisconnect,
            &$contexts,
        ): void {
            if (!isset($contexts[$fd])) {
                return;
            }

            $context = $contexts[$fd];
            unset($contexts[$fd]);

            $channel->disconnect($context['state'], $context['sender']);
            if (is_callable($onDisconnect)) {
                $onDisconnect([
                    'state' => $context['state'],
                    'sender' => $context['sender'],
                    'subscriptions' => $context['subscriptions'],
                ]);
            }
        });

        $server->on('shutdown', static function () use ($cleanupPublishers, $signalPipe): void {
            $cleanupPublishers();
            fclose($signalPipe);
        });

        $server->start();
    }
}
